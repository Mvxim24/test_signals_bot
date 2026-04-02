import asyncio
import logging
import os
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd
import ccxt
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.session.aiohttp import AiohttpSession
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

if not TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в .env!")

db_path = "signals.db"

bot = Bot(token=TOKEN, session=AiohttpSession())
dp = Dispatcher()

exchange = ccxt.mexc({'enableRateLimit': True})

last_signal_time = defaultdict(lambda: datetime(2000, 1, 1, tzinfo=timezone.utc))
is_generating = False
generate_lock = asyncio.Lock()

scheduler = AsyncIOScheduler(timezone="UTC")


# ====================== БАЗА ДАННЫХ ======================
async def init_db():
    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS subscribers (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                subscribed_at TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT,
                direction TEXT,
                entry_price REAL,
                tp REAL,
                sl REAL,
                timestamp TEXT,
                status TEXT DEFAULT 'open',
                close_price REAL,
                hashtag TEXT
            )
        ''')
        await db.commit()


async def add_subscriber(user: types.User):
    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            INSERT OR REPLACE INTO subscribers (user_id, username, first_name, subscribed_at)
            VALUES (?, ?, ?, ?)
        ''', (user.id, user.username, user.first_name, datetime.now(timezone.utc).isoformat()))
        await db.commit()


async def remove_subscriber(user_id: int):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("DELETE FROM subscribers WHERE user_id = ?", (user_id,))
        await db.commit()


async def get_all_subscribers():
    async with aiosqlite.connect(db_path) as db:
        rows = await db.execute_fetchall("SELECT user_id FROM subscribers")
        return [row[0] for row in rows]


async def broadcast_message(text: str):
    subscribers = await get_all_subscribers()
    if not subscribers:
        return
    tasks = [bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True) for uid in subscribers]
    await asyncio.gather(*tasks, return_exceptions=True)


# ====================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======================
def get_tick_size(symbol: str) -> float:
    """Получаем минимальный шаг цены"""
    try:
        markets = exchange.load_markets()
        return float(markets[symbol]['precision']['price'])
    except:
        return 0.01


def round_price(price: float, tick_size: float) -> float:
    return round(price / tick_size) * tick_size


# ====================== ОТПРАВКА СИГНАЛА ======================
async def send_signal(pair: str, direction: str, entry_price: float, tp: float, sl: float):
    key = f"{pair}_{direction}"
    now = datetime.now(timezone.utc)

    if (now - last_signal_time[key]).total_seconds() < 2700:  # 45 минут
        return

    last_signal_time[key] = now

    tick_size = get_tick_size(pair)
    entry_price = round_price(entry_price, tick_size)
    tp = round_price(tp, tick_size)
    sl = round_price(sl, tick_size)

    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            INSERT INTO signals (pair, direction, entry_price, tp, sl, timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?, 'open')
        ''', (pair, direction, entry_price, tp, sl, now.isoformat()))
        await db.commit()
        cursor = await db.execute("SELECT last_insert_rowid()")
        signal_id = (await cursor.fetchone())[0]

    hashtag = f"SIG_{signal_id:04d}"

    async with aiosqlite.connect(db_path) as db:
        await db.execute("UPDATE signals SET hashtag = ? WHERE id = ?", (hashtag, signal_id))
        await db.commit()

    emoji = "📈" if direction == "LONG" else "📉"
    direction_text = "LONG ▲" if direction == "LONG" else "SHORT ▼"

    tp_p = ((tp - entry_price) / entry_price) * 100
    sl_p = ((sl - entry_price) / entry_price) * 100

    text = f"""🚨 <b>НОВЫЙ ТОРГОВЫЙ СИГНАЛ #{hashtag}</b>

{emoji} <b>{pair}</b> — <b>{direction_text}</b> {emoji}

──────────────────
💰 <b>Цена входа:</b> <code>{entry_price:,.2f} USDT</code>
🎯 <b>Take Profit:</b> <code>{tp:,.2f} USDT</code> <b>({tp_p:+.2f}%)</b>
🛑 <b>Stop Loss:</b> <code>{sl:,.2f} USDT</code> <b>({sl_p:+.2f}%)</b>

🕒 <b>Время сигнала:</b> {now.strftime('%d.%m.%Y %H:%M:%S UTC')}

🔍 <b>#{hashtag}</b>
📌 Стратегия от Максима"""

    await broadcast_message(text)
    print(f"✅ Сигнал отправлен → {pair} | {direction} | Entry: {entry_price}")


# ====================== ГЕНЕРАЦИЯ СИГНАЛОВ (EMA 200 — строгое закрытие на EMA) ======================
async def generate_signals():
    global is_generating
    if is_generating:
        return

    async with generate_lock:
        is_generating = True
        try:
            pairs = ["BTC/USDT", "ETH/USDT"]

            # Получаем пары с открытыми сигналами
            async with aiosqlite.connect(db_path) as db:
                open_rows = await db.execute_fetchall("SELECT pair FROM signals WHERE status = 'open'")
                open_pairs = {row[0] for row in open_rows}

            for pair in pairs:
                if pair in open_pairs:
                    continue

                try:
                    ohlcv = exchange.fetch_ohlcv(pair, '15m', limit=300)
                    df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])

                    # EMA 200
                    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()

                    if len(df) < 210 or pd.isna(df.iloc[-1]['ema200']):
                        continue

                    curr = df.iloc[-1]
                    prev = df.iloc[-2]
                    price = float(curr['close'])
                    ema200 = float(curr['ema200'])

                    # Свеча закрылась строго на EMA 200 (±0.05%)
                    close_on_ema = abs(price - ema200) / ema200 <= 0.0005

                    direction = None
                    sl = tp = None
                    entry = price

                    # === LONG ===
                    # Пересечение сверху вниз + закрытие на EMA + откат вниз 0.5%
                    if (prev['close'] >= prev['ema200'] and curr['close'] <= curr['ema200']) and close_on_ema:
                        if price <= ema200 * 0.995:          # -0.5% от EMA200
                            direction = "LONG"
                            sl = round_price(entry * 0.992, get_tick_size(pair))   # -0.8%
                            tp = round_price(entry * 1.012, get_tick_size(pair))   # +1.2%

                    # === SHORT ===
                    # Пересечение снизу вверх + закрытие на EMA + откат вверх 0.5%
                    elif (prev['close'] <= prev['ema200'] and curr['close'] >= curr['ema200']) and close_on_ema:
                        if price >= ema200 * 1.005:          # +0.5% от EMA200
                            direction = "SHORT"
                            sl = round_price(entry * 1.008, get_tick_size(pair))   # +0.8%
                            tp = round_price(entry * 0.988, get_tick_size(pair))   # -1.2%

                    if direction:
                        await send_signal(pair, direction, entry, tp, sl)

                except Exception as e:
                    logging.error(f"Ошибка анализа {pair}: {e}")

        finally:
            is_generating = False


# ====================== МОНИТОРИНГ TP/SL ======================
async def monitor_open_signals():
    try:
        async with aiosqlite.connect(db_path) as db:
            rows = await db.execute_fetchall("""
                SELECT id, pair, direction, tp, sl, hashtag, entry_price 
                FROM signals WHERE status = 'open'
            """)

        for row in rows:
            signal_id, pair, direction, tp, sl, hashtag, entry = row
            try:
                ticker = exchange.fetch_ticker(pair)
                current_price = float(ticker['last'])

                closed = False
                status = None

                if direction == "LONG":
                    if current_price >= tp:
                        status = "closed_tp"
                        closed = True
                    elif current_price <= sl:
                        status = "closed_sl"
                        closed = True
                else:  # SHORT
                    if current_price <= tp:
                        status = "closed_tp"
                        closed = True
                    elif current_price >= sl:
                        status = "closed_sl"
                        closed = True

                if closed:
                    async with aiosqlite.connect(db_path) as db:
                        await db.execute(
                            "UPDATE signals SET status = ?, close_price = ? WHERE id = ?",
                            (status, current_price, signal_id)
                        )
                        await db.commit()

                    status_text = "✅ TAKE PROFIT" if status == "closed_tp" else "❌ STOP LOSS"
                    text = f"""📢 <b>Сигнал закрыт #{hashtag}</b>

{status_text}
Цена закрытия: <b>{current_price:,.2f} USDT</b>
Вход был: <b>{entry:,.2f} USDT</b>"""

                    await broadcast_message(text)
                    print(f"✅ Сигнал закрыт → #{hashtag} | {status}")

            except Exception as e:
                logging.error(f"Ошибка мониторинга {hashtag}: {e}")
    except Exception as e:
        logging.error(f"Ошибка monitor_open_signals: {e}")


# ====================== ХЭНДЛЕРЫ ======================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await add_subscriber(message.from_user)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📜 История сигналов")],
            [KeyboardButton(text="❌ Отписаться")]
        ],
        resize_keyboard=True
    )
    await message.answer(
        f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n"
        "🤖 Бот использует стратегию **EMA 200 Pullback** на 15-минутном таймфрейме.\n"
        "📌 <b>Стратегия от Максима</b>\n\n"
        "Сигналы приходят автоматически.",
        parse_mode="HTML",
        reply_markup=kb
    )


@dp.message(F.text == "📜 История сигналов")
async def show_history(message: types.Message):
    try:
        async with aiosqlite.connect(db_path) as db:
            history = await db.execute_fetchall("""
                SELECT * FROM signals ORDER BY id DESC LIMIT 20
            """)

        if not history:
            await message.answer("📭 Пока нет сигналов.")
            return

        text = "📜 <b>История сигналов (последние 20)</b>\n\n"
        for row in history:
            _, pair, direction, entry, tp, sl, ts, status, close_p, hashtag = row
            emoji = "📈" if direction == "LONG" else "📉"
            st = "✅ TAKE PROFIT" if status == "closed_tp" else "❌ STOP LOSS" if status == "closed_sl" else "⏳ Открыт"

            line = f"<b>#{hashtag}</b> {pair} {direction} {emoji}\n"
            line += f"Вход: <code>{entry:,.2f}</code>\n"
            if close_p:
                line += f"Закрыто: <code>{close_p:,.2f}</code> — {st}\n"
            else:
                line += f"Статус: {st}\n"
            line += f"Время: {ts[:16]}\n\n"
            text += line

        await message.answer(text, parse_mode="HTML")

    except Exception as e:
        logging.error(f"Ошибка в show_history: {e}")
        await message.answer("❌ Не удалось загрузить историю.")


@dp.message(F.text == "❌ Отписаться")
async def unsubscribe(message: types.Message):
    await remove_subscriber(message.from_user.id)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="✅ Подписаться")]], resize_keyboard=True)
    await message.answer("❌ Вы отписались от сигналов.", reply_markup=kb)


@dp.message(F.text == "✅ Подписаться")
async def subscribe_again(message: types.Message):
    await add_subscriber(message.from_user)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📜 История сигналов")], [KeyboardButton(text="❌ Отписаться")]],
        resize_keyboard=True
    )
    await message.answer("✅ Подписка успешно активирована!", reply_markup=kb)


# ====================== ЗАПУСК ======================
async def main():
    await init_db()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    scheduler.add_job(generate_signals, 'interval', seconds=20, replace_existing=True)
    scheduler.add_job(monitor_open_signals, 'interval', seconds=25, replace_existing=True)

    scheduler.start()
    print("🚀 Бот успешно запущен | Стратегия: EMA 200 Pullback (закрытие на EMA) | От Максима")

    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())