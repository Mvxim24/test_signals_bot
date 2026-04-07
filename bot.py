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

# Глобальный обменник
exchange = ccxt.mexc({
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})

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
    try:
        market = exchange.market(symbol)
        return float(market['precision']['price'])
    except Exception:
        return 0.0001


def round_price(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return round(price, 4)
    return round(price / tick_size) * tick_size


async def load_markets_once():
    try:
        await asyncio.to_thread(exchange.load_markets)
        logging.info(f"✅ Загружено {len(exchange.markets)} рынков с MEXC")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки markets: {e}")


# ====================== ОТПРАВКА СИГНАЛА ======================
async def send_signal(pair: str, direction: str, entry_price: float, tp: float, sl: float):
    try:
        key = f"{pair}_{direction}"
        now = datetime.now(timezone.utc)

        # Защита от частых сигналов (45 минут)
        if (now - last_signal_time[key]).total_seconds() < 2700:
            return

        last_signal_time[key] = now

        # Проверка валидности
        if direction == "LONG":
            if tp <= entry_price or sl >= entry_price:
                return
        else:
            if tp >= entry_price or sl <= entry_price:
                return

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
💰 <b>Цена входа:</b> <code>{entry_price:,.4f} USDT</code>
🎯 <b>Take Profit:</b> <code>{tp:,.4f} USDT</code> <b>(+{tp_p:.2f}%)</b>
🛑 <b>Stop Loss:</b> <code>{sl:,.4f} USDT</code> <b>({sl_p:.2f}%)</b>

🕒 <b>Время сигнала:</b> {now.strftime('%d.%m.%Y %H:%M:%S UTC')}

🔍 <b>#{hashtag}</b>"""

        await broadcast_message(text)
        print(f"✅ Сигнал отправлен → {pair} | {direction} | Entry: {entry_price}")

    except Exception as e:
        logging.error(f"Ошибка при отправке сигнала {pair}: {e}")


# ====================== ГЕНЕРАЦИЯ СИГНАЛОВ ======================
async def generate_signals():
    global is_generating
    if is_generating:
        return

    async with generate_lock:
        is_generating = True
        try:
            pairs = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "DOGE/USDT"]

            async with aiosqlite.connect(db_path) as db:
                open_pairs = {row[0] for row in await db.execute_fetchall(
                    "SELECT pair FROM signals WHERE status = 'open'")}

            for pair in pairs:
                if pair in open_pairs:
                    continue

                try:
                    ohlcv = await asyncio.to_thread(exchange.fetch_ohlcv, pair, '15m', limit=300)
                    df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                    df = df[['open', 'high', 'low', 'close', 'vol']].astype(float)

                    # Индикаторы
                    df['bb_middle'] = df['close'].rolling(window=25).mean()
                    df['bb_std'] = df['close'].rolling(window=25).std()
                    df['bb_upper'] = df['bb_middle'] + (df['bb_std'] * 2)
                    df['bb_lower'] = df['bb_middle'] - (df['bb_std'] * 2)

                    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
                    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()

                    if len(df) < 100 or df.iloc[-1].isna().any():
                        continue

                    curr = df.iloc[-1]
                    prev = df.iloc[-2]

                    direction = None
                    entry_price = curr['close']
                    sl = None
                    tp = None

                    # Улучшенные условия входа
                    if (prev['ema9'] <= prev['ema21'] and curr['ema9'] > curr['ema21'] and
                        curr['close'] > curr['bb_middle'] and curr['low'] <= curr['bb_lower'] * 1.001):

                        direction = "LONG"
                        sl = curr['low'] * 0.9975          # SL чуть ниже
                        risk = entry_price - sl
                        tp = entry_price + (risk * 2.2)    # R:R ≈ 1:2.2

                    elif (prev['ema9'] >= prev['ema21'] and curr['ema9'] < curr['ema21'] and
                          curr['close'] < curr['bb_middle'] and curr['high'] >= curr['bb_upper'] * 0.999):

                        direction = "SHORT"
                        sl = curr['high'] * 1.0025
                        risk = sl - entry_price
                        tp = entry_price - (risk * 2.2)

                    if not direction:
                        continue

                    # Фильтр по объёму
                    vol_ma = df['vol'].rolling(20).mean().iloc[-1]
                    if curr['vol'] < vol_ma * 0.75:
                        continue

                    await send_signal(pair, direction, entry_price, tp, sl)

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
                ticker = await asyncio.to_thread(exchange.fetch_ticker, pair)
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
Цена закрытия: <b>{current_price:,.4f} USDT</b>
Вход: <b>{entry:,.4f} USDT</b>"""

                    await broadcast_message(text)
                    print(f"✅ Сигнал закрыт → #{hashtag} | {status}")

            except Exception as e:
                logging.error(f"Ошибка мониторинга {hashtag} ({pair}): {e}")

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
        "🤖 Бот торгует по стратегии <b>Bollinger Bands 25 + EMA 9/21</b> на таймфрейме 15m.\n\n"
        "Сигналы приходят автоматически.",
        parse_mode="HTML",
        reply_markup=kb
    )


@dp.message(F.text == "📜 История сигналов")
async def show_history(message: types.Message):
    try:
        async with aiosqlite.connect(db_path) as db:
            history = await db.execute_fetchall("""
                SELECT * FROM signals 
                ORDER BY id DESC LIMIT 20
            """)

        if not history:
            await message.answer("📭 Пока нет сигналов.")
            return

        text = "📜 <b>История последних 20 сигналов</b>\n\n"
        for row in history:
            _, pair, direction, entry, tp, sl, ts, status, close_p, hashtag = row
            emoji = "📈" if direction == "LONG" else "📉"
            st = "✅ TAKE PROFIT" if status == "closed_tp" else "❌ STOP LOSS" if status == "closed_sl" else "⏳ Открыт"

            line = f"<b>#{hashtag}</b> {pair} {direction} {emoji}\n"
            line += f"Вход: <code>{entry:,.4f}</code>\n"
            if close_p:
                line += f"Закрыто: <code>{close_p:,.4f}</code> — {st}\n"
            else:
                line += f"TP: <code>{tp:,.4f}</code> | SL: <code>{sl:,.4f}</code>\n"
                line += f"Статус: {st}\n"
            line += f"Время: {ts[:16]}\n\n"
            text += line

        await message.answer(text, parse_mode="HTML")

    except Exception as e:
        logging.error(f"Ошибка в show_history: {e}")
        await message.answer("❌ Не удалось загрузить историю.")


@dp.message(Command("stats"))
async def admin_stats(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect(db_path) as db:
        total_subs = (await db.execute_fetchall("SELECT COUNT(*) FROM subscribers"))[0][0]
        open_signals = (await db.execute_fetchall("SELECT COUNT(*) FROM signals WHERE status = 'open'"))[0][0]
    await message.answer(f"📊 <b>Статистика бота:</b>\n\n"
                         f"Подписчиков: <b>{total_subs}</b>\n"
                         f"Открытых сигналов: <b>{open_signals}</b>")


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
    await load_markets_once()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    scheduler.add_job(generate_signals, 'interval', seconds=30, replace_existing=True)
    scheduler.add_job(monitor_open_signals, 'interval', seconds=15, replace_existing=True)

    scheduler.start()
    print("🚀 Бот успешно запущен | Стратегия: BB25 + EMA9/21 | R:R 1:2.2")

    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
