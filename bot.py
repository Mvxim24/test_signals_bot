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

# ==================== УЛУЧШЕННАЯ ИНИЦИАЛИЗАЦИЯ БОТА ====================
session = AiohttpSession(timeout=30.0)
bot = Bot(token=TOKEN, session=session)
dp = Dispatcher()

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
    tasks = [bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True) 
             for uid in subscribers]
    await asyncio.gather(*tasks, return_exceptions=True)


# ====================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======================
def get_tick_size(symbol: str) -> float:
    try:
        markets = exchange.load_markets()
        return float(markets[symbol]['precision']['price'])
    except:
        return 0.0001 if 'BTC' in symbol else 0.01


def round_price(price: float, tick_size: float) -> float:
    return round(price / tick_size) * tick_size


def get_higher_tf_trend(pair: str) -> bool:
    """Возвращает True, если на H1 восходящий тренд"""
    try:
        ohlcv = exchange.fetch_ohlcv(pair, '1h', limit=80)
        df_h = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        sma20_h = df_h['close'].rolling(20).mean()
        sma50_h = df_h['close'].rolling(50).mean()
        return sma20_h.iloc[-1] > sma50_h.iloc[-1]
    except:
        return False


# ====================== ГЕНЕРАЦИЯ СИГНАЛОВ ======================
async def generate_signals():
    global is_generating
    if is_generating:
        return

    async with generate_lock:
        is_generating = True
        try:
            pairs = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT"]

            async with aiosqlite.connect(db_path) as db:
                open_rows = await db.execute_fetchall("SELECT pair FROM signals WHERE status = 'open'")
                open_pairs = {row[0] for row in open_rows}

            logging.info(f"🔄 Запуск генерации сигналов. Открытых: {len(open_pairs)}")

            for pair in pairs:
                if pair in open_pairs:
                    continue

                try:
                    # Асинхронные запросы к бирже
                    ohlcv = await asyncio.to_thread(exchange.fetch_ohlcv, pair, '15m', limit=400)
                    df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])

                    # Индикаторы
                    df['sma20'] = df['close'].rolling(20).mean()
                    df['sma50'] = df['close'].rolling(50).mean()
                    df['sma200'] = df['close'].rolling(200).mean()
                    df['vol_ma20'] = df['vol'].rolling(20).mean()

                    # ATR
                    df['tr'] = pd.concat([
                        df['high'] - df['low'],
                        abs(df['high'] - df['close'].shift()),
                        abs(df['low'] - df['close'].shift())
                    ], axis=1).max(axis=1)
                    df['atr'] = df['tr'].rolling(14).mean()

                    if len(df) < 250 or pd.isna(df.iloc[-1]['sma200']) or pd.isna(df.iloc[-1]['atr']):
                        continue

                    curr = df.iloc[-1]
                    prev = df.iloc[-2]
                    price = float(curr['close'])
                    vol = float(curr['vol'])
                    vol_ma = float(curr['vol_ma20'])
                    atr = float(curr['atr'])

                    # Тренды
                    is_bull_trend = (curr['sma20'] > curr['sma50'] > curr['sma200']) and (curr['sma20'] > curr['sma200'] * 1.005)
                    is_bear_trend = (curr['sma20'] < curr['sma50'] < curr['sma200']) and (curr['sma20'] < curr['sma200'] * 0.995)

                    h1_bull = await asyncio.to_thread(get_higher_tf_trend, pair)

                    logging.info(f"{pair} | Bull: {is_bull_trend} | Bear: {is_bear_trend} | H1 Bull: {h1_bull} | "
                                 f"Vol Ratio: {vol/vol_ma:.2f if vol_ma > 0 else 0:.2f}")

                    if is_bull_trend and not h1_bull:
                        continue
                    if is_bear_trend and h1_bull:
                        continue
                    if not (is_bull_trend or is_bear_trend):
                        continue

                    direction = None
                    sl = tp = None
                    tick_size = await asyncio.to_thread(get_tick_size, pair)

                    if is_bull_trend:
                        if (prev['close'] <= prev['sma20'] and 
                            curr['close'] > curr['sma20'] and 
                            curr['close'] > curr['open'] and 
                            vol > vol_ma * 1.15):
                            
                            direction = "LONG"
                            sl = round_price(price - atr * 1.85, tick_size)
                            tp = round_price(price + atr * 3.1, tick_size)

                    elif is_bear_trend:
                        if (prev['close'] >= prev['sma20'] and 
                            curr['close'] < curr['sma20'] and 
                            curr['close'] < curr['open'] and 
                            vol > vol_ma * 1.15):
                            
                            direction = "SHORT"
                            sl = round_price(price + atr * 1.85, tick_size)
                            tp = round_price(price - atr * 3.1, tick_size)

                    if direction:
                        logging.info(f"✅ СИГНАЛ → {pair} {direction}")
                        await send_signal(pair, direction, price, tp, sl)

                except Exception as e:
                    logging.error(f"Ошибка анализа {pair}: {e}")

        finally:
            is_generating = False


# ====================== ОТПРАВКА СИГНАЛА ======================
async def send_signal(pair: str, direction: str, entry_price: float, tp: float, sl: float):
    key = f"{pair}_{direction}"
    now = datetime.now(timezone.utc)

    if (now - last_signal_time[key]).total_seconds() < 2700:  # 45 минут
        return

    last_signal_time[key] = now

    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            INSERT INTO signals (pair, direction, entry_price, tp, sl, timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?, 'open')
        ''', (pair, direction, entry_price, tp, sl, now.isoformat()))
        await db.commit()
        cursor = await db.execute("SELECT last_insert_rowid()")
        signal_id = (await cursor.fetchone())[0]

    hashtag = f"SIG_{signal_id:04d}"
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

🕒 <b>Время:</b> {now.strftime('%d.%m.%Y %H:%M:%S UTC')}

🔍 <b>#{hashtag}</b>"""

    await broadcast_message(text)
    print(f"📤 Сигнал отправлен → {pair} | {direction}")


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
                    print(f"🔒 Сигнал закрыт → #{hashtag} | {status}")

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
        "🤖 Бот работает на улучшенной стратегии SMA + ATR + H1 фильтр.\n"
        "Сигналы приходят реже, но качественнее.",
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

        text = "📜 <b>Последние 20 сигналов:</b>\n\n"
        for row in history:
            _, pair, direction, entry, tp, sl, ts, status, close_p, hashtag = row
            emoji = "📈" if direction == "LONG" else "📉"
            st = "✅ TP" if status == "closed_tp" else "❌ SL" if status == "closed_sl" else "⏳ Открыт"

            line = f"<b>#{hashtag}</b> {pair} {direction} {emoji}\n"
            line += f"Вход: <code>{entry:,.4f}</code> | "
            if close_p:
                line += f"Закрыто: <code>{close_p:,.4f}</code> — {st}\n"
            else:
                line += f"TP: <code>{tp:,.4f}</code> | SL: <code>{sl:,.4f}</code>\n"
            line += f"Время: {ts[:16]}\n\n"
            text += line

        await message.answer(text, parse_mode="HTML")

    except Exception as e:
        logging.error(f"Ошибка истории: {e}")
        await message.answer("❌ Ошибка загрузки истории.")


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
    await message.answer("✅ Подписка активирована!", reply_markup=kb)


# ====================== ЗАПУСК ======================
async def main():
    await init_db()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Увеличенные интервалы для стабильности
    scheduler.add_job(generate_signals, 'interval', seconds=45, replace_existing=True)
    scheduler.add_job(monitor_open_signals, 'interval', seconds=30, replace_existing=True)

    scheduler.start()
    print("🚀 Бот успешно запущен | Улучшенная версия с анти-таймаутами")

    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
