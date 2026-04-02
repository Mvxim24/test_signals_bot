import asyncio
import logging
import os
import signal
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd
import ccxt.pro as ccxtpro
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.session.aiohttp import AiohttpSession
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")

if not TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в .env!")

db_path = "signals.db"

bot = Bot(token=TOKEN, session=AiohttpSession())
dp = Dispatcher()

# ==================== Bybit WebSocket ====================
exchange = ccxtpro.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})

last_signal_time = defaultdict(lambda: datetime(2000, 1, 1, tzinfo=timezone.utc))
is_generating = False
generate_lock = asyncio.Lock()

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
            INSERT OR REPLACE INTO subscribers 
            (user_id, username, first_name, subscribed_at)
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


# ====================== ВСПОМОГАТЕЛЬНЫЕ ======================
def get_tick_size(symbol: str) -> float:
    try:
        return float(exchange.markets[symbol]['precision']['price'])
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
    await db.execute("UPDATE signals SET hashtag = ? WHERE id = ?", (hashtag, signal_id))
    await db.commit()

    emoji = "📈" if direction == "LONG" else "📉"
    direction_text = "LONG ▲" if direction == "LONG" else "SHORT ▼"
    tp_p = ((tp - entry_price) / entry_price) * 100
    sl_p = ((sl - entry_price) / entry_price) * 100

    text = f"""🚨 <b>НОВЫЙ ТОРГОВЫЙ СИГНАЛ #{hashtag}</b>

{emoji} <b>{pair}</b> — <b>{direction_text}</b> {emoji}

──────────────────
💰 Вход: <code>{entry_price:,.2f} USDT</code>
🎯 TP: <code>{tp:,.2f} USDT</code> <b>({tp_p:+.2f}%)</b>
🛑 SL: <code>{sl:,.2f} USDT</code> <b>({sl_p:+.2f}%)</b>

🕒 {now.strftime('%d.%m.%Y %H:%M:%S UTC')}
📍 Bybit

🔍 #{hashtag} | Стратегия от Максима"""

    await broadcast_message(text)
    print(f"✅ Сигнал → {pair} | {direction} | Entry: {entry_price}")


# ====================== WebSocket: Генерация сигналов ======================
async def watch_ohlcv_and_generate():
    global is_generating
    pairs = ["BTC/USDT", "ETH/USDT"]
    timeframe = '15m'

    while True:
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Подключение OHLCV WebSocket Bybit...")
            for pair in pairs:
                if is_generating:
                    await asyncio.sleep(0.5)
                    continue

                async with generate_lock:
                    is_generating = True
                    try:
                        ohlcv = await exchange.watch_ohlcv(pair, timeframe, limit=300)
                        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])

                        if len(df) < 210:
                            continue

                        df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
                        curr = df.iloc[-1]
                        prev = df.iloc[-2]
                        price = float(curr['close'])
                        ema200 = float(curr['ema200'])

                        close_on_ema = abs(price - ema200) / ema200 <= 0.0005

                        direction = None
                        entry = price

                        if (prev['close'] >= prev['ema200'] and curr['close'] <= curr['ema200']) and close_on_ema:
                            if price <= ema200 * 0.995:
                                direction = "LONG"
                                sl = round_price(entry * 0.992, get_tick_size(pair))
                                tp = round_price(entry * 1.012, get_tick_size(pair))

                        elif (prev['close'] <= prev['ema200'] and curr['close'] >= curr['ema200']) and close_on_ema:
                            if price >= ema200 * 1.005:
                                direction = "SHORT"
                                sl = round_price(entry * 1.008, get_tick_size(pair))
                                tp = round_price(entry * 0.988, get_tick_size(pair))

                        if direction:
                            async with aiosqlite.connect(db_path) as db:
                                open_pairs = {row[0] for row in await db.execute_fetchall(
                                    "SELECT pair FROM signals WHERE status = 'open'")}
                            if pair not in open_pairs:
                                await send_signal(pair, direction, entry, tp, sl)
                    finally:
                        is_generating = False
                await asyncio.sleep(0.3)
        except Exception as e:
            logging.error(f"OHLCV WebSocket ошибка: {e}")
            await asyncio.sleep(5)


# ====================== WebSocket: Мониторинг TP/SL ======================
async def watch_tickers_and_monitor():
    pairs = ["BTC/USDT", "ETH/USDT"]
    while True:
        try:
            tickers = {}
            for pair in pairs:
                ticker = await exchange.watch_ticker(pair)
                tickers[pair] = float(ticker['last'])

            async with aiosqlite.connect(db_path) as db:
                rows = await db.execute_fetchall("""
                    SELECT id, pair, direction, tp, sl, hashtag, entry_price 
                    FROM signals WHERE status = 'open'
                """)

            for row in rows:
                signal_id, pair, direction, tp, sl, hashtag, entry = row
                if pair not in tickers:
                    continue
                current_price = tickers[pair]
                closed = False
                status = None

                if direction == "LONG":
                    if current_price >= tp: 
                        status = "closed_tp"
                        closed = True
                    elif current_price <= sl: 
                        status = "closed_sl"
                        closed = True
                else:
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
Закрытие: <b>{current_price:,.2f} USDT</b>
Вход: <b>{entry:,.2f} USDT</b>
Биржа: Bybit"""

                    await broadcast_message(text)
                    print(f"✅ Закрыт сигнал #{hashtag} | {status}")

        except Exception as e:
            logging.error(f"Ticker WebSocket ошибка: {e}")
            await asyncio.sleep(3)


# ====================== GRACEFUL SHUTDOWN ======================
async def shutdown():
    print("⚠️ Graceful shutdown запущен...")
    try:
        await exchange.close()
    except:
        pass
    try:
        await bot.session.close()
    except:
        pass
    print("✅ Бот завершён корректно.")


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
        f"👋 Привет, {message.from_user.first_name}!\n\n"
        "🤖 Бот работает на **Bybit** через WebSocket.\n"
        "Стратегия: EMA 200 Pullback (15m)",
        parse_mode="HTML",
        reply_markup=kb
    )


@dp.message(F.text == "📜 История сигналов")
async def show_history(message: types.Message):
    try:
        async with aiosqlite.connect(db_path) as db:
            history = await db.execute_fetchall("SELECT * FROM signals ORDER BY id DESC LIMIT 20")
        if not history:
            await message.answer("📭 Сигналов пока нет.")
            return

        text = "📜 <b>Последние 20 сигналов</b>\n\n"
        for row in history:
            _, pair, direction, entry, tp, sl, ts, status, close_p, hashtag = row
            emoji = "📈" if direction == "LONG" else "📉"
            st = "✅ TP" if status == "closed_tp" else "❌ SL" if status == "closed_sl" else "⏳ Открыт"
            line = f"<b>#{hashtag}</b> {pair} {direction} {emoji}\nВход: <code>{entry:,.2f}</code>\n"
            if close_p:
                line += f"Закрыто: <code>{close_p:,.2f}</code> — {st}\n"
            else:
                line += f"Статус: {st}\n"
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
    await message.answer("❌ Вы отписались.", reply_markup=kb)


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
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    print("🚀 Бот запущен | Bybit WebSocket | EMA 200 Pullback")
    print("✅ Polling запущен (оптимизировано для Bothost.ru)")

    # Обработка SIGTERM от хостинга
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))

    # Запускаем polling ПЕРВЫМ (важно для Bothost!)
    polling_task = asyncio.create_task(dp.start_polling(bot))

    # Даём polling время стабилизироваться
    await asyncio.sleep(3)

    # Запускаем WebSocket задачи в фоне
    try:
        await asyncio.gather(
            watch_ohlcv_and_generate(),
            watch_tickers_and_monitor(),
            return_exceptions=True
        )
    finally:
        polling_task.cancel()
        await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
