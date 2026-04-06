import asyncio
import logging
import os
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List, Optional

import pandas as pd
import ccxt
import ccxt.pro as ccxtpro  # Для асинхронной версии
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.session.aiohttp import AiohttpSession
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

# ====================== КОНФИГУРАЦИЯ ======================
class Config:
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", 0))
    DB_PATH: str = "signals.db"
    
    # Стратегия
    PAIRS: List[str] = ["BTC/USDT", "ETH/USDT"]          # Добавляй сюда новые пары
    TIMEFRAME: str = "15m"
    EMA_PERIOD: int = 200
    COOLDOWN_SECONDS: int = 2700                         # 45 минут между сигналами по одной паре
    
    # Параметры сделки
    TP_PERCENT: float = 1.2
    SL_PERCENT: float = 0.8
    PULLBACK_THRESHOLD: float = 0.5                      # в процентах
    
    # Расписания
    GENERATE_INTERVAL: int = 20      # секунд
    MONITOR_INTERVAL: int = 25       # секунд


if not Config.TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не найден в .env!")

# ====================== ИНИЦИАЛИЗАЦИЯ ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = Bot(token=Config.TELEGRAM_TOKEN, session=AiohttpSession())
dp = Dispatcher()

# Асинхронный exchange (рекомендуется)
exchange = ccxtpro.mexc({
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})

# Глобальные состояния
last_signal_time: Dict[str, datetime] = defaultdict(
    lambda: datetime(2000, 1, 1, tzinfo=timezone.utc)
)

# Один коннект к БД + lock для безопасности
db_conn: Optional[aiosqlite.Connection] = None
db_lock = asyncio.Lock()


# ====================== БАЗА ДАННЫХ ======================
async def init_db() -> None:
    global db_conn
    db_conn = await aiosqlite.connect(Config.DB_PATH)
    await db_conn.execute('''
        CREATE TABLE IF NOT EXISTS subscribers (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            subscribed_at TEXT
        )
    ''')
    await db_conn.execute('''
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
    await db_conn.commit()
    logger.info("✅ База данных инициализирована")


async def close_db() -> None:
    global db_conn
    if db_conn:
        await db_conn.close()


# ====================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======================
async def get_tick_size(symbol: str) -> float:
    try:
        markets = await exchange.load_markets()
        return float(markets[symbol]['precision']['price'])
    except Exception as e:
        logger.warning(f"Не удалось получить tick_size для {symbol}: {e}")
        return 0.01


def round_price(price: float, tick_size: float) -> float:
    return round(price / tick_size) * tick_size


# ====================== ОТПРАВКА СИГНАЛА ======================
async def send_signal(pair: str, direction: str, entry_price: float, tp: float, sl: float) -> None:
    key = f"{pair}_{direction}"
    now = datetime.now(timezone.utc)

    if (now - last_signal_time[key]).total_seconds() < Config.COOLDOWN_SECONDS:
        return

    last_signal_time[key] = now

    tick_size = await get_tick_size(pair)
    entry_price = round_price(entry_price, tick_size)
    tp = round_price(tp, tick_size)
    sl = round_price(sl, tick_size)

    async with db_lock:
        cursor = await db_conn.execute('''
            INSERT INTO signals (pair, direction, entry_price, tp, sl, timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?, 'open')
        ''', (pair, direction, entry_price, tp, sl, now.isoformat()))
        await db_conn.commit()

        signal_id = cursor.lastrowid

        hashtag = f"SIG_{signal_id:04d}"
        await db_conn.execute(
            "UPDATE signals SET hashtag = ? WHERE id = ?",
            (hashtag, signal_id)
        )
        await db_conn.commit()

    # Формирование сообщения
    emoji = "📈" if direction == "LONG" else "📉"
    direction_text = "LONG ▲" if direction == "LONG" else "SHORT ▼"

    tp_p = ((tp - entry_price) / entry_price) * 100
    sl_p = ((sl - entry_price) / entry_price) * 100

    text = f"""🚨 <b>НОВЫЙ ТОРГОВЫЙ СИГНАЛ #{hashtag}</b>

{emoji} <b>{pair}</b> — <b>{direction_text}</b> {emoji}

──────────────────
💰 <b>Цена входа:</b> <code>{entry_price:,.2f} USDT</code>
🎯 <b>Take Profit:</b> <code>{tp:,.2f} USDT</code> <b>(+{tp_p:.2f}%)</b>
🛑 <b>Stop Loss:</b> <code>{sl:,.2f} USDT</code> <b>({sl_p:.2f}%)</b>

🕒 <b>Время сигнала:</b> {now.strftime('%d.%m.%Y %H:%M:%S UTC')}

🔍 <b>#{hashtag}</b>"""

    await broadcast_message(text)
    logger.info(f"✅ Сигнал отправлен → {pair} | {direction} | Entry: {entry_price}")


# ====================== ГЕНЕРАЦИЯ СИГНАЛОВ ======================
async def generate_signals() -> None:
    try:
        # Получаем открытые сигналы
        async with db_lock:
            rows = await db_conn.execute_fetchall(
                "SELECT pair FROM signals WHERE status = 'open'"
            )
            open_pairs = {row[0] for row in rows}

        for pair in Config.PAIRS:
            if pair in open_pairs:
                continue

            try:
                ohlcv = await exchange.fetch_ohlcv(pair, Config.TIMEFRAME, limit=300)
                df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])

                df['ema200'] = df['close'].ewm(span=Config.EMA_PERIOD, adjust=False).mean()

                if len(df) < Config.EMA_PERIOD + 1 or pd.isna(df.iloc[-1]['ema200']):
                    continue

                ticker = await exchange.fetch_ticker(pair)
                curr_price = float(ticker['last'])

                last = df.iloc[-1]
                prev = df.iloc[-2]
                ema = float(last['ema200'])

                direction = None
                pullback = Config.PULLBACK_THRESHOLD / 100

                # LONG
                if (prev['close'] > ema and
                    last['low'] <= ema and
                    last['close'] <= ema * 1.001 and
                    curr_price <= ema * (1 - pullback)):
                    direction = "LONG"
                    tp = curr_price * (1 + Config.TP_PERCENT / 100)
                    sl = curr_price * (1 - Config.SL_PERCENT / 100)

                # SHORT
                elif (prev['close'] < ema and
                      last['high'] >= ema and
                      last['close'] >= ema * 0.999 and
                      curr_price >= ema * (1 + pullback)):
                    direction = "SHORT"
                    tp = curr_price * (1 - Config.TP_PERCENT / 100)
                    sl = curr_price * (1 + Config.SL_PERCENT / 100)

                if direction:
                    await send_signal(pair, direction, curr_price, tp, sl)

            except Exception as e:
                logger.error(f"Ошибка анализа пары {pair}: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Критическая ошибка в generate_signals: {e}", exc_info=True)


# ====================== МОНИТОРИНГ TP/SL ======================
async def monitor_open_signals() -> None:
    try:
        async with db_lock:
            rows = await db_conn.execute_fetchall("""
                SELECT id, pair, direction, tp, sl, hashtag, entry_price 
                FROM signals WHERE status = 'open'
            """)

        for row in rows:
            signal_id, pair, direction, tp, sl, hashtag, entry = row
            try:
                ticker = await exchange.fetch_ticker(pair)
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
                    async with db_lock:
                        await db_conn.execute(
                            "UPDATE signals SET status = ?, close_price = ? WHERE id = ?",
                            (status, current_price, signal_id)
                        )
                        await db_conn.commit()

                    status_text = "✅ TAKE PROFIT" if status == "closed_tp" else "❌ STOP LOSS"
                    text = f"""📢 <b>Сигнал закрыт #{hashtag}</b>

{status_text}
Цена закрытия: <b>{current_price:,.2f} USDT</b>
Вход был: <b>{entry:,.2f} USDT</b>"""

                    await broadcast_message(text)
                    logger.info(f"✅ Сигнал закрыт → #{hashtag} | {status}")

            except Exception as e:
                logger.error(f"Ошибка мониторинга сигнала {hashtag}: {e}")

    except Exception as e:
        logger.error(f"Ошибка в monitor_open_signals: {e}", exc_info=True)


async def broadcast_message(text: str) -> None:
    async with db_lock:
        rows = await db_conn.execute_fetchall("SELECT user_id FROM subscribers")
        subscribers = [row[0] for row in rows]

    if not subscribers:
        return

    tasks = [bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True)
             for uid in subscribers]
    await asyncio.gather(*tasks, return_exceptions=True)


# ====================== ХЭНДЛЕРЫ (оставил почти без изменений) ======================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    async with db_lock:
        await db_conn.execute('''
            INSERT OR REPLACE INTO subscribers (user_id, username, first_name, subscribed_at)
            VALUES (?, ?, ?, ?)
        ''', (message.from_user.id, message.from_user.username, message.from_user.first_name,
              datetime.now(timezone.utc).isoformat()))
        await db_conn.commit()

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📜 История сигналов")],
            [KeyboardButton(text="❌ Отписаться")]
        ],
        resize_keyboard=True
    )
    await message.answer(
        f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n"
        "🤖 Бот использует стратегию **EMA 200 Pullback** на 15m.\n\n"
        "Сигналы приходят автоматически.",
        parse_mode="HTML",
        reply_markup=kb
    )


# Остальные хэндлеры (show_history, unsubscribe, subscribe_again) — аналогично,
# только используй async with db_lock и db_conn вместо нового connect()

# ====================== ЗАПУСК ======================
async def main():
    await init_db()

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(generate_signals, 'interval', seconds=Config.GENERATE_INTERVAL, replace_existing=True)
    scheduler.add_job(monitor_open_signals, 'interval', seconds=Config.MONITOR_INTERVAL, replace_existing=True)
    scheduler.start()

    logger.info("🚀 Бот успешно запущен | Стратегия: EMA 200 Pullback")

    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown()
        await close_db()
        await bot.session.close()
        await exchange.close()   # важно для ccxt.pro


if __name__ == "__main__":
    asyncio.run(main())
