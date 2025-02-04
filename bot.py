from telegram import Update, ReplyKeyboardMarkup, Message
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import Optional, Dict
import logging
import re
import asyncio
from cachetools import TTLCache, LRUCache
import nest_asyncio
from contextlib import asynccontextmanager
import os
import sys
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.pool import SimpleConnectionPool
from urllib.parse import urlparse
from signal import signal, SIGINT, SIGTERM, SIGABRT
import time
import telegram
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Apply nest_asyncio at startup
nest_asyncio.apply()

# Logging configuration - only log errors
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO  # Cambiado a INFO para ver más detalles
)

logger = logging.getLogger(__name__)

# Bot configuration
TOKEN = os.getenv('BOT_TOKEN')
if not TOKEN:
    print("Trying alternative token variable name...")
    TOKEN = os.getenv('TOKEN')  # Intentar con el nombre alternativo

ADMIN_ID = os.getenv('ADMIN_ID')
USDT_ADDRESS = os.getenv('USDT_ADDRESS')

# Debug logging
print("\nEnvironment variables:")
print(f"BOT_TOKEN/TOKEN: {TOKEN}")
print(f"ADMIN_ID: {ADMIN_ID}")
print(f"USDT_ADDRESS: {USDT_ADDRESS}")

# Lista todas las variables de entorno disponibles (sin valores sensibles)
print("\nAvailable environment variables:")
for key in os.environ.keys():
    print(f"- {key}")

if not all([TOKEN, ADMIN_ID, USDT_ADDRESS]):
    missing = []
    if not TOKEN:
        missing.append("BOT_TOKEN/TOKEN")
    if not ADMIN_ID:
        missing.append("ADMIN_ID")
    if not USDT_ADDRESS:
        missing.append("USDT_ADDRESS")
    raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

# Rewards system
REWARDS = {
    "claim": Decimal("1"),
    "daily": Decimal("5"),
    "referral": Decimal("3"),
    "min_withdraw": Decimal("36"),
    "network_fee": Decimal("2"),
    "min_referrals": 10
}

class DatabasePool:
    def __init__(self, pool_size=50):  # Aumentado el tamaño del pool
        self.pool_size = pool_size
        self.pool = None
        self.user_cache = TTLCache(maxsize=10000, ttl=300)
        self._lock = asyncio.Lock()
        self._connection_semaphore = asyncio.Semaphore(pool_size)

    async def initialize(self):
        """Initialize database pool with better connection management"""
        try:
            DATABASE_URL = os.getenv('DATABASE_URL')
            if not DATABASE_URL:
                raise ValueError("DATABASE_URL environment variable is required")

            url = urlparse(DATABASE_URL)
            self.pool = SimpleConnectionPool(
                5,  # minconn
                self.pool_size,  # maxconn
                user=url.username,
                password=url.password,
                host=url.hostname,
                port=url.port,
                database=url.path[1:],
                sslmode='require'
            )
            
            # Initialize tables
            self._initialize_tables()
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise

    def _initialize_tables(self):
        """Initialize database tables"""
        try:
            conn = self.pool.getconn()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id TEXT PRIMARY KEY,
                        username TEXT,
                        balance TEXT DEFAULT '0',
                        total_earned TEXT DEFAULT '0',
                        referrals INTEGER DEFAULT 0,
                        referred_by TEXT,
                        last_claim TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_daily TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        wallet TEXT,
                        join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (referred_by) REFERENCES users(user_id)
                    )
                """)
                # Verificar si la columna join_date existe
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'users' AND column_name = 'join_date'
                """)
                if not cur.fetchone():
                    # Si no existe, agregar la columna
                    cur.execute("""
                        ALTER TABLE users 
                        ADD COLUMN join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    """)
                conn.commit()
                logger.info("Database tables initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing tables: {e}")
            raise
        finally:
            if conn:
                self.pool.putconn(conn)

    def get_connection(self):
        """Get a database connection from the pool"""
        if not self.pool:
            raise Exception("Database pool not initialized")
        return self.pool.getconn()

    def put_connection(self, conn):
        """Return a connection to the pool"""
        if self.pool:
            self.pool.putconn(conn)

    @asynccontextmanager
    async def connection(self):
        """Mejorado manejo de conexiones con semáforo"""
        async with self._connection_semaphore:  # Limitar conexiones concurrentes
            conn = None
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    conn = self.get_connection()
                    if conn:
                        conn.autocommit = True
                        yield conn
                        return
                except psycopg2.OperationalError as e:
                    retry_count += 1
                    logger.warning(f"Connection attempt {retry_count} failed: {e}")
                    if conn:
                        try:
                            self.put_connection(conn)
                        except:
                            pass
                    if retry_count == max_retries:
                        raise
                    await asyncio.sleep(0.5 * retry_count)  # Backoff exponencial
                finally:
                    if conn:
                        self.put_connection(conn)

    async def get_user(self, user_id: str) -> Optional[Dict]:
        """Get user with connection retry"""
        # Check cache first
        if user_id in self.user_cache:
            return self.user_cache[user_id]
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.connection() as conn:
                    with conn.cursor(cursor_factory=DictCursor) as cur:
                        cur.execute("""
                            SELECT user_id, username, balance, total_earned, 
                                   referrals, last_claim, last_daily, wallet, 
                                   referred_by, join_date
                            FROM users 
                            WHERE user_id = %s
                        """, (user_id,))
                        
                        result = cur.fetchone()
                        if result:
                            user_data = dict(result)
                            user_data["last_claim"] = user_data["last_claim"].isoformat() if user_data["last_claim"] else None
                            user_data["last_daily"] = user_data["last_daily"].isoformat() if user_data["last_daily"] else None
                            user_data["join_date"] = user_data["join_date"].isoformat() if user_data["join_date"] else None
                            self.user_cache[user_id] = user_data
                            return user_data
                        return None
                        
            except psycopg2.OperationalError as e:
                if attempt == max_retries - 1:
                    logger.error(f"Final attempt failed for user {user_id}: {e}")
                    raise
                await asyncio.sleep(0.5 * (attempt + 1))
            except Exception as e:
                logger.error(f"Error getting user {user_id}: {e}")
                raise

    async def save_user(self, user_data: dict):
        """Save user with improved connection handling"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO users 
                            (user_id, username, balance, total_earned, referrals, 
                            last_claim, last_daily, wallet, referred_by, join_date)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (user_id) DO UPDATE SET
                            username = EXCLUDED.username,
                            balance = EXCLUDED.balance,
                            total_earned = EXCLUDED.total_earned,
                            referrals = EXCLUDED.referrals,
                            last_claim = EXCLUDED.last_claim,
                            last_daily = EXCLUDED.last_daily,
                            wallet = EXCLUDED.wallet,
                            referred_by = EXCLUDED.referred_by
                        """, (
                            user_data["user_id"],
                            user_data["username"],
                            str(Decimal(user_data["balance"])),
                            str(Decimal(user_data["total_earned"])),
                            user_data["referrals"],
                            datetime.fromisoformat(user_data["last_claim"]) if user_data["last_claim"] else None,
                            datetime.fromisoformat(user_data["last_daily"]) if user_data["last_daily"] else None,
                            user_data.get("wallet"),
                            user_data.get("referred_by"),
                            datetime.fromisoformat(user_data.get("join_date", datetime.now(UTC).isoformat()))
                        ))
                        self.user_cache[user_data["user_id"]] = user_data.copy()
                        return
            except psycopg2.OperationalError as e:
                if attempt == max_retries - 1:
                    logger.error(f"Final save attempt failed: {e}")
                    raise
                await asyncio.sleep(0.5 * (attempt + 1))
            except Exception as e:
                logger.error(f"Error saving user: {e}")
                raise

    async def optimize_db(self):
        """Optimize database performance for PostgreSQL"""
        async with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Cambiado a comandos PostgreSQL
                cur.execute("VACUUM ANALYZE users")

class USDTBot:
    def __init__(self):
        self.db_pool = DatabasePool(pool_size=20)
        self.admin_id = str(ADMIN_ID)
        self.user_cache = TTLCache(maxsize=10000, ttl=300)
        self.application = None
        self.blocked_users = set()
        self.is_running = True
        self._message_lock = asyncio.Lock()

    async def init_db(self):
        """Initialize database only"""
        await self.db_pool.initialize()
        logger.info("Database initialized successfully")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle messages with lock para prevenir race conditions"""
        if not update.message or not update.message.text:
            return

        async with self._message_lock:
            try:
                user_id = str(update.effective_user.id)
                text = update.message.text

                user_data = await self.get_user(user_id)
                if not user_data:
                    await self.start(update, context)
                    return

                # Handle commands with better error handling
                try:
                    if text == "💸 COLLECT 💸":
                        await self.handle_claim(update, user_data)
                    elif text == "💵 Daily Bonus":
                        await self.handle_daily(update, user_data)
                    elif text == "📊 Statistics":
                        await self.handle_balance(update, user_data)
                    elif text == "🤝 Community":
                        await self.handle_referral(update, context, user_data)
                    elif text == "💰 Withdraw":
                        await self.handle_withdraw(update, user_data)
                    elif text == "🏦 Wallet":
                        await self.handle_wallet(update)
                    elif text == "📈 Leaders":
                        await self.handle_ranking(update)
                    elif text == "📗 Help":
                        await self.handle_help(update)
                    else:
                        # Este es el mensaje que se muestra para cualquier texto no reconocido
                        await update.message.reply_text(
                            "❌ Command not recognized\n"
                            "──────────────────\n"
                            "🔄 Press /start to restart the bot\n"
                            "──────────────────\n"
                            "Need help? Use 📗 Help button"
                        )
                except Exception as e:
                    logger.error(f"Command handling error: {e}")
                    await update.message.reply_text("❌ Please try again in a moment.")
            except Exception as e:
                logger.error(f"Message handling error: {e}")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle start command and referral"""
        if not update.message:
            return

        user = update.effective_user
        user_id = str(user.id)
        
        try:
            # Verificar si el usuario ya existe
            user_data = await self.get_user(user_id)
            
            # Si es un usuario nuevo
            if not user_data:
                # Procesar referido si existe
                referred_by = None
                if context.args:
                    referrer_id = context.args[0]
                    if referrer_id != user_id:  # Evitar auto-referidos
                        referrer_data = await self.get_user(referrer_id)
                        if referrer_data:
                            referred_by = referrer_id
                            # Actualizar referidor
                            referrer_data["referrals"] = int(referrer_data.get("referrals", 0)) + 1
                            referrer_balance = Decimal(referrer_data["balance"]) + REWARDS["referral"]
                            referrer_total = Decimal(referrer_data["total_earned"]) + REWARDS["referral"]
                            referrer_data.update({
                                "balance": str(referrer_balance),
                                "total_earned": str(referrer_total)
                            })
                            await self.save_user(referrer_data)
                            
                            # Notificar al referidor
                            try:
                                await context.bot.send_message(
                                    chat_id=referrer_id,
                                    text=f"🎉 New Referral!\n"
                                         f"User: @{user.username or 'Anonymous'}\n"
                                         f"Reward: +{REWARDS['referral']} USDT"
                                )
                            except Exception as e:
                                logger.error(f"Failed to notify referrer: {e}")

                # Crear nuevo usuario
                user_data = {
                    "user_id": user_id,
                    "username": user.username or "Anonymous",
                    "balance": str(REWARDS["referral"] if referred_by else "0"),
                    "total_earned": str(REWARDS["referral"] if referred_by else "0"),
                    "referrals": 0,
                    "referred_by": referred_by,
                    "last_claim": datetime.now(UTC).isoformat(),
                    "last_daily": datetime.now(UTC).isoformat(),
                    "wallet": None,
                    "join_date": datetime.now(UTC).isoformat()
                }
                await self.save_user(user_data)

            # Mensaje de bienvenida
            keyboard = [
                ["💸 COLLECT 💸"],  # Botón más grande y destacado
                ["💵 Daily Bonus", "📊 Statistics"],
                ["🤝 Community", "💰 Withdraw"],
                ["🏦 Wallet", "📈 Leaders"],
                ["📗 Help"]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            welcome_text = (
                f"💎 {'Welcome' if not user_data.get('referred_by') else 'Welcome! +3 USDT Bonus'}\n"
                f"──────────────────\n"
                f"💰 Balance: {user_data['balance']} USDT\n"
                f"🤝 Community: {user_data['referrals']}\n"
                f"──────────────────\n"
                f"Start earning now! 💹"
            )
            
            await update.message.reply_text(welcome_text, reply_markup=reply_markup)

        except Exception as e:
            logger.error(f"Error in start: {e}")
            await update.message.reply_text("❌ An error occurred. Please try again!")

    async def handle_claim(self, update: Update, user_data: dict):
        """Handle claim command"""
        try:
            now = datetime.now(UTC)
            last_claim = datetime.fromisoformat(user_data["last_claim"])
            
            if now.replace(tzinfo=None) - last_claim.replace(tzinfo=None) < timedelta(minutes=5):
                time_left = timedelta(minutes=5) - (now.replace(tzinfo=None) - last_claim.replace(tzinfo=None))
                minutes = int(time_left.total_seconds() // 60)
                seconds = int(time_left.total_seconds() % 60)
                
                await update.message.reply_text(
                    f"⏳ Next Reward Available In:\n"
                    f"──────────────────\n"
                    f"⌚ {minutes}m {seconds}s\n"
                    f"──────────────────\n"
                    f"💡 Come back later!"
                )
                return

            # Update balance
            new_balance = Decimal(user_data["balance"]) + REWARDS["claim"]
            new_total = Decimal(user_data["total_earned"]) + REWARDS["claim"]
            
            # Update user data
            user_data.update({
                "balance": str(new_balance),
                "total_earned": str(new_total),
                "last_claim": now.isoformat()
            })
            
            # Save to database
            await self.save_user(user_data)
            
            await update.message.reply_text(
                f"💸 Reward Collected!\n"
                f"──────────────────\n"
                f"💰 Earned: {REWARDS['claim']} USDT\n"
                f"💵 Balance: {new_balance:.2f} USDT\n"
                f"──────────────────\n"
                f"⏱ Next reward in 5 minutes"
            )
            
        except Exception as e:
            logger.error(f"Error in claim handler: {e}")
            await update.message.reply_text("❌ An error occurred. Please try again!")

    async def handle_daily(self, update: Update, user_data: dict):
        """Handle daily command"""
        try:
            now = datetime.now(UTC)
            last_daily = datetime.fromisoformat(user_data["last_daily"])
            
            if now.replace(tzinfo=None) - last_daily.replace(tzinfo=None) < timedelta(days=1):
                time_left = timedelta(days=1) - (now.replace(tzinfo=None) - last_daily.replace(tzinfo=None))
                hours = int(time_left.total_seconds() // 3600)
                minutes = int((time_left.total_seconds() % 3600) // 60)
                
                await update.message.reply_text(
                    f"⏳ Next Daily Bonus In:\n"
                    f"──────────────────\n"
                    f"⌚ {hours}h {minutes}m\n"
                    f"──────────────────\n"
                    f"💡 Come back tomorrow!"
                )
                return

            # Update balance
            new_balance = Decimal(user_data["balance"]) + REWARDS["daily"]
            new_total = Decimal(user_data["total_earned"]) + REWARDS["daily"]
            
            # Update user data
            user_data.update({
                "balance": str(new_balance),
                "total_earned": str(new_total),
                "last_daily": now.isoformat()
            })
            
            # Save to database
            await self.save_user(user_data)
            
            await update.message.reply_text(
                f"💵 Daily Bonus Collected!\n"
                f"──────────────────\n"
                f"💰 Earned: {REWARDS['daily']} USDT\n"
                f"💵 Balance: {new_balance:.2f} USDT\n"
                f"──────────────────\n"
                f"⏱ Next bonus in 24 hours"
            )
            
        except Exception as e:
            logger.error(f"Error in daily handler: {e}")
            await update.message.reply_text("❌ An error occurred. Please try again!")

    async def handle_balance(self, update: Update, user_data: dict):
        await update.message.reply_text(
            f"📊 Your Statistics:\n"
            f"──────────────────\n"
            f"💰 Balance: {user_data['balance']} USDT\n"
            f"🤝 Community: {user_data['referrals']}\n"
            f"💵 Total earned: {user_data['total_earned']} USDT"
        )

    async def handle_referral(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: dict):
        ref_link = f"https://t.me/{context.bot.username}?start={user_data['user_id']}"
        await update.message.reply_text(
            f"🤝 Community: Your referral link:\n{ref_link}\n\n"
            f"Current referrals: {user_data['referrals']}\n"
            f"Reward per referral: {REWARDS['referral']} USDT\n\n"
            f"✨ You and your referral get {REWARDS['referral']} USDT!"
        )

    async def handle_withdraw(self, update: Update, user_data: dict):
        """Handle withdraw command"""
        if not user_data.get("wallet"):
            await update.message.reply_text(
                "🏦 Please set your USDT wallet address first!\n"
                "Use the 🏦 Wallet button to connect your wallet."
            )
            return

        # Get current balance and referrals
        balance = Decimal(user_data["balance"])
        referrals = user_data["referrals"]

        # First message: Requirements overview
        await update.message.reply_text(
            f"🎯 Withdrawal Requirements\n"
            f"──────────────────\n"
            f"📌 Minimum Balance: {REWARDS['min_withdraw']} USDT\n"
            f"📌 Required Referrals: {REWARDS['min_referrals']}\n"
            f"──────────────────\n"
            f"💼 Your Status:\n"
            f"💰 Balance: {balance:.2f} USDT\n"
            f"🤝 Community: {referrals}\n"
            f"──────────────────\n"
            f"📱 Required Channels:\n"
            f"• @USDT_Community_Tracker\n"
            f"• @USDT_Community_News\n"
            f"• @USDT_Community_QA"
        )

        # Check requirements and show appropriate message
        if referrals < REWARDS["min_referrals"]:
            await update.message.reply_text(
                f"⚠️ Referral Requirement Not Met\n"
                f"──────────────────\n"
                f"• Need: {REWARDS['min_referrals']} referrals\n"
                f"• Have: {referrals} referrals\n\n"
                f"📢 Share your referral link to earn more!"
            )
            return

        if balance < REWARDS["min_withdraw"]:
            await update.message.reply_text(
                f"⚠️ Balance Requirement Not Met\n"
                f"──────────────────\n"
                f"• Need: {REWARDS['min_withdraw']} USDT\n"
                f"• Have: {balance:.2f} USDT\n\n"
                f"💡 Keep collecting rewards to reach the minimum!"
            )
            return

        # If all requirements are met
        await update.message.reply_text(
            f"✅ Withdrawal Request\n"
            f"──────────────────\n"
            f" Amount: {balance:.2f} USDT\n"
            f"🏦 Wallet: {user_data['wallet']}\n"
            f"🌐 Network: USDT Network\n"
            f"──────────────────\n"
            f"📌 Network Fee: {REWARDS['network_fee']} USDT\n"
            f"💫 Total to Receive: {balance - REWARDS['network_fee']:.2f} USDT\n"
            f"──────────────────\n"
            f"📤 Send fee to this address:\n"
            f"`{USDT_ADDRESS}`\n"
            f"──────────────────\n"
            f"⏱ Processing Time: 5-15 minutes\n"
            f"💡 Important:\n"
            f"• Send exact fee amount\n"
            f"• Use USDT Network only\n"
            f"• Withdrawal processed after fee"
        )

    async def handle_wallet(self, update: Update):
        await update.message.reply_text(
            " Send your USDT (USDT) wallet address:\n\n"
            "⚠️ IMPORTANT WARNING:\n"
            "• Double check your USDT address carefully\n"
            "• Incorrect addresses will result in permanent loss of funds\n"
            "• We are not responsible for funds sent to wrong addresses\n\n"
            
        )

    async def handle_ranking(self, update: Update):
        """Handle the leaders command"""
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute("""
                    SELECT username, total_earned, referrals 
                    FROM users 
                    ORDER BY CAST(total_earned AS DECIMAL) DESC 
                    LIMIT 10
                """)
                rows = cur.fetchall()

                if not rows:
                    await update.message.reply_text("No leaders yet!")
                    return

                message = "📈 Top 10 Leaders:\n\n"
                for i, row in enumerate(rows, 1):
                    username = row['username'] or "Anonymous"
                    total_earned = Decimal(row['total_earned'])
                    referrals = row['referrals']
                    
                    message += (
                        f"{i}. @{username}\n"
                        f"💰 Earned: {total_earned:.2f} USDT\n"
                        f"🤝 Community: {referrals}\n\n"
                    )

                await update.message.reply_text(message)

        except Exception as e:
            logger.error(f"Error in ranking handler: {e}")
            await update.message.reply_text(
                "❌ Error loading leaderboard. Please try again later!"
            )

    async def handle_help(self, update: Update):
        await update.message.reply_text(
            "💰 Welcome to Rewards Bot!\n\n"
            "💸 Earning Methods:\n"
            "• 💵 Quick Rewards (5min)\n"
            "• 💰 Daily Bonus (24h)\n"
            "• 🤝 Community Program\n\n"
            "💎 Withdrawal Info:\n"
            "• 🏦 Network: USDT Network\n"
            "• ⏱ Processing: 5-15 minutes\n\n"
            "📱 Official Channel:\n"
            "• @USDT_Community_Official\n\n"
            "🔐 Security Notice:\n"
            "• Always verify wallet addresses\n"
            "• Never share personal information"
        )

    async def handle_admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin commands"""
        if not update.message:
            return

        user_id = str(update.effective_user.id)
        
        # Debug log para verificar IDs
        logger.info(f"Admin command attempt - User ID: {user_id}, Admin ID: {self.admin_id}")
        
        if user_id != self.admin_id:
            logger.warning(f"Unauthorized admin access attempt from user {user_id}")
            await update.message.reply_text("❌ Unauthorized access")
            return

        try:
            if not context.args:
                await update.message.reply_text(
                    "📋 Admin Commands:\n"
                    "──────────────────\n"
                    "1️⃣ /admin stats\n"
                    "2️⃣ /admin broadcast <message>\n"
                    "3️⃣ /admin addbalance <user_id> <amount>\n"
                    "4️⃣ /admin removeuser <user_id>\n"
                    "──────────────────"
                )
                return

            command = context.args[0].lower()
            
            # Debug log para comando
            logger.info(f"Admin command: {command} with args: {context.args}")

            if command == "stats":
                await self.handle_admin_stats(update)
                
            elif command == "broadcast":
                if len(context.args) < 2:
                    await update.message.reply_text("❌ Please provide a message to broadcast")
                    return
                message = ' '.join(context.args[1:])
                await self.handle_admin_broadcast(update, message)
                
            elif command == "addbalance":
                if len(context.args) != 3:
                    await update.message.reply_text("❌ Format: /admin addbalance <user_id> <amount>")
                    return
                target_user_id = context.args[1]
                amount = context.args[2]
                await self.handle_admin_add_balance(update, target_user_id, amount)
                
            elif command == "removeuser":
                if len(context.args) != 2:
                    await update.message.reply_text("❌ Format: /admin removeuser <user_id>")
                    return
                target_user_id = context.args[1]
                await self.handle_admin_remove_user(update, target_user_id)
                
            else:
                await update.message.reply_text("❌ Unknown command. Use /admin for help.")
                
        except Exception as e:
            logger.error(f"Admin command error: {e}")
            await update.message.reply_text(
                "❌ Error executing command\n"
                "Check logs for details"
            )

    async def handle_admin_stats(self, update: Update):
        """Handle admin stats command"""
        try:
            async with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    # Total users
                    cur.execute("SELECT COUNT(*) FROM users")
                    total_users = cur.fetchone()[0]

                    # Total balance
                    cur.execute("SELECT SUM(CAST(balance AS DECIMAL)) FROM users")
                    total_balance = cur.fetchone()[0] or 0

                    # Active users (last 24h)
                    cur.execute("""
                        SELECT COUNT(*) FROM users 
                        WHERE last_claim > NOW() - INTERVAL '24 hours'
                    """)
                    active_users = cur.fetchone()[0]

                    # Total withdrawals
                    cur.execute("SELECT SUM(CAST(total_earned AS DECIMAL)) FROM users")
                    total_earned = cur.fetchone()[0] or 0

                    await update.message.reply_text(
                        f"📊 Bot Statistics\n"
                        f"──────────────────\n"
                        f"🤝 Community: {total_users:,}\n"
                        f"📱 Active Users (24h): {active_users:,}\n"
                        f"💰 Total Balance: {total_balance:.2f} USDT\n"
                        f"💎 Total Earned: {total_earned:.2f} USDT\n"
                        f"──────────────────"
                    )
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            await update.message.reply_text("❌ Error getting statistics")

    async def handle_admin_broadcast(self, update: Update, message: str):
        """Handle admin broadcast command"""
        if not message:
            await update.message.reply_text("❌ Please provide a message to broadcast")
            return

        try:
            async with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT user_id FROM users")
                    users = cur.fetchall()

                    sent = 0
                    failed = 0
                    for user in users:
                        try:
                            await self.application.bot.send_message(
                                chat_id=user[0],
                                text=f"📢 Announcement\n──────────────────\n{message}"
                            )
                            sent += 1
                            await asyncio.sleep(0.05)  # Prevent flood
                        except Exception:
                            failed += 1

                    await update.message.reply_text(
                        f"📨 Broadcast Results\n"
                        f"──────────────────\n"
                        f"✅ Sent: {sent}\n"
                        f"❌ Failed: {failed}\n"
                        f"📝 Total: {sent + failed}"
                    )
        except Exception as e:
            logger.error(f"Broadcast error: {e}")
            await update.message.reply_text("❌ Error sending broadcast")

    async def handle_admin_add_balance(self, update: Update, target_user_id: str, amount: str):
        """Handle admin add balance command"""
        try:
            amount = Decimal(amount)
            if amount <= 0:
                await update.message.reply_text("❌ Amount must be positive")
                return

            user_data = await self.get_user(target_user_id)
            if not user_data:
                await update.message.reply_text("❌ User not found")
                return

            # Update balance
            user_data["balance"] = str(Decimal(user_data["balance"]) + amount)
            await self.save_user(user_data)

            await update.message.reply_text(
                f"✅ Balance Added\n"
                f"──────────────────\n"
                f"🤝 User: {user_data['username']}\n"
                f"💰 Added: {amount} USDT\n"
                f"💎 New Balance: {user_data['balance']} USDT"
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid amount")
        except Exception as e:
            logger.error(f"Add balance error: {e}")
            await update.message.reply_text("❌ Error adding balance")

    async def handle_admin_remove_user(self, update: Update, target_user_id: str):
        """Handle admin remove user command"""
        try:
            async with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM users WHERE user_id = %s RETURNING username", (target_user_id,))
                    result = cur.fetchone()
                    conn.commit()

                    if result:
                        username = result[0]
                        if target_user_id in self.user_cache:
                            del self.user_cache[target_user_id]
                        await update.message.reply_text(f"✅ User @{username} removed successfully")
                    else:
                        await update.message.reply_text("❌ User not found")
        except Exception as e:
            logger.error(f"Remove user error: {e}")
            await update.message.reply_text("❌ Error removing user")

    async def get_user(self, user_id: str) -> Optional[Dict]:
        """Get user data from cache or database"""
        # Check cache first
        if user_id in self.user_cache:
            return self.user_cache[user_id]
        
        # Get from database
        try:
            async with self.db_pool.connection() as conn:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute("""
                        SELECT user_id, username, balance, total_earned, 
                               referrals, last_claim, last_daily, wallet, 
                               referred_by, join_date
                        FROM users 
                        WHERE user_id = %s
                    """, (user_id,))
                    
                    result = cur.fetchone()
                    if result:
                        # Convert to dict and cache
                        user_data = dict(result)
                        # Convert datetime to ISO format string
                        user_data["last_claim"] = user_data["last_claim"].isoformat() if user_data["last_claim"] else None
                        user_data["last_daily"] = user_data["last_daily"].isoformat() if user_data["last_daily"] else None
                        user_data["join_date"] = user_data["join_date"].isoformat() if user_data["join_date"] else None
                        # Cache the result
                        self.user_cache[user_id] = user_data
                        return user_data
                    return None
                    
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            return None

    async def save_user(self, user_data: dict):
        """Save user data to database"""
        try:
            async with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO users 
                        (user_id, username, balance, total_earned, referrals, 
                        last_claim, last_daily, wallet, referred_by, join_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        balance = EXCLUDED.balance,
                        total_earned = EXCLUDED.total_earned,
                        referrals = EXCLUDED.referrals,
                        last_claim = EXCLUDED.last_claim,
                        last_daily = EXCLUDED.last_daily,
                        wallet = EXCLUDED.wallet,
                        referred_by = EXCLUDED.referred_by
                    """, (
                        user_data["user_id"],
                        user_data["username"],
                        str(Decimal(user_data["balance"])),
                        str(Decimal(user_data["total_earned"])),
                        user_data["referrals"],
                        datetime.fromisoformat(user_data["last_claim"]) if user_data["last_claim"] else None,
                        datetime.fromisoformat(user_data["last_daily"]) if user_data["last_daily"] else None,
                        user_data.get("wallet"),
                        user_data.get("referred_by"),
                        datetime.fromisoformat(user_data.get("join_date", datetime.now(UTC).isoformat()))
                    ))
                    conn.commit()
                    self.user_cache[user_data["user_id"]] = user_data.copy()
        except Exception as e:
            logger.error(f"Error saving user: {e}")
            raise

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors"""
    logger.error(f"Update {update} caused error {context.error}")
    try:
        if update.effective_message:
            await update.effective_message.reply_text(
                "❌ An error occurred. Please try again later!"
            )
    except Exception as e:
        logger.error(f"Failed to send error message: {e}")

def main():
    """Start the bot"""
    # Create application
    application = Application.builder().token(TOKEN).build()
    bot = USDTBot()
    bot.application = application
    
    # Initialize database
    asyncio.get_event_loop().run_until_complete(bot.init_db())

    # Add handlers - Asegurarse que el comando admin esté registrado primero
    application.add_handler(CommandHandler("admin", bot.handle_admin_command))
    application.add_handler(CommandHandler("start", bot.start))
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, 
        bot.handle_message
    ))

    # Add error handler
    application.add_error_handler(error_handler)

    logger.info(f"Bot started. Admin ID: {bot.admin_id}")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()