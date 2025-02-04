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
from telegram import BotCommandScopeChat

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
ADMIN_ID = os.getenv('ADMIN_ID')
SUI_ADDRESS = os.getenv('SUI_ADDRESS')

if not all([TOKEN, ADMIN_ID, SUI_ADDRESS]):
    raise ValueError("Missing required environment variables")

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
    def __init__(self, pool_size=20, max_overflow=10, timeout=30):
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.timeout = timeout
        self.pool = []
        self.in_use = set()
        self.lock = asyncio.Lock()
        
    async def initialize(self):
        """Initialize the connection pool"""
        try:
            for _ in range(self.pool_size):
                conn = psycopg2.connect(
                    dbname=os.getenv('DB_NAME'),
                    user=os.getenv('DB_USER'),
                    password=os.getenv('DB_PASSWORD'),
                    host=os.getenv('DB_HOST'),
                    port=os.getenv('DB_PORT')
                )
                conn.autocommit = False
                self.pool.append(conn)
            
            logger.info(f"Database pool initialized with {self.pool_size} connections")
        except Exception as e:
            logger.error(f"Error initializing database pool: {e}")
            raise

    async def get_connection(self):
        """Get a connection from the pool with retry logic"""
        async with self.lock:
            for _ in range(3):  # Intentar 3 veces
                try:
                    # Intentar obtener una conexión disponible
                    for conn in self.pool:
                        if conn not in self.in_use:
                            if not conn.closed:
                                self.in_use.add(conn)
                                return conn
                            else:
                                # Reemplazar conexión cerrada
                                self.pool.remove(conn)
                                new_conn = psycopg2.connect(
                                    dbname=os.getenv('DB_NAME'),
                                    user=os.getenv('DB_USER'),
                                    password=os.getenv('DB_PASSWORD'),
                                    host=os.getenv('DB_HOST'),
                                    port=os.getenv('DB_PORT')
                                )
                                new_conn.autocommit = False
                                self.pool.append(new_conn)
                                self.in_use.add(new_conn)
                                return new_conn

                    # Si no hay conexiones disponibles y no excedemos max_overflow
                    if len(self.pool) < self.pool_size + self.max_overflow:
                        new_conn = psycopg2.connect(
                            dbname=os.getenv('DB_NAME'),
                            user=os.getenv('DB_USER'),
                            password=os.getenv('DB_PASSWORD'),
                            host=os.getenv('DB_HOST'),
                            port=os.getenv('DB_PORT')
                        )
                        new_conn.autocommit = False
                        self.pool.append(new_conn)
                        self.in_use.add(new_conn)
                        return new_conn

                    # Esperar un momento y reintentar
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error getting connection: {e}")
                    await asyncio.sleep(0.2)

            raise Exception("connection pool exhausted after retries")

    def put_connection(self, conn):
        """Return a connection to the pool"""
        try:
            if conn in self.in_use:
                self.in_use.remove(conn)
                if conn.closed:
                    self.pool.remove(conn)
                elif len(self.pool) > self.pool_size:
                    conn.close()
                    self.pool.remove(conn)
        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}")

    async def cleanup(self):
        """Clean up all connections"""
        for conn in self.pool:
            try:
                conn.close()
            except:
                pass
        self.pool.clear()
        self.in_use.clear()

class SUIBot:
    def __init__(self):
        # Validate environment variables
        self.token = os.getenv('BOT_TOKEN')
        self.admin_id = str(os.getenv('ADMIN_ID'))
        self.sui_address = os.getenv('SUI_ADDRESS')
        
        if not all([self.token, self.admin_id, self.sui_address]):
            raise ValueError("Missing required environment variables")
            
        # Initialize database and cache
        self.db_pool = DatabasePool(pool_size=50, max_overflow=20, timeout=30)
        self.user_cache = TTLCache(maxsize=10000, ttl=300)
        
        # Initialize keyboard with better layout
        self._keyboard = ReplyKeyboardMarkup([
            ["🌟 Collect", "📅 Daily Reward"],
            ["📊 My Stats", "👨‍👦‍👦 Invite"],
            ["💸 Cash Out", "🔑 SUI Address"],
            ["🏆 Leaders", "❓ Info"]
        ], resize_keyboard=True)
        
        self.notification_task = None
        self.mailing_in_progress = False
        self.stop_mailing = False
        self.mailing_task = None

        self.admin_commands = {
            '/stats': self.handle_stats,
            '/mailing': self.handle_mailing,
            '/broadcast': self.handle_mailing,  # alias para mailing
            '/admin': self.handle_admin
        }

    async def init_db(self):
        """Initialize database connection"""
        await self.db_pool.initialize()

    async def get_user(self, user_id: str) -> Optional[Dict]:
        """Get user data with improved error handling"""
        conn = None
        try:
            # Check cache first
            if user_id in self.user_cache:
                return self.user_cache[user_id]
            
            # Get from database
            conn = await self.db_pool.get_connection()
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute("""
                    SELECT * FROM users WHERE user_id = %s
                """, (user_id,))
                result = cur.fetchone()
                
                if result:
                    user_data = dict(result)
                    user_data["last_claim"] = user_data["last_claim"].isoformat()
                    user_data["last_daily"] = user_data["last_daily"].isoformat()
                    self.user_cache[user_id] = user_data
                    return user_data
                return None

        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            return None
        finally:
            if conn:
                self.db_pool.put_connection(conn)

    async def save_user(self, user_data: dict):
        """Save user data to database"""
        conn = None
        try:
            conn = await self.db_pool.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users 
                    (user_id, username, balance, total_earned, referrals, 
                    last_claim, last_daily, wallet, referred_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    user_data["balance"],
                    user_data["total_earned"],
                    user_data["referrals"],
                    datetime.fromisoformat(user_data["last_claim"]),
                    datetime.fromisoformat(user_data["last_daily"]),
                    user_data.get("wallet"),
                    user_data.get("referred_by")
                ))
                conn.commit()
                
                # Update cache
                self.user_cache[user_data["user_id"]] = user_data.copy()
                
        except Exception as e:
            logger.error(f"Error saving user data: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.db_pool.put_connection(conn)

    async def _notify_referrer(self, bot, referrer_id: str):
        """Notify referrer about new referral"""
        try:
            await bot.send_message(
                chat_id=referrer_id,
                text=(
                    f"👥 New referral joined!\n"
                    f"💰 You earned {REWARDS['referral']} SUI!"
                )
            )
        except Exception as e:
            logger.error(f"Failed to notify referrer {referrer_id}: {e}")

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
                                         f"Reward: +{REWARDS['referral']} SUI"
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
                ["🌟 Collect", "📅 Daily Reward"],
                ["📊 My Stats", "👨‍👦‍👦 Invite"],
                ["💸 Cash Out", "🔑 SUI Address"],
                ["🏆 Leaders", "❓ Info"]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            welcome_text = (
                f"👋 {'Welcome' if not user_data.get('referred_by') else 'Welcome! +3 SUI Bonus'}\n"
                f"──────────────────\n"
                f"💰 Balance: {user_data['balance']} SUI\n"
                f"👥 Referrals: {user_data['referrals']}\n"
                f"──────────────────\n"
                f"Start earning now! 🚀"
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
                    f"⏳ Next Bonus Available In:\n"
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
                f"✅ Bonus Collected!\n"
                f"──────────────────\n"
                f"💰 Earned: {REWARDS['claim']} SUI\n"
                f"💎 Balance: {new_balance:.2f} SUI\n"
                f"──────────────────\n"
                f"⏱ Next bonus in 5 minutes"
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
                    f"⏳ Next Daily Reward In:\n"
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
                f"✅ Daily Reward Collected!\n"
                f"──────────────────\n"
                f"💰 Earned: {REWARDS['daily']} SUI\n"
                f"💎 Balance: {new_balance:.2f} SUI\n"
                f"──────────────────\n"
                f"⏱ Next reward in 24 hours"
            )
            
        except Exception as e:
            logger.error(f"Error in daily handler: {e}")
            await update.message.reply_text("❌ An error occurred. Please try again!")

    async def handle_balance(self, update: Update, user_data: dict):
        await update.message.reply_text(
            f"📊 My Stats: {user_data['balance']} SUI\n"
            f"👨‍👦‍👦 Invite: {user_data['referrals']}\n"
            f"🌟 Total earned: {user_data['total_earned']} SUI"
        )

    async def handle_referral(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: dict):
        ref_link = f"https://t.me/{context.bot.username}?start={user_data['user_id']}"
        await update.message.reply_text(
            f"👨‍👦‍👦 Your referral link:\n{ref_link}\n\n"
            f"Current referrals: {user_data['referrals']}\n"
            f"Reward per referral: {REWARDS['referral']} SUI\n\n"
            f"✨ You and your referral get {REWARDS['referral']} SUI!"
        )

    async def handle_withdraw(self, update: Update, user_data: dict):
        """Handle withdraw command"""
        if not user_data.get("wallet"):
            await update.message.reply_text(
                "🔑 Please set your SUI wallet address first!\n"
                "Use the SUI Address button to connect your wallet."
            )
            return

        # Get current balance and referrals
        balance = Decimal(user_data["balance"])
        referrals = user_data["referrals"]

        # First message: Requirements overview
        await update.message.reply_text(
            f"🎯 Withdrawal Requirements\n"
            f"──────────────────\n"
            f"📌 Minimum Balance: {REWARDS['min_withdraw']} SUI\n"
            f"📌 Required Referrals: {REWARDS['min_referrals']}\n"
            f"──────────────────\n"
            f"💼 Your Status:\n"
            f"💰 Balance: {balance:.2f} SUI\n"
            f"👥 Referrals: {referrals}\n"
            f"──────────────────\n"
            f"📱 Required Channels:\n"
            f"• @SUI_Capital_Tracker\n"
            f"• @SUI_Capital_News\n"
            f"• @SUI_Capital_QA"
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
                f"• Need: {REWARDS['min_withdraw']} SUI\n"
                f"• Have: {balance:.2f} SUI\n\n"
                f"💡 Keep collecting rewards to reach the minimum!"
            )
            return

        # If all requirements are met
        await update.message.reply_text(
            f"✅ Withdrawal Request\n"
            f"──────────────────\n"
            f" Amount: {balance:.2f} SUI\n"
            f"🏦 Wallet: {user_data['wallet']}\n"
            f"🌐 Network: SUI Network\n"
            f"──────────────────\n"
            f"📌 Network Fee: {REWARDS['network_fee']} SUI\n"
            f"💫 Total to Receive: {balance - REWARDS[network_fee]:.2f} SUI\n"
            f"──────────────────\n"
            f"📤 Send fee to this address:\n"
            f"`{SUI_ADDRESS}`\n"
            f"──────────────────\n"
            f"⏱ Processing Time: 5-15 minutes\n"
            f"💡 Important:\n"
            f"• Send exact fee amount\n"
            f"• Use SUI Network only\n"
            f"• Withdrawal processed after fee"
        )

    async def handle_wallet(self, update: Update):
        await update.message.reply_text(
            "🔑 Send your SUI (SUI) wallet address:\n\n"
            "⚠️ IMPORTANT WARNING:\n"
            "• Double check your SUI address carefully\n"
            "• Incorrect addresses will result in permanent loss of funds\n"
            "• We are not responsible for funds sent to wrong addresses\n\n"
            
        )

    async def handle_ranking(self, update: Update):
        """Handle the leaders command"""
        try:
            conn = await self.db_pool.get_connection()
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

                message = "🏆 Top 10 Leaders:\n\n"
                for i, row in enumerate(rows, 1):
                    username = row['username'] or "Anonymous"
                    total_earned = Decimal(row['total_earned'])
                    referrals = row['referrals']
                    
                    message += (
                        f"{i}. @{username}\n"
                        f"💰 Earned: {total_earned:.2f} SUI\n"
                        f"👥 Referrals: {referrals}\n\n"
                    )

                await update.message.reply_text(message)

        except Exception as e:
            logger.error(f"Error in ranking handler: {e}")
            await update.message.reply_text(
                "❌ Error loading leaderboard. Please try again later!"
            )

    async def handle_help(self, update: Update):
        await update.message.reply_text(
            "🌟 Welcome to SUI Rewards Bot!\n\n"
            "💎 Earning Opportunities:\n"
            "• 🕒 Minutes Claim Bonus\n"
            "• 📅 Daily Reward (24h)\n"
            "• 👥 Referral Program\n\n"
            "💰 Withdrawal Information:\n"
            "• ⚡ Network: SUI (SUI)\n"
            "• ⏱ Processing: 5 minutes\n\n"
            "📱 Official Channel:\n"
            "• @SUI_Capital_Tracker\n\n"
            "🔐 Security Notice:\n"
            "• Always verify wallet addresses\n"
            "• Never share personal information"
        )

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all messages"""
        if not update.message or not update.message.text:
            return

        user_id = str(update.effective_user.id)
        text = update.message.text

        # Get user data
        user_data = await self.get_user(user_id)
        if not user_data:
            await self.start(update, context)
            return

        # Handle different commands
        if text == "🌟 Collect":
            await self.handle_claim(update, user_data)
        elif text == "📅 Daily Reward":
            await self.handle_daily(update, user_data)
        elif text == "📊 My Stats":
            await self.handle_balance(update, user_data)
        elif text == "👨‍👦‍👦 Invite":
            await self.handle_referral(update, context, user_data)
        elif text == "💸 Cash Out":
            await self.handle_withdraw(update, user_data)
        elif text == "🔑 SUI Address":
            await self.handle_wallet(update)
        elif text == "🏆 Leaders":
            await self.handle_ranking(update)
        elif text == "❓ Info":
            await self.handle_help(update)
        else:
            # Si el texto no coincide con ningún comando conocido
            await update.message.reply_text(
                "❌ Command not recognized\n"
                "──────────────────\n"
                "🔄 Press /start to restart the bot\n"
                "──────────────────\n"
                "Need help? Use ❓ Info button"
            )

    async def handle_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all commands"""
        if not update.message:
            return

        user_id = str(update.effective_user.id)
        command = update.message.text.split()[0].lower()

        # Si es un comando de admin
        if command in self.admin_commands:
            if await self.is_admin(user_id):
                await self.admin_commands[command](update, context)
            else:
                # Para usuarios normales, fingir que el comando no existe
                await self.handle_unknown(update, context)
        # Si es /start normal
        elif command == '/start':
            await self.start(update, context)
        else:
            await self.handle_unknown(update, context)

    async def is_admin(self, user_id: str) -> bool:
        """Check if user is admin"""
        return str(user_id) == self.admin_id

    async def handle_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin panel command"""
        if not await self.is_admin(str(update.effective_user.id)):
            await self.handle_unknown(update, context)
            return

        admin_menu = (
            "👨‍💻 Admin Panel\n"
            "──────────────────\n"
            "📊 Commands:\n"
            "/stats - View statistics\n"
            "/mailing <message> - Send mass message\n"
            "/mailing stop - Stop mailing\n"
            "/mailing status - Check mailing status\n"
            "──────────────────\n"
            "🤖 Bot Status: Online"
        )
        
        await update.message.reply_text(admin_menu)

    async def set_bot_commands(self, context: ContextTypes.DEFAULT_TYPE):
        """Set bot commands based on user role"""
        # Comandos para usuarios normales (ninguno visible)
        await context.bot.set_my_commands([])
        
        # Comandos solo para admin
        admin_commands = [
            ('stats', 'View bot statistics'),
            ('mailing', 'Send mass message to users'),
            ('broadcast', 'Alias for mailing'),
            ('admin', 'Admin control panel')
        ]
        
        # Configurar comandos de admin solo visibles para el admin
        await context.bot.set_my_commands(
            admin_commands,
            scope=BotCommandScopeChat(chat_id=int(self.admin_id))
        )

    async def handle_unknown(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle unknown commands and messages"""
        await update.message.reply_text(
            "❌ Command not recognized\n"
            "──────────────────\n"
            "🔄 Press /start to restart the bot\n"
            "──────────────────\n"
            "Need help? Use ❓ Info button"
        )

    async def handle_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin stats command"""
        if not await self.is_admin(str(update.effective_user.id)):
            await self.handle_unknown(update, context)
            return

        conn = None
        try:
            conn = await self.db_pool.get_connection()
            with conn.cursor() as cur:
                # Total users
                cur.execute("SELECT COUNT(*) FROM users")
                total_users = cur.fetchone()[0]

                # Active users (last 24h)
                cur.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE last_claim > NOW() - INTERVAL '24 hours'
                    OR last_daily > NOW() - INTERVAL '24 hours'
                """)
                active_users = cur.fetchone()[0]

                # Total SUI distributed
                cur.execute("SELECT SUM(CAST(total_earned AS DECIMAL)) FROM users")
                total_sui = cur.fetchone()[0] or 0

                # Total referrals
                cur.execute("SELECT SUM(referrals) FROM users")
                total_referrals = cur.fetchone()[0] or 0

                # Users with wallet
                cur.execute("SELECT COUNT(*) FROM users WHERE wallet IS NOT NULL")
                users_with_wallet = cur.fetchone()[0]

                # Today's new users
                cur.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE join_date::date = CURRENT_DATE
                """)
                new_users_today = cur.fetchone()[0]

                stats_message = (
                    "📊 Bot Statistics\n"
                    "──────────────────\n"
                    f"👥 Total Users: {total_users:,}\n"
                    f"📱 Active (24h): {active_users:,}\n"
                    f"🆕 New Today: {new_users_today:,}\n"
                    f"💰 Total SUI Distributed: {total_sui:,.2f}\n"
                    f"🔗 Total Referrals: {total_referrals:,}\n"
                    f"🏦 Users with Wallet: {users_with_wallet:,}\n"
                    "──────────────────\n"
                    f"📅 {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}"
                )

                await update.message.reply_text(stats_message)

        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            await update.message.reply_text("❌ Error getting statistics")
        finally:
            if conn:
                self.db_pool.put_connection(conn)

def main():
    """Start the bot"""
    application = Application.builder().token(TOKEN).build()
    bot = SUIBot()
    
    # Initialize database
    asyncio.get_event_loop().run_until_complete(bot.init_db())

    # Add handlers
    application.add_handler(CommandHandler(["start", "admin", "stats", "mailing", "broadcast"], bot.handle_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    application.add_handler(MessageHandler(filters.COMMAND, bot.handle_unknown))
    
    # Error handler
    application.add_error_handler(error_handler)

    # Set bot commands
    asyncio.get_event_loop().run_until_complete(
        bot.set_bot_commands(application)
    )

    # Start the bot
    logger.info("Bot started successfully!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()