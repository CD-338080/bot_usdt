from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from datetime import datetime, timedelta
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
from urllib.parse import urlparse
from signal import signal, SIGINT, SIGTERM, SIGABRT
import time

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
    def __init__(self, pool_size: int = 20):
        self.pool_size = pool_size
        self.conn = None
        self._lock = asyncio.Lock()
        self._initialized = False
        
        # Get database URL from environment
        self.db_url = os.getenv('DATABASE_URL')
        if not self.db_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        # Parse DATABASE_URL
        url = urlparse(self.db_url)
        self.db_params = {
            'dbname': url.path[1:],
            'user': url.username,
            'password': url.password,
            'host': url.hostname,
            'port': url.port
        }
        
        # Initialize user cache
        self.user_cache = TTLCache(maxsize=100000, ttl=600)

    def get_connection(self):
        """Get a database connection"""
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(**self.db_params)
            self.conn.autocommit = True
        return self.conn

    async def initialize(self):
        """Initialize database connection"""
        if not self._initialized:
            try:
                self.conn = self.get_connection()
                with self.conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            user_id VARCHAR(32) PRIMARY KEY,
                            username VARCHAR(64),
                            balance NUMERIC(20,8) DEFAULT 0,
                            total_earned NUMERIC(20,8) DEFAULT 0,
                            referrals INT DEFAULT 0,
                            last_claim TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            last_daily TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            wallet VARCHAR(42),
                            referred_by VARCHAR(32),
                            join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                self._initialized = True
                logger.info("Database tables initialized successfully")
            except Exception as e:
                logger.error(f"Database initialization failed: {str(e)}")
                raise

    async def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

    @asynccontextmanager
    async def connection(self):
        """Get a database connection"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                yield cur
        finally:
            if conn:
                conn.close()

    async def save_user(self, user_data: dict):
        """Guardar datos del usuario en PostgreSQL"""
        conn = self.get_connection()
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
                Decimal(user_data["balance"]),
                Decimal(user_data["total_earned"]),
                user_data["referrals"],
                datetime.fromisoformat(user_data["last_claim"]) if user_data["last_claim"] else None,
                datetime.fromisoformat(user_data["last_daily"]) if user_data["last_daily"] else None,
                user_data.get("wallet"),
                user_data.get("referred_by"),
                datetime.fromisoformat(user_data.get("join_date", datetime.now().isoformat()))
            ))
        
        # Actualizar caché
        self.user_cache[user_data["user_id"]] = user_data.copy()

    async def optimize_db(self):
        """Optimize database performance for PostgreSQL"""
        async with self.conn.cursor() as cur:
            # Cambiado a comandos PostgreSQL
            cur.execute("VACUUM ANALYZE users")

    async def get_user(self, user_id: str) -> Optional[Dict]:
        """Get user data from cache or database"""
        # Check cache first
        if user_id in self.user_cache:
            return self.user_cache[user_id]
        
        # Get from database
        try:
            conn = self.get_connection()
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
                    user_data["last_claim"] = user_data["last_claim"].isoformat()
                    user_data["last_daily"] = user_data["last_daily"].isoformat()
                    user_data["join_date"] = user_data["join_date"].isoformat()
                    # Cache the result
                    self.user_cache[user_id] = user_data
                    return user_data
                return None
                
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            return None

class SUIBot:
    def __init__(self):
        self.token = os.getenv('BOT_TOKEN')
        if not self.token:
            raise ValueError("BOT_TOKEN environment variable is required")
            
        self.admin_id = os.getenv('ADMIN_ID')
        if not self.admin_id:
            raise ValueError("ADMIN_ID environment variable is required")
        
        # Initialize database pool
        self.db_pool = DatabasePool(pool_size=20)
        
        # Initialize keyboard markup
        self._keyboard = ReplyKeyboardMarkup([
            ["🌟 Collect", "📅 Daily Reward"],
            ["📊 My Stats", "👨‍👦‍👦 Invite"],
            ["💸 Cash Out", "🔑 SUI Address"],
            ["🏆 Leaders", "❓ Info"]
        ], resize_keyboard=True)
        
        self.notification_task = None

    async def init_db(self):
        """Initialize database connection"""
        await self.db_pool.initialize()

    async def get_user(self, user_id: str) -> Optional[Dict]:
        """Get user data from database"""
        return await self.db_pool.get_user(user_id)

    async def save_user(self, user_data: dict):
        """Save user data to database"""
        await self.db_pool.save_user(user_data)

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
        """Start command handler"""
        if not update.message:
            return

        user_id = str(update.effective_user.id)
        username = update.effective_user.username or "Anonymous"
        
        try:
            user_data = await self.get_user(user_id)
            
            if not user_data:
                # Initialize new user data
                user_data = {
                    "user_id": user_id,
                    "username": username,
                    "balance": "0",
                    "total_earned": "0",
                    "referrals": 0,
                    "last_claim": (datetime.now() - timedelta(hours=2)).isoformat(),
                    "last_daily": (datetime.now() - timedelta(days=2)).isoformat(),
                    "wallet": None,
                    "referred_by": None,
                    "join_date": datetime.now().isoformat()
                }

                # Process referral
                if context.args and len(context.args) > 0:
                    referrer_id = context.args[0]
                    
                    if referrer_id != user_id:  # Prevent self-referral
                        referrer_data = await self.get_user(referrer_id)
                        
                        if referrer_data:
                            # Update new user with referral bonus
                            user_data["balance"] = str(REWARDS["referral"])
                            user_data["total_earned"] = str(REWARDS["referral"])
                            user_data["referred_by"] = referrer_id

                            # Save new user first
                            await self.save_user(user_data)

                            try:
                                # Update referrer's data
                                referrer_data["balance"] = str(Decimal(referrer_data["balance"]) + REWARDS["referral"])
                                referrer_data["total_earned"] = str(Decimal(referrer_data["total_earned"]) + REWARDS["referral"])
                                referrer_data["referrals"] = referrer_data["referrals"] + 1

                                # Save referrer's updated data
                                await self.save_user(referrer_data)

                                # Notify referrer
                                await self._notify_referrer(context.bot, referrer_id)
                                
                            except Exception as e:
                                logger.error(f"Error updating referrer: {e}")
                else:
                    # Save new user without referral
                    await self.save_user(user_data)

            # Send welcome message
            welcome_msg = self._get_welcome_message(user_data)
            await update.message.reply_text(welcome_msg, reply_markup=self._keyboard)

        except Exception as e:
            logger.error(f"Critical error in start handler: {str(e)}")
            await update.message.reply_text("⚠️ Something went wrong. Please try again later.")

    def _get_welcome_message(self, user_data):
        """Separate method for welcome message to improve readability"""
        if user_data.get("referred_by"):
            return (
                f"🌟 Welcome to SUI Capital Bot〽️\n\n"
                f"💎 Congratulations! You've received {REWARDS['referral']} SUI as a referral bonus!\n\n"
                "💰 Earning Methods:\n"
                "• ⚡ Hourly Claims\n"
                "• 📅 Daily Rewards\n"
                "• 👥 Referral Program\n\n"
                "🔥 Start earning now with our reward system!"
            )
        return (
            "🌟 Welcome to SUI Capital Bot〽️\n\n"
            "💰 Start Earning SUI:\n"
            "• ⚡ Hourly Claims\n"
            "• 📅 Daily Rewards\n"
            "• 👥 Referral Program\n\n"
            "🔥 Join our community and start earning today!"
        )

    async def handle_claim(self, update: Update, user_data: dict):
        user_id = str(update.effective_user.id)
        
        try:
            last_claim = datetime.fromisoformat(user_data["last_claim"])
            if datetime.now() - last_claim < timedelta(minutes=5):
                time_left = timedelta(minutes=5) - (datetime.now() - last_claim)
                minutes = int(time_left.total_seconds() / 60)
                seconds = int(time_left.total_seconds() % 60)
                await update.message.reply_text(f"⏳ Wait {minutes}m {seconds}s for next claim!")
                return

            current_balance = Decimal(user_data["balance"])
            reward = REWARDS["claim"]
            new_balance = str(current_balance + reward)
            
            user_data.update({
                "balance": new_balance,
                "total_earned": str(Decimal(user_data["total_earned"]) + reward),
                "last_claim": datetime.now().isoformat()
            })
            
            # Async save
            asyncio.create_task(self.save_user(user_data))
            
            await update.message.reply_text(
                f"🌟 Collected {reward} SUI!\n"
                f"📊 My Stats: {new_balance} SUI"
            )
        except Exception as e:
            logger.error(f"Error in claim handler: {e}")
            await update.message.reply_text("❌ An error occurred!")

    async def handle_daily(self, update: Update, user_data: dict):
        try:
            last_daily = datetime.fromisoformat(user_data["last_daily"])
            if datetime.now() - last_daily < timedelta(days=1):
                time_left = timedelta(days=1) - (datetime.now() - last_daily)
                hours = int(time_left.total_seconds() / 3600)
                minutes = int((time_left.total_seconds() % 3600) / 60)
                await update.message.reply_text(
                    f"⏳ Wait {hours}h {minutes}m for next bonus!"
                )
                return

            reward = REWARDS["daily"]
            new_balance = str(Decimal(user_data["balance"]) + reward)
            user_data.update({
                "balance": new_balance,
                "total_earned": str(Decimal(user_data["total_earned"]) + reward),
                "last_daily": datetime.now().isoformat()
            })
            
            asyncio.create_task(self.save_user(user_data))
            
            await update.message.reply_text(
                f"📅 Daily Reward claimed: {reward} SUI!\n"
                f"📊 My Stats: {new_balance} SUI"
            )
        except Exception as e:
            logger.error(f"Error in daily handler: {e}")
            await update.message.reply_text("❌ An error occurred!")

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
        if not user_data.get("wallet"):
            await update.message.reply_text("⚠️ Connect your wallet first!")
            return

        if user_data["referrals"] < REWARDS["min_referrals"]:
            await update.message.reply_text(
                f"⚠️ You need at least {REWARDS['min_referrals']} referrals to withdraw!\n"
                f"Your referrals: {user_data['referrals']}\n\n"
                f"📢 You must also join our official channels:\n"
                f"• @SUI_Capital_Tracker\n"
                f"• @SUI_Capital_News\n"
                f"• @SUI_Capital_QA"
            )
            return

        balance = Decimal(user_data["balance"])
        if balance < REWARDS["min_withdraw"]:
            await update.message.reply_text(
                f"⚠️ Minimum withdrawal: {REWARDS['min_withdraw']} SUI\n"
                f"Your balance: {balance} SUI"
            )
            return

        await update.message.reply_text(
            f"⭐ Withdrawal Request\n\n"
            f"Amount to Withdraw: {balance} SUI\n"
            f"🔸 Destination Wallet: {user_data['wallet']}\n"
            f"🔸 Network Used: SUI Network (SUI)\n\n"
            f"📢 Network Fee: {REWARDS['network_fee']} SUI (required to process the transaction)\n\n"
            f"📨 Please send the fee to the following wallet to complete your request:\n`{SUI_ADDRESS}`\n\n"
            f"⌛ Processing Time: 24-48 hours after the fee payment is confirmed.\n\n"
            f"⚠️ Important Note: The network fee is necessary to cover SUI network operational costs and ensure the success of your transfer. Without this payment, your request will not be processed."
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
        try:
            async with self.db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        SELECT username, total_earned 
                        FROM users 
                        ORDER BY CAST(total_earned AS DECIMAL) DESC 
                        LIMIT 10
                    """)
                    rows = await cur.fetchall()

            message = "🏆 Leaders:\n\n"
            for i, row in enumerate(rows, 1):
                username = row['username'] or "Anonymous"
                message += f"{i}. @{username}: {row['total_earned']} SUI\n"
            await update.message.reply_text(message)
        except Exception as e:
            logger.error(f"Error in ranking: {e}")
            await update.message.reply_text("❌ Error loading ranking!")

    async def handle_help(self, update: Update):
        await update.message.reply_text(
            "🌟 Welcome to SUI Rewards Bot!\n\n"
            "💎 Earning Opportunities:\n"
            "• 🕒 Minutes Claim Bonus\n"
            "• 📅 Daily Reward (24h)\n"
            "• 👥 Referral Program\n\n"
            "💰 Withdrawal Information:\n"
            "• ⚡ Network: SUI (SUI)\n"
            "• ⏱ Processing: 24-48h\n\n"
            "📱 Official Channel:\n"
            "• @SUI_Capital_Tracker\n\n"
            "🔐 Security Notice:\n"
            "• Always verify wallet addresses\n"
            "• Never share personal information"
        )

    async def handle_mailing(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin command to send message to all users"""
        if str(update.effective_user.id) != ADMIN_ID:
            return

        if not context.args:
            await update.message.reply_text("Usage: /mailing <message>")
            return

        message = ' '.join(context.args)
        success = 0
        failed = 0

        async with self.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT user_id FROM users")
                async for (user_id,) in cur:
                    try:
                        async with conn.cursor() as cur:
                            await cur.execute("""
                                UPDATE users 
                                SET last_claim = CURRENT_TIMESTAMP,
                                    last_daily = CURRENT_TIMESTAMP
                                WHERE user_id = %s
                            """, (user_id,))
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=f"📢 Announcement:\n\n{message}"
                        )
                        success += 1
                    except Exception as e:
                        logger.error(f"Failed to send to {user_id}: {e}")
                        failed += 1
                    
                    # Add small delay to avoid rate limits
                    await asyncio.sleep(0.05)
        
        await update.message.reply_text(
            f"✅ Mailing completed:\n"
            f"Success: {success}\n"
            f"Failed: {failed}"
        )

    async def handle_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin command to get bot statistics"""
        if str(update.effective_user.id) != ADMIN_ID:
            return

        try:
            async with self.db_pool.connection() as conn:
                # Get total users
                async with conn.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM users")
                    total_users = (await cur.fetchone())[0]

                # Get active users (last 24h)
                day_ago = (datetime.now() - timedelta(days=1)).isoformat()
                async with conn.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM users WHERE last_claim > %s", (day_ago,))
                    active_users = (await cur.fetchone())[0]

                # Get total earned
                async with conn.cursor() as cur:
                    await cur.execute("SELECT SUM(CAST(total_earned AS DECIMAL)) FROM users")
                    total_earned = (await cur.fetchone())[0] or 0

            await update.message.reply_text(
                "📊 Bot Statistics:\n\n"
                f"👨‍👦‍👦 Total Users: {total_users:,}\n"
                f"✨ Active Users (24h): {active_users:,}\n"
                f"📊 Total SUI Earned: {total_earned:,.2f}\n"
                f"💾 Cached Users: {len(self.user_cache):,}"
            )
        except Exception as e:
            logger.error(f"Error in stats: {e}")
            await update.message.reply_text("❌ Error getting statistics!")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message or not update.message.text:
            await update.message.reply_text("⚡ Please send /start to begin")
            return

        text = update.message.text
        user_id = str(update.effective_user.id)
        
        try:
            user_data = await self.get_user(user_id)
            if not user_data:
                await update.message.reply_text("⚡ Please send /start to begin")
                return

            # Handle wallet address - accept any text but show warning
            if len(text) > 25 and not text.startswith('/'):  # Basic check for potential address
                user_data["wallet"] = text
                asyncio.create_task(self.save_user(user_data))
                await update.message.reply_text(
                    "✅ SUI address saved!\n\n"
                    "⚠️ IMPORTANT:\n"
                    "• Verify your address is correct\n"
                    "• Wrong addresses will result in lost funds\n"
                    "• No recovery possible for incorrect addresses"
                )
                return

            # Command mapping to handlers
            handlers = {
                "🌟 Collect": (self.handle_claim, (update, user_data)),
                "📅 Daily Reward": (self.handle_daily, (update, user_data)),
                "📊 My Stats": (self.handle_balance, (update, user_data)),
                "👨‍👦‍👦 Invite": (self.handle_referral, (update, context, user_data)),
                "💸 Cash Out": (self.handle_withdraw, (update, user_data)),
                "🔑 SUI Address": (self.handle_wallet, (update,)),
                "🏆 Leaders": (self.handle_ranking, (update,)),
                "❓ Info": (self.handle_help, (update,))
            }

            if handler_info := handlers.get(text):
                handler, args = handler_info
                await handler(*args)
            else:
                await update.message.reply_text("⚠️ Unknown command")
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            await update.message.reply_text("❌ An error occurred!")

    async def start_notification_task(self):
        """Background task to check and send notifications"""
        while True:
            try:
                conn = self.db_pool.get_connection()
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute("""
                        SELECT user_id, last_claim, last_daily 
                        FROM users 
                        WHERE last_claim < NOW() - INTERVAL '1 hour'
                        OR last_daily < NOW() - INTERVAL '1 day'
                        LIMIT 1000
                    """)
                    rows = cur.fetchall()
                    
                    for row in rows:
                        try:
                            user_data = dict(row)
                            last_claim = user_data["last_claim"]
                            last_daily = user_data["last_daily"]
                            user_id = user_data["user_id"]

                            if datetime.now() - last_daily > timedelta(days=1):
                                await self.application.bot.send_message(
                                    chat_id=user_id,
                                    text="📅 Your daily bonus is ready!\nCome back to claim it!"
                                )
                            
                            if datetime.now() - last_claim > timedelta(hours=1):
                                await self.application.bot.send_message(
                                    chat_id=user_id,
                                    text="🌟 Hey! Collect your bonus\nClaim it now!"
                                )
                        except Exception as e:
                            logger.error(f"Error processing notification: {e}")
                        await asyncio.sleep(0.05)
                        
            except Exception as e:
                logger.error(f"Error in notification task: {e}")
            finally:
                await asyncio.sleep(300)  # Wait 5 minutes before next check

    async def handle_invite(self, update: Update):
        """Handle invite command"""
        if not update.message:
            return
        
        user_id = str(update.effective_user.id)
        bot_username = (await update.get_bot()).username
        
        try:
            user_data = await self.get_user(user_id)
            if not user_data:
                await update.message.reply_text("⚠️ Please start the bot first with /start")
                return

            referral_link = f"https://t.me/{bot_username}?start={user_id}"
            referrals = user_data.get("referrals", 0)
            earned = Decimal(user_data.get("total_earned", "0"))
            
            await update.message.reply_text(
                f"🤝 Share your referral link:\n"
                f"{referral_link}\n\n"
                f"👥 Your referrals: {referrals}\n"
                f"💰 Earned from referrals: {earned} SUI\n\n"
                f"💎 Earn {REWARDS['referral']} SUI for each referral!"
            )
        except Exception as e:
            logger.error(f"Error in invite handler: {e}")
            await update.message.reply_text("❌ An error occurred!")

async def main():
    """Initialize and start the bot"""
    try:
        # Initialize bot and database
        bot = SUIBot()
        await bot.init_db()
        
        # Initialize application with better error handling
        application = (
            Application.builder()
            .token(bot.token)
            .connect_timeout(30)  # Increased timeout
            .read_timeout(30)
            .write_timeout(30)
            .get_updates_connect_timeout(30)
            .get_updates_read_timeout(30)
            .get_updates_write_timeout(30)
            .build()
        )
        bot.application = application

        # Add handlers
        application.add_handler(CommandHandler("start", bot.start))
        application.add_handler(CommandHandler("mailing", bot.handle_mailing))
        application.add_handler(CommandHandler("stats", bot.handle_stats))
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            bot.handle_message
        ))
        
        # Add error handler with retry logic
        application.add_error_handler(error_handler)
        
        # Start notification task
        bot.notification_task = asyncio.create_task(bot.start_notification_task())
        
        logger.info("Bot started successfully!")
        
        # Start polling with better error handling
        await application.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
            close_loop=False,
            pool_timeout=30,
            read_timeout=30,
            write_timeout=30
        )
        
    except Exception as e:
        logger.error(f"Critical error: {e}")
        raise

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors in the telegram bot with retry logic."""
    error = context.error
    
    # Log the error
    logger.error(f"Exception while handling an update: {error}")
    
    # Handle specific errors
    if "Conflict: terminated by other getUpdates request" in str(error):
        logger.info("Detected multiple instance conflict, waiting to stabilize...")
        await asyncio.sleep(5)  # Wait for other instance to settle
        return
    
    # Notify admin of other errors
    if ADMIN_ID:
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"❌ Error in bot: {error}"
            )
        except Exception as e:
            logger.error(f"Failed to send error message to admin: {e}")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    
    # Set up signal handlers
    for sig in (SIGINT, SIGTERM, SIGABRT):
        signal(sig, lambda s, f: sys.exit(0))
    
    # Apply nest_asyncio
    nest_asyncio.apply()
    
    # Run the bot with retry logic
    retry_count = 0
    max_retries = 3
    
    while retry_count < max_retries:
        try:
            asyncio.run(main())
            break
        except (KeyboardInterrupt, SystemExit):
            logger.info("Bot stopped")
            break
        except Exception as e:
            retry_count += 1
            logger.error(f"Error (attempt {retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                logger.info(f"Retrying in 5 seconds...")
                time.sleep(5)
            else:
                logger.error("Max retries reached, exiting.")
                sys.exit(1)