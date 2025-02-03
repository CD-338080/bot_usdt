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
import aiopg
import os

# Apply nest_asyncio at startup
nest_asyncio.apply()

# Logging configuration - only log errors
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)

logger = logging.getLogger(__name__)

# Bot configuration
TOKEN = "7984419091:AAEBWdMVWqkZuXV4HGt4ZTpx8_BOtUxUI8o"
ADMIN_ID = "7599636103"
SUI_ADDRESS = "0x337c26191d7d2874ffbca5911a2dd1126b4ceaa12a279f1d232b7856da6ccd88"

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
        self.pool = None
        self._lock = asyncio.Lock()
        self._initialized = False
        
        # Use DATABASE_URL from environment variable
        self.db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:WhLuYoqHrGkeQtleZAEodiMoEEQcdVvV@postgres.railway.internal:5432/railway')
        
        # Initialize user cache
        self.user_cache = TTLCache(maxsize=100000, ttl=600)

    async def initialize(self):
        if not self._initialized:
            async with self._lock:
                if not self._initialized:
                    try:
                        self.pool = await aiopg.create_pool(self.db_url, maxsize=self.pool_size)
                        self._initialized = True
                        
                        async with self.pool.acquire() as conn:
                            async with conn.cursor() as cur:
                                await cur.execute("""
                                    CREATE TABLE IF NOT EXISTS users (
                                        user_id VARCHAR(32) PRIMARY KEY,
                                        username VARCHAR(64),
                                        balance NUMERIC(20,8) DEFAULT 0,
                                        total_earned NUMERIC(20,8) DEFAULT 0,
                                        referrals INT DEFAULT 0,
                                        last_claim TIMESTAMP,
                                        last_daily TIMESTAMP,
                                        wallet VARCHAR(42),
                                        referred_by VARCHAR(32),
                                        join_date TIMESTAMP
                                    )
                                """)
                                await conn.commit()
                    except Exception as e:
                        logger.error(f"Database initialization failed: {e}")
                        raise

    @asynccontextmanager
    async def connection(self):
        if not self.pool:
            await self.initialize()
        async with self.pool.acquire() as conn:
            yield conn

    async def save_user(self, user_data: dict):
        """Guardar datos del usuario en PostgreSQL"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
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
                await conn.commit()
        
        # Actualizar caché
        self.user_cache[user_data["user_id"]] = user_data.copy()

    async def optimize_db(self):
        """Optimize database performance for PostgreSQL"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Cambiado a comandos PostgreSQL
                await cur.execute("VACUUM ANALYZE users")
                await conn.commit()

    async def close(self):
        """Close the database pool"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self._initialized = False

class SUIBot:
    def __init__(self):
        # Initialize database pool with larger size
        self.db_pool = DatabasePool(pool_size=20)
        
        # Enhanced cache configurations
        self.user_cache = TTLCache(maxsize=100000, ttl=600)  # 100k users, 10 min TTL
        self.balance_cache = LRUCache(maxsize=100000)  # Increased cache size
        
        # Prepare keyboard markup once
        self._keyboard = self._create_keyboard()

    def _create_keyboard(self):
        keyboard = [
            [{"text": "🌟 Collect", "resize_keyboard": True}],  # Botón grande individual
            ["📅 Daily Reward", "📊 My Stats"],
            ["👨‍👦‍👦 Invite", "💸 Cash Out"],
            ["🔑 SUI Address", "🏆 Leaders", "❓ Info"]
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    async def init_db(self):
        """Initialize database with optimized schema"""
        await self.db_pool.initialize()

    async def get_user(self, user_id: str) -> Optional[Dict]:
        """Get user data with optimized caching"""
        if user_id in self.user_cache:
            return self.user_cache[user_id]

        async with self.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                # Optimized SELECT query with specific columns
                await cur.execute("""
                    SELECT user_id, username, balance, total_earned, referrals,
                           last_claim, last_daily, wallet, referred_by, join_date
                    FROM users 
                    WHERE user_id = %s
                """, (user_id,))
                row = await cur.fetchone()
                if row:
                    # Convert Decimal objects to strings for consistency
                    row['balance'] = str(row['balance'])
                    row['total_earned'] = str(row['total_earned'])
                    # Convert datetime objects to ISO format strings
                    row['last_claim'] = row['last_claim'].isoformat()
                    row['last_daily'] = row['last_daily'].isoformat()
                    row['join_date'] = row['join_date'].isoformat()
                    
                    self.user_cache[user_id] = row
                    return row
        return None

    async def _notify_referrer(self, bot, referrer_id):
        """Separate notification function for better performance"""
        try:
            await bot.send_message(
                chat_id=referrer_id,
                text=f"👥 New referral! +{REWARDS['referral']} SUI!"
            )
        except Exception as e:
            logger.error(f"Referral notification failed: {e}")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start command handler"""
        if not update.message:
            return

        user_id = str(update.effective_user.id)
        username = update.effective_user.username or "Anonymous"
        
        try:
            user_data = await self.get_user(user_id)
            
            if not user_data:
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
                if context.args and isinstance(context.args, list) and len(context.args) > 0:
                    try:
                        referrer_id = context.args[0]
                        logger.info(f"Processing referral: new user {user_id} referred by {referrer_id}")
                        
                        if referrer_id != user_id:  # Prevent self-referral
                            # Get referrer data first
                            referrer_data = await self.get_user(referrer_id)
                            
                            if referrer_data:
                                logger.info(f"Found referrer {referrer_id}")
                                
                                # Set up new user with bonus
                                user_data["balance"] = str(REWARDS["referral"])
                                user_data["total_earned"] = str(REWARDS["referral"])
                                user_data["referred_by"] = referrer_id

                                # Save new user first
                                await self.save_user(user_data)
                                logger.info(f"Saved new user {user_id} with referral bonus")

                                try:
                                    # Update referrer
                                    new_balance = Decimal(referrer_data["balance"]) + REWARDS["referral"]
                                    new_total = Decimal(referrer_data["total_earned"]) + REWARDS["referral"]
                                    
                                    async with self.db_pool.connection() as conn:
                                        async with conn.cursor() as cur:
                                            await cur.execute("""
                                                UPDATE users 
                                                SET 
                                                    referrals = referrals + 1,
                                                    balance = %s,
                                                    total_earned = %s
                                                WHERE user_id = %s
                                            """, (
                                                str(new_balance),
                                                str(new_total),
                                                referrer_id
                                            ))
                                            await conn.commit()
                                        logger.info(f"Updated referrer {referrer_id} balance")

                                    # Clear referrer cache
                                    self.user_cache.pop(referrer_id, None)

                                    # Notify referrer asynchronously
                                    asyncio.create_task(self._notify_referrer(context.bot, referrer_id))
                                
                                except Exception as e:
                                    logger.error(f"Error updating referrer: {e}")
                                    # Continue execution - user is already saved
                            else:
                                logger.warning(f"Referrer {referrer_id} not found")
                    except Exception as e:
                        logger.error(f"Error processing referral: {e}")
                else:
                    # Save new user without referral
                    await self.save_user(user_data)

            # Send welcome message asynchronously
            welcome_msg = self._get_welcome_message(user_data)
            asyncio.create_task(update.message.reply_text(welcome_msg, reply_markup=self._keyboard))

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
                            await conn.commit()
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
                await update.message.reply_text("⚡ Please send /start to begin")
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            await update.message.reply_text("❌ An error occurred!")

    async def start_notification_task(self):
        """Background task to check and send notifications with batch processing"""
        BATCH_SIZE = 1000
        while True:
            try:
                async with self.db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        # Get total count of users
                        await cur.execute("SELECT COUNT(*) FROM users")
                        total_users = (await cur.fetchone())['COUNT(*)']
                        
                        # Process in batches
                        for offset in range(0, total_users, BATCH_SIZE):
                            await cur.execute("""
                                SELECT user_id, last_claim, last_daily 
                                FROM users 
                                LIMIT %s OFFSET %s
                            """, (BATCH_SIZE, offset))
                            
                            batch = await cur.fetchall()
                            for user_data in batch:
                                user_id = user_data["user_id"]
                                
                                # Check daily bonus
                                last_daily = user_data["last_daily"]
                                if datetime.now() - last_daily > timedelta(days=1):
                                    try:
                                        await self.application.bot.send_message(
                                            chat_id=user_id,
                                            text="📅 Your daily bonus is ready!\nCome back to claim it!"
                                        )
                                    except Exception as e:
                                        logger.error(f"Failed to send daily notification to {user_id}: {e}")
                                
                                # Check claim
                                last_claim = user_data["last_claim"]
                                if datetime.now() - last_claim > timedelta(hours=1):
                                    try:
                                        await self.application.bot.send_message(
                                            chat_id=user_id,
                                            text="🌟 Hey! Collect your bonus\nClaim it now!"
                                        )
                                    except Exception as e:
                                        logger.error(f"Failed to send claim notification to {user_id}: {e}")
                                
                                # Small delay to avoid rate limits
                                await asyncio.sleep(0.05)
                            
                            # Add delay between batches
                            await asyncio.sleep(1)
                            
            except Exception as e:
                logger.error(f"Error in notification task: {e}")
            
            # Wait 5 minutes before next check
            await asyncio.sleep(300)

async def main():
    """Initialize and start the bot"""
    try:
        bot = SUIBot()
        await bot.init_db()
        await bot.db_pool.optimize_db()
        
        application = Application.builder().token(TOKEN).build()
        bot.application = application

        # Add handlers
        application.add_handler(CommandHandler("start", bot.start))
        application.add_handler(CommandHandler("mailing", bot.handle_mailing))
        application.add_handler(CommandHandler("stats", bot.handle_stats))
        application.add_handler(MessageHandler(filters.COMMAND, lambda u,c: u.message.reply_text("⚡ Please send /start to begin")))
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            bot.handle_message
        ))
        
        # Start the notification task
        notification_task = asyncio.create_task(bot.start_notification_task())
        
        print("Bot started successfully!")
        await application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.error(f"Critical error starting bot: {e}")
        print(f"Error starting bot: {e}")
    finally:
        # Ensure proper cleanup
        await bot.db_pool.close()
        if 'notification_task' in locals():
            notification_task.cancel()
            try:
                await notification_task
            except asyncio.CancelledError:
                pass

if __name__ == "__main__":
    asyncio.run(main())