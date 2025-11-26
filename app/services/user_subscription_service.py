"""
User Subscription Service
=========================
Handles user account status and subscription limit checks for FastAPI.
Connects to MongoDB to check user subscription status before allowing comparisons.
"""

import os
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, Any, Optional
from datetime import datetime
import logging
from bson import ObjectId

logger = logging.getLogger(__name__)


class UserSubscriptionService:
    """
    Service for checking user subscription status and limits.
    """

    def __init__(self):
        self.client = None
        self.database = None
        self.users_collection = None

        # Subscription plan limits (matching NestJS backend)
        self.SUBSCRIPTION_PLANS = {
            "starter": {
                "comparisonsLimit": 50,
                "quotesPerCase": 3,
            },
            "professional": {
                "comparisonsLimit": 250,
                "quotesPerCase": 3,
            },
            "enterprise": {
                "comparisonsLimit": -1,  # -1 means unlimited
                "quotesPerCase": 8,
            },
        }

    async def connect(self):
        """Connect to MongoDB and initialize users collection"""
        try:
            # Get MongoDB connection string from environment
            # Use the same MongoDB as NestJS backend
            # NestJS uses MONGO_URI, FastAPI might use MONGODB_URL
            mongodb_url = os.getenv(
                "MONGO_URI", os.getenv("MONGODB_URL", "mongodb://localhost:27017")
            )

            # Extract database name from URI or use environment variable
            # MongoDB URI format: mongodb://host:port/database_name?options
            # OR: mongodb://host:port/ (no database name in URI)
            # Priority: 1. MONGODB_DATABASE env var, 2. MONGO_DB_NAME env var, 3. Extract from URI, 4. Default
            database_name = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB_NAME")

            if not database_name:
                # Try to extract from URI if database name is in the path
                # MongoDB URI can be:
                # - mongodb://localhost:27017/database_name
                # - mongodb://localhost:27017/database_name?options
                # - mongodb://localhost:27017/ (no database)

                # Parse the URI to extract database name
                try:
                    from urllib.parse import urlparse

                    parsed = urlparse(mongodb_url)
                    path = parsed.path.strip("/")

                    if path and path not in ["", "mongodb"]:
                        # Database name is in the path
                        database_name = path.split("?")[0]  # Remove query params if any
                        logger.info(
                            f"📦 Extracted database name from URI: {database_name}"
                        )
                except Exception as e:
                    logger.warning(f"⚠️  Could not extract database name from URI: {e}")

            # Default database name (same as backend)
            if not database_name:
                database_name = "hakemAI"
                logger.warning(
                    f"⚠️  No database name specified, using default: {database_name}"
                )
                logger.warning(
                    f"   If this is wrong, set MONGODB_DATABASE environment variable"
                )

            logger.info(f"🔌 Connecting to MongoDB for user subscription checks")
            logger.info(f"   URL: {mongodb_url}")
            logger.info(f"   Database: {database_name}")

            self.client = AsyncIOMotorClient(mongodb_url)
            self.database = self.client[database_name]

            # Initialize users collection (same as NestJS backend)
            self.users_collection = self.database.users

            # Test connection
            await self.client.admin.command("ping")
            logger.info(
                "✅ Successfully connected to MongoDB for user subscription checks"
            )

            # Verify database and collection
            db_list = await self.client.list_database_names()
            logger.info(f"📋 Available databases: {db_list}")

            collections = await self.database.list_collection_names()
            logger.info(f"📋 Collections in '{database_name}': {collections}")

            # Check if users collection exists and has documents
            if "users" in collections:
                user_count = await self.users_collection.count_documents({})
                logger.info(f"📊 Users collection has {user_count} documents")
            else:
                logger.warning(
                    f"⚠️  'users' collection not found in database '{database_name}'"
                )
                logger.warning(f"   Available collections: {collections}")

        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            raise

    async def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            logger.info("🔌 Disconnected from MongoDB")

    async def check_user_can_compare(
        self, user_id: str, files_count: int
    ) -> Dict[str, Any]:
        """
        Check if user can perform comparison.

        Args:
            user_id: User MongoDB ObjectId as string
            files_count: Number of PDF files being uploaded

        Returns:
            Dict with:
                - allowed: bool
                - reason: str (if not allowed)
                - remainingComparisons: int (if allowed)
                - quotesPerCase: int
                - user: Dict (user data)

        Raises:
            HTTPException if user not found or validation fails
        """
        try:
            # Validate ObjectId format
            if not ObjectId.is_valid(user_id):
                logger.error(f"❌ Invalid user ID format: {user_id}")
                return {
                    "allowed": False,
                    "reason": f"Invalid user ID format: {user_id}",
                    "user": None,
                }

            # Find user by ID
            logger.info(f"🔍 Searching for user with ID: {user_id}")
            logger.info(f"📊 Database: {self.database.name}")
            logger.info(f"📊 Collection: {self.users_collection.name}")

            user = await self.users_collection.find_one({"_id": ObjectId(user_id)})

            if not user:
                # Diagnostic logging
                logger.warning(f"⚠️  User not found with ID: {user_id}")
                logger.warning(f"   Database: {self.database.name}")
                logger.warning(f"   Collection: {self.users_collection.name}")

                # Check if collection exists and has any documents
                try:
                    count = await self.users_collection.count_documents({})
                    logger.info(f"📊 Total users in collection: {count}")

                    # Try to get a sample user to see the structure
                    if count > 0:
                        sample_user = await self.users_collection.find_one({})
                        if sample_user:
                            logger.info(f"📋 Sample user ID: {sample_user.get('_id')}")
                            logger.info(
                                f"📋 Sample user email: {sample_user.get('email', 'N/A')}"
                            )
                            logger.info(
                                f"📋 Sample user username: {sample_user.get('username', 'N/A')}"
                            )
                except Exception as e:
                    logger.error(f"❌ Error checking collection: {e}")

                return {
                    "allowed": False,
                    "reason": f"User not found with ID: {user_id}. Please check database name matches NestJS backend.",
                    "user": None,
                }

            logger.info(
                f"✅ Found user: {user.get('username', 'N/A')} ({user.get('email', 'N/A')})"
            )

            # Check account status
            account_status = user.get("accountStatus")
            if account_status != "active":
                if account_status == "pending":
                    return {
                        "allowed": False,
                        "reason": "Account is pending activation. Please upload payment proof and wait for admin approval.",
                        "user": None,
                    }
                elif account_status == "frozen":
                    return {
                        "allowed": False,
                        "reason": "Account is frozen. Please contact support.",
                        "user": None,
                    }
                else:
                    return {
                        "allowed": False,
                        "reason": f"Account status is {account_status}. Account must be active to use comparison service.",
                        "user": None,
                    }

            # Check if user has an active subscription
            subscription_plan = user.get("subscriptionPlan")
            if not subscription_plan:
                return {
                    "allowed": False,
                    "reason": "No active subscription plan. Please contact support to activate your account.",
                    "user": None,
                }

            # Check subscription expiration
            subscription_end_date = user.get("subscriptionEndDate")
            if subscription_end_date:
                from datetime import datetime

                # Handle different date formats
                if isinstance(subscription_end_date, str):
                    try:
                        subscription_end_date = datetime.fromisoformat(
                            subscription_end_date.replace("Z", "+00:00")
                        )
                    except:
                        subscription_end_date = datetime.fromisoformat(
                            subscription_end_date
                        )
                elif not isinstance(subscription_end_date, datetime):
                    # MongoDB datetime object - convert to Python datetime
                    subscription_end_date = subscription_end_date

                now = datetime.utcnow()
                # Compare dates (handle timezone-aware dates)
                if hasattr(subscription_end_date, "replace"):
                    subscription_end_date = subscription_end_date.replace(tzinfo=None)
                if subscription_end_date < now:
                    return {
                        "allowed": False,
                        "reason": "Subscription has expired. Please renew your subscription.",
                        "user": None,
                    }

            # Reset monthly usage if needed (subscription period has passed)
            await self._reset_monthly_usage_if_needed(user)

            # Re-fetch user after potential reset
            user = await self.users_collection.find_one({"_id": ObjectId(user_id)})

            # Get plan configuration
            plan_config = self.SUBSCRIPTION_PLANS.get(subscription_plan)
            if not plan_config:
                return {
                    "allowed": False,
                    "reason": f"Invalid subscription plan: {subscription_plan}",
                    "user": None,
                }

            # Check quotes per case limit
            quotes_per_case = user.get("quotesPerCase", plan_config["quotesPerCase"])
            if files_count > quotes_per_case:
                return {
                    "allowed": False,
                    "reason": f"Maximum {quotes_per_case} quotes per case allowed for your plan. You uploaded {files_count} files.",
                    "quotesPerCase": quotes_per_case,
                    "user": None,
                }

            # Check monthly comparison limit (unless unlimited)
            comparisons_limit = user.get(
                "comparisonsLimit", plan_config["comparisonsLimit"]
            )
            comparisons_used = user.get("comparisonsUsed", 0)

            # Check if unlimited (-1 means unlimited)
            if comparisons_limit == -1:
                return {
                    "allowed": True,
                    "remainingComparisons": -1,  # -1 means unlimited
                    "quotesPerCase": quotes_per_case,
                    "user": user,
                }

            # Check if limit reached
            if comparisons_used >= comparisons_limit:
                return {
                    "allowed": False,
                    "reason": f"Monthly comparison limit reached ({comparisons_limit}). Limit resets on subscription renewal.",
                    "remainingComparisons": 0,
                    "user": None,
                }

            remaining = comparisons_limit - comparisons_used

            return {
                "allowed": True,
                "remainingComparisons": remaining,
                "quotesPerCase": quotes_per_case,
                "user": user,
            }

        except Exception as e:
            logger.error(f"❌ Error checking user subscription: {e}")
            return {
                "allowed": False,
                "reason": f"Error checking subscription: {str(e)}",
                "user": None,
            }

    async def _reset_monthly_usage_if_needed(self, user: Dict[str, Any]) -> None:
        """
        Reset monthly usage if subscription period has passed.
        Updates user document in database.
        """
        try:
            subscription_end = user.get("subscriptionEndDate")

            if not subscription_end:
                return

            # Convert to datetime if needed
            from datetime import datetime

            if isinstance(subscription_end, str):
                try:
                    subscription_end = datetime.fromisoformat(
                        subscription_end.replace("Z", "+00:00")
                    )
                except:
                    subscription_end = datetime.fromisoformat(subscription_end)
            elif not isinstance(subscription_end, datetime):
                # Handle MongoDB datetime - already a datetime object
                pass

            now = datetime.utcnow()

            # Handle timezone-aware dates
            if hasattr(subscription_end, "replace"):
                subscription_end_naive = subscription_end.replace(tzinfo=None)
            else:
                subscription_end_naive = subscription_end

            # Check if subscription period has passed
            if now >= subscription_end_naive:
                # Reset usage and update dates
                new_start = datetime(now.year, now.month, now.day, 0, 0, 0)
                # Calculate next month end date
                if now.month == 12:
                    new_end = datetime(now.year + 1, 1, now.day, 0, 0, 0)
                else:
                    new_end = datetime(now.year, now.month + 1, now.day, 0, 0, 0)

                await self.users_collection.update_one(
                    {"_id": user["_id"]},
                    {
                        "$set": {
                            "comparisonsUsed": 0,
                            "subscriptionStartDate": new_start,
                            "subscriptionEndDate": new_end,
                            "subscriptionRenewalDate": new_end,
                        }
                    },
                )

                logger.info(f"✅ Reset monthly usage for user {user['_id']}")

        except Exception as e:
            logger.error(f"⚠️  Error resetting monthly usage: {e}")

    async def record_comparison(self, user_id: str) -> bool:
        """
        Record a comparison usage after successful comparison.
        Increments comparisonsUsed counter.

        Args:
            user_id: User MongoDB ObjectId as string

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get user first to check if unlimited
            user = await self.users_collection.find_one({"_id": ObjectId(user_id)})

            if not user:
                logger.error(f"❌ User not found: {user_id}")
                return False

            # Check if unlimited (comparisonsLimit = -1)
            comparisons_limit = user.get("comparisonsLimit")
            if comparisons_limit == -1:
                # Unlimited plan, no need to increment
                logger.info(
                    f"✅ Unlimited plan - no usage tracking needed for user {user_id}"
                )
                return True

            # Increment comparisonsUsed
            result = await self.users_collection.update_one(
                {"_id": ObjectId(user_id)}, {"$inc": {"comparisonsUsed": 1}}
            )

            if result.modified_count > 0:
                logger.info(f"✅ Recorded comparison usage for user {user_id}")
                return True
            else:
                logger.warning(
                    f"⚠️  Failed to update comparison usage for user {user_id}"
                )
                return False

        except Exception as e:
            logger.error(f"❌ Error recording comparison: {e}")
            return False


# Create global service instance
user_subscription_service = UserSubscriptionService()
