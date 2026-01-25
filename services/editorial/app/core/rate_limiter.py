from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import Session


class RateLimitExceeded(Exception):
    def __init__(self, provider: str, reset_in: int):
        self.provider = provider
        self.reset_in = reset_in
        super().__init__(f"Rate limit exceeded for {provider}. Retry in {reset_in}s")


class RateLimiter:
    def __init__(self, session: Session):
        self.session = session

    def check_and_consume(
        self, model_id: str, cost: int = 1, metric: str = "tokens"
    ) -> bool:
        """
        Atomically checks limits and consumes quota using a Leaky Bucket algorithm.
        Returns True if allowed, False if blocked.
        """
        # 1. Clean Key
        key = f"{model_id}:{metric}"

        # 2. Leaky Bucket SQL
        # We use 'window_start' as 'last_updated_at' to track the drip.
        query = text("""
        WITH config AS (
            SELECT limit_value, window_seconds
            FROM rate_limit_config
            WHERE model_id = :model_id AND metric = :metric
        ),
        current_state AS (
            -- Insert new row if missing, or get existing
            INSERT INTO rate_limit_usage (key, current_value, window_start)
            VALUES (:key, 0, NOW())
            ON CONFLICT (key) DO NOTHING
            RETURNING current_value, window_start
        ),
        active_state AS (
            SELECT current_value, window_start FROM rate_limit_usage WHERE key = :key
            UNION ALL
            SELECT current_value, window_start FROM current_state
            LIMIT 1
        ),
        calc AS (
            SELECT
                c.limit_value,
                c.window_seconds,
                s.current_value as old_usage,
                s.window_start as last_updated,
                EXTRACT(EPOCH FROM (NOW() - s.window_start)) as elapsed_seconds,
                -- Drip Rate (Tokens per second)
                (c.limit_value::float / NULLIF(c.window_seconds, 0)) as drip_rate
            From config c, active_state s
        ),
        projection AS (
            SELECT
                limit_value,
                drip_rate,
                -- Leaky Logic: Max(0, Old - (Elapsed * Rate))
                GREATEST(0, old_usage - (elapsed_seconds * drip_rate)) as leaked_usage
            FROM calc
        ),
        decision AS (
            SELECT
                limit_value,
                drip_rate,
                leaked_usage,
                -- Check if new cost fits
                (leaked_usage + :cost) <= limit_value as is_allowed,
                (leaked_usage + :cost) as new_usage
            FROM projection
        ),
        update_op AS (
            UPDATE rate_limit_usage
            SET
                current_value = CASE
                    WHEN (SELECT is_allowed FROM decision) THEN (SELECT new_usage FROM decision)
                    ELSE current_value -- No change if blocked
                END,
                window_start = CASE
                    WHEN (SELECT is_allowed FROM decision) THEN NOW()
                    ELSE window_start -- No change if blocked
                END
            WHERE key = :key AND (SELECT is_allowed FROM decision)
            RETURNING current_value
        )
        SELECT
            d.is_allowed,
            d.limit_value,
            d.new_usage,
            d.drip_rate
        FROM decision d;
        """)

        try:
            result = self.session.execute(
                query,
                {
                    "model_id": model_id,
                    "metric": metric.upper(),
                    "key": key,
                    "cost": cost,
                },
            ).fetchone()

            self.session.commit()

            if not result:
                # Fail open if no config exists
                return True

            is_allowed, limit, current_level, drip_rate = result

            if not is_allowed:
                # Calculate strict wait time based on drip rate
                # We need to leak enough to fit the cost
                # Required Space = Cost
                # Available Space = Limit - Current
                # Deficit = Cost - Available Space = Cost - (Limit - Current) = Current + Cost - Limit
                # Wait = Deficit / Drip_Rate

                deficit = current_level - limit
                if drip_rate and drip_rate > 0:
                    wait_seconds = int(deficit / drip_rate) + 1
                else:
                    wait_seconds = 60  # Fallback

                logger.debug(
                    f"🚫 Rate Limit ({model_id}): Used {int(current_level)}/{limit}. Drip {drip_rate:.2f}/s. Wait {wait_seconds}s"
                )
                raise RateLimitExceeded(model_id, wait_seconds)

            return True

        except RateLimitExceeded:
            raise
        except Exception as e:
            logger.error(f"Rate Limiter DB Error: {e}")
            self.session.rollback()
            return True

    def handle_exhaustion(
        self, model_id: str, metric: str = "tokens", reset_in: int = 3600
    ):
        """
        Called when an external API returns 429/Rate Limit.
        Forces the local counter to the limit to prevent further calls.
        """
        # Key must match check_and_consume (which uses the metric string as-is, usually lowercase)
        key = f"{model_id}:{metric}"

        query = text("""
        UPDATE rate_limit_usage
        SET
            current_value = (SELECT limit_value FROM rate_limit_config WHERE model_id = :model_id AND metric = :metric_enum),
            window_start = :now
        WHERE key = :key
        """)

        try:
            self.session.execute(
                query,
                {
                    "model_id": model_id,
                    "metric_enum": metric.upper(),  # Config table uses ENUM (Requires Uppercase)
                    "key": key,  # Usage table uses string key (lowercase)
                    "now": datetime.now(timezone.utc),
                },
            )
            self.session.commit()
            logger.warning(f"🔒 Force-locked {key} due to external rate limit.")
        except Exception as e:
            logger.error(f"Failed to handle exhaustion for {model_id}: {e}")
            self.session.rollback()
