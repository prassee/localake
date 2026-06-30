import logging
import os
import random
import time
from datetime import datetime, timedelta

import psycopg2
from faker import Faker
from psycopg2.extras import execute_values

TABLE_NAME = "public.upi_transactions"

_UPI_EVENT_TYPES = [
    "payment_initiated",
    "payment_success",
    "payment_failed",
    "money_received",
    "balance_check",
    "upi_pin_change",
    "account_linked",
    "login",
    "logout",
    "otp_verified",
    "autopay_setup",
    "collect_request_sent",
    "refund_initiated",
    "refund_success",
]

_UPI_BANKS = [
    "SBI",
    "HDFC",
    "ICICI",
    "Axis",
    "Kotak",
    "PNB",
    "BOB",
    "Canara",
    "Union Bank",
    "IDFC First",
]

_UPI_HANDLES = ["@okicici", "@okhdfcbank", "@oksbi", "@okaxis", "@ybl", "@paytm"]
_UPI_APPS = ["PhonePe", "Google Pay", "Paytm", "Amazon Pay", "BHIM"]
_TXN_TYPES = ["P2P", "P2M", "COLLECT", "REFUND", "AUTOPAY"]
_CURRENCIES = ["INR"]

_STATUSES = ["success", "failed", "pending"]
_PAYMENT_EVENTS = {
    "payment_initiated",
    "payment_success",
    "money_received",
    "refund_success",
}

_CREDIT_EVENTS = {"money_received", "refund_success"}

faker = Faker()
Faker.seed(42)
random.seed(42)

_MIN_CREATED_AT = datetime(2025, 6, 1)
_MIN_MODIFIED_GAP = timedelta(days=1)
_MAX_MODIFIED_GAP = timedelta(days=5)
_HOTSPOT_REUPDATE_COUNT = 500

logger = logging.getLogger(__name__)


def _connect():
    return psycopg2.connect(
        host=os.getenv("UPI_PG_HOST", "localhost"),
        port=int(os.getenv("UPI_PG_PORT", "5432")),
        dbname=os.getenv("UPI_PG_DB", "nessie"),
        user=os.getenv("UPI_PG_USER", "nessie"),
        password=os.getenv("UPI_PG_PASSWORD", "nessiepass"),
    )


def create_upi_transactions_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id BIGINT PRIMARY KEY,
                transaction_ref TEXT,
                event_ts TIMESTAMP NOT NULL,
                user_id TEXT NOT NULL,
                payer_upi_id TEXT,
                payee_upi_id TEXT,
                event_type TEXT NOT NULL,
                transaction_type TEXT,
                bank TEXT NOT NULL,
                amount DOUBLE PRECISION,
                credit_debit TEXT,
                currency_code TEXT,
                status TEXT NOT NULL,
                device_id TEXT NOT NULL,
                app_name TEXT,
                merchant_name TEXT,
                note TEXT,
                date_created TIMESTAMP NOT NULL,
                date_modified TIMESTAMP NOT NULL
            );
            """
        )

        # Add new columns for existing tables created by older versions of this script.
        cur.execute(
            f"""
            ALTER TABLE {TABLE_NAME}
            ADD COLUMN IF NOT EXISTS transaction_ref TEXT,
            ADD COLUMN IF NOT EXISTS payer_upi_id TEXT,
            ADD COLUMN IF NOT EXISTS payee_upi_id TEXT,
            ADD COLUMN IF NOT EXISTS transaction_type TEXT,
            ADD COLUMN IF NOT EXISTS currency_code TEXT,
            ADD COLUMN IF NOT EXISTS credit_debit TEXT,
            ADD COLUMN IF NOT EXISTS app_name TEXT,
            ADD COLUMN IF NOT EXISTS merchant_name TEXT,
            ADD COLUMN IF NOT EXISTS note TEXT;
            """
        )

        cur.execute(
            f"""
            UPDATE {TABLE_NAME}
            SET
                transaction_ref = COALESCE(transaction_ref, 'TXN-' || id::text),
                payer_upi_id = COALESCE(payer_upi_id, user_id || '@okicici'),
                payee_upi_id = COALESCE(payee_upi_id, 'merchant' || id::text || '@okicici'),
                transaction_type = COALESCE(transaction_type, 'P2P'),
                credit_debit = COALESCE(credit_debit, CASE WHEN event_type IN ('money_received', 'refund_success') THEN 'credit' ELSE 'debit' END),
                currency_code = COALESCE(currency_code, 'INR'),
                app_name = COALESCE(app_name, 'PhonePe')
            WHERE
                transaction_ref IS NULL
                OR payer_upi_id IS NULL
                OR payee_upi_id IS NULL
                OR transaction_type IS NULL
                OR credit_debit IS NULL
                OR currency_code IS NULL
                OR app_name IS NULL;
            """
        )

        # Ensure transaction_ref is non-unique so inserts do not conflict on this column.
        cur.execute(
            f"""
            ALTER TABLE {TABLE_NAME}
            DROP CONSTRAINT IF EXISTS upi_transactions_transaction_ref_key;
            """
        )
        cur.execute("DROP INDEX IF EXISTS idx_upi_transactions_transaction_ref;")

        cur.execute(
            f"""
            DO $$
            BEGIN
                ALTER TABLE {TABLE_NAME}
                DROP CONSTRAINT IF EXISTS chk_upi_transactions_date_window;

                UPDATE {TABLE_NAME}
                SET date_created = LEAST(
                    GREATEST(date_created, TIMESTAMP '2025-06-01 00:00:00'),
                    NOW() - INTERVAL '1 day'
                );

                UPDATE {TABLE_NAME}
                SET date_modified = date_created
                    + INTERVAL '1 day'
                    + (random() * EXTRACT(EPOCH FROM INTERVAL '4 days')) * INTERVAL '1 second'
                WHERE
                    date_modified <= date_created
                    OR date_modified - date_created < INTERVAL '1 day'
                    OR date_modified - date_created > INTERVAL '5 days'
                    OR date_modified < TIMESTAMP '2025-06-01 00:00:00'
                    OR date_created > NOW() - INTERVAL '1 day'
                    OR date_modified > NOW();

                UPDATE {TABLE_NAME}
                SET date_modified = LEAST(
                    NOW(),
                    date_created
                        + INTERVAL '1 day'
                        + (random() * EXTRACT(EPOCH FROM INTERVAL '4 days')) * INTERVAL '1 second'
                )
                WHERE date_modified > NOW();

                ALTER TABLE {TABLE_NAME}
                ADD CONSTRAINT chk_upi_transactions_date_window
                CHECK (
                    date_created >= TIMESTAMP '2025-06-01 00:00:00'
                    AND date_modified >= TIMESTAMP '2025-06-01 00:00:00'
                    AND date_created <= NOW()
                    AND date_modified <= NOW()
                    AND date_modified > date_created
                    AND date_modified - date_created >= INTERVAL '1 day'
                    AND date_modified - date_created <= INTERVAL '5 days'
                );
            END
            $$;
            """
        )
    conn.commit()


def _random_user_id() -> str:
    return f"{random.randint(6, 9)}{random.randint(100_000_000, 999_999_999)}"


def _random_upi_id() -> str:
    return f"{faker.first_name().lower()}.{faker.last_name().lower()}{random.choice(_UPI_HANDLES)}".replace(
        " ", ""
    )


def _random_payee_upi_id() -> str:
    return f"{faker.company().lower().replace(' ', '')}{random.randint(10, 999)}{random.choice(_UPI_HANDLES)}"


def _random_amount(event_type: str):
    if event_type in _PAYMENT_EVENTS:
        return round(min(random.expovariate(1 / 500.0), 200000.0), 2)
    return None


def _credit_debit_for_event(event_type: str) -> str:
    if event_type in _CREDIT_EVENTS:
        return "credit"
    return "debit"


def _random_created_modified_pair(now: datetime) -> tuple[datetime, datetime]:
    minimum_modified = _MIN_CREATED_AT + _MIN_MODIFIED_GAP
    if now <= minimum_modified:
        return _MIN_CREATED_AT, minimum_modified

    modified_seconds_span = int((now - minimum_modified).total_seconds())
    modified_at = minimum_modified + timedelta(
        seconds=random.randint(0, max(0, modified_seconds_span))
    )

    created_lower_bound = max(_MIN_CREATED_AT, modified_at - _MAX_MODIFIED_GAP)
    created_upper_bound = modified_at - _MIN_MODIFIED_GAP
    created_seconds_span = int(
        (created_upper_bound - created_lower_bound).total_seconds()
    )
    created_at = created_lower_bound + timedelta(
        seconds=random.randint(0, max(0, created_seconds_span))
    )
    return created_at, modified_at


def _generate_rows(start_id: int, n_rows: int):
    now = datetime.utcnow()
    rows = []
    for i in range(n_rows):
        event_type = random.choice(_UPI_EVENT_TYPES)
        created_at, modified_at = _random_created_modified_pair(now)
        rows.append(
            (
                start_id + i,
                f"TXN-{start_id + i}-{random.randint(1000, 9999)}",
                now - timedelta(seconds=random.randint(0, 60 * 60 * 24 * 7)),
                _random_user_id(),
                _random_upi_id(),
                _random_payee_upi_id(),
                event_type,
                random.choice(_TXN_TYPES),
                random.choice(_UPI_BANKS),
                _random_amount(event_type),
                _credit_debit_for_event(event_type),
                random.choice(_CURRENCIES),
                random.choice(_STATUSES),
                faker.uuid4(),
                random.choice(_UPI_APPS),
                faker.company(),
                faker.sentence(nb_words=6),
                created_at,
                modified_at,
            )
        )
    return rows


def _next_id(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(MAX(id), -1) + 1 FROM {TABLE_NAME};")
        return int(cur.fetchone()[0])


def insert_transactions(conn, start_id: int, n_rows: int) -> int:
    rows = _generate_rows(start_id, n_rows)
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {TABLE_NAME}
                (
                    id, transaction_ref, event_ts, user_id, payer_upi_id, payee_upi_id,
                    event_type, transaction_type, bank, amount, credit_debit, currency_code,
                    status, device_id, app_name, merchant_name, note,
                    date_created, date_modified
                )
            VALUES %s
            """,
            rows,
            page_size=1000,
        )
    conn.commit()
    return start_id + n_rows


def _row_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
        return int(cur.fetchone()[0])


def _sample_hotspot_transaction_refs(conn, n_rows: int) -> list[str]:
    if n_rows <= 0:
        return []
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT transaction_ref
            FROM (
                SELECT DISTINCT transaction_ref
                FROM {TABLE_NAME}
                WHERE transaction_ref IS NOT NULL
            ) refs
            ORDER BY random()
            LIMIT %s;
            """,
            (n_rows,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def update_existing_transactions(
    conn, n_rows: int, target_refs: list[str] | None = None
) -> int:
    with conn.cursor() as cur:
        if target_refs:
            cur.execute(
                f"""
                UPDATE {TABLE_NAME} t
                SET
                    status = CASE WHEN random() < 0.7 THEN 'success' WHEN random() < 0.9 THEN 'failed' ELSE 'pending' END,
                    event_type = (ARRAY['payment_initiated','payment_success','payment_failed','money_received','balance_check','upi_pin_change','account_linked','login','logout','otp_verified','autopay_setup','collect_request_sent','refund_initiated','refund_success'])[1 + floor(random() * 14)::int],
                    date_modified = LEAST(
                        NOW(),
                        t.date_created
                            + INTERVAL '1 day'
                            + (
                                random() * EXTRACT(EPOCH FROM INTERVAL '4 days')
                            ) * INTERVAL '1 second'
                    )
                WHERE t.transaction_ref = ANY(%s);
                """,
                (target_refs,),
            )
        else:
            cur.execute(
                f"""
                WITH picked AS (
                    SELECT transaction_ref
                    FROM (
                        SELECT DISTINCT transaction_ref
                        FROM {TABLE_NAME}
                        WHERE transaction_ref IS NOT NULL
                    ) refs
                    ORDER BY random()
                    LIMIT %s
                )
                UPDATE {TABLE_NAME} t
                SET
                    status = CASE WHEN random() < 0.7 THEN 'success' WHEN random() < 0.9 THEN 'failed' ELSE 'pending' END,
                    event_type = (ARRAY['payment_initiated','payment_success','payment_failed','money_received','balance_check','upi_pin_change','account_linked','login','logout','otp_verified','autopay_setup','collect_request_sent','refund_initiated','refund_success'])[1 + floor(random() * 14)::int],
                    date_modified = LEAST(
                        NOW(),
                        t.date_created
                            + INTERVAL '1 day'
                            + (
                                random() * EXTRACT(EPOCH FROM INTERVAL '4 days')
                            ) * INTERVAL '1 second'
                    )
                FROM picked
                WHERE t.transaction_ref = picked.transaction_ref;
                """,
                (n_rows,),
            )
        updated = cur.rowcount
    conn.commit()
    return updated


def update_date_modified_only(
    conn, n_rows: int, target_refs: list[str] | None = None
) -> int:
    with conn.cursor() as cur:
        if target_refs:
            cur.execute(
                f"""
                UPDATE {TABLE_NAME} t
                SET
                    date_modified = LEAST(
                        NOW(),
                        t.date_created
                            + INTERVAL '1 day'
                            + (
                                random() * EXTRACT(EPOCH FROM INTERVAL '4 days')
                            ) * INTERVAL '1 second'
                    )
                WHERE t.transaction_ref = ANY(%s);
                """,
                (target_refs,),
            )
        else:
            cur.execute(
                f"""
                WITH picked AS (
                    SELECT transaction_ref
                    FROM (
                        SELECT DISTINCT transaction_ref
                        FROM {TABLE_NAME}
                        WHERE transaction_ref IS NOT NULL
                    ) refs
                    ORDER BY random()
                    LIMIT %s
                )
                UPDATE {TABLE_NAME} t
                SET
                    date_modified = LEAST(
                        NOW(),
                        t.date_created
                            + INTERVAL '1 day'
                            + (
                                random() * EXTRACT(EPOCH FROM INTERVAL '4 days')
                            ) * INTERVAL '1 second'
                    )
                FROM picked
                WHERE t.transaction_ref = picked.transaction_ref;
                """,
                (n_rows,),
            )
        updated = cur.rowcount
    conn.commit()
    return updated


def update_hotspot_same_day_intervals(conn, target_refs: list[str]) -> tuple[int, int]:
    if not target_refs:
        return 0, 0

    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {TABLE_NAME} t
            SET
                status = CASE WHEN random() < 0.7 THEN 'success' WHEN random() < 0.9 THEN 'failed' ELSE 'pending' END,
                event_type = (ARRAY['payment_initiated','payment_success','payment_failed','money_received','balance_check','upi_pin_change','account_linked','login','logout','otp_verified','autopay_setup','collect_request_sent','refund_initiated','refund_success'])[1 + floor(random() * 14)::int],
                date_modified = LEAST(
                    NOW(),
                    date_trunc('day', t.date_created + INTERVAL '2 days') + INTERVAL '10 hours'
                )
            WHERE t.transaction_ref = ANY(%s);
            """,
            (target_refs,),
        )
        first_pass = cur.rowcount

        cur.execute(
            f"""
            UPDATE {TABLE_NAME} t
            SET date_modified = LEAST(
                NOW(),
                date_trunc('day', t.date_created + INTERVAL '2 days') + INTERVAL '14 hours'
            )
            WHERE t.transaction_ref = ANY(%s);
            """,
            (target_refs,),
        )
        second_pass = cur.rowcount

    conn.commit()
    return first_pass, second_pass


def run_live_mutations(loop_seconds: int = 5) -> None:
    conn = _connect()
    create_upi_transactions_table(conn)

    next_id = _next_id(conn)
    logger.info("Starting upi_transactions mutation loop from id=%s", next_id)

    while True:
        cycle_start = time.time()

        # Always insert first, then run updates in the same cycle.
        inserts = random.randint(300, 600)
        next_id = insert_transactions(conn, next_id, inserts)

        existing_count = _row_count(conn)
        if existing_count >= 1200:
            max_updates = min(1200, existing_count)
            updates_target = random.randint(600, max_updates)
            date_only_target = max(1, int(updates_target * 0.4))
            full_update_target = updates_target - date_only_target

            full_updates_done = update_existing_transactions(conn, full_update_target)
            date_only_updates_done = update_date_modified_only(conn, date_only_target)

            hotspot_refs = _sample_hotspot_transaction_refs(
                conn, min(_HOTSPOT_REUPDATE_COUNT, existing_count)
            )
            hotspot_full_done, hotspot_date_only_done = (
                update_hotspot_same_day_intervals(conn, hotspot_refs)
            )

            updates_done = (
                full_updates_done
                + date_only_updates_done
                + hotspot_full_done
                + hotspot_date_only_done
            )
        else:
            updates_target = 0
            date_only_target = 0
            full_update_target = 0
            full_updates_done = 0
            date_only_updates_done = 0
            hotspot_full_done = 0
            hotspot_date_only_done = 0
            updates_done = 0

        logger.info(
            "Cycle complete: inserted=%s, requested_updates=%s, requested_full_updates=%s, requested_date_only_updates=%s, full_updates_done=%s, date_only_updates_done=%s, hotspot_full_done=%s, hotspot_date_only_done=%s, updated=%s, total=%s",
            inserts,
            updates_target,
            full_update_target,
            date_only_target,
            full_updates_done,
            date_only_updates_done,
            hotspot_full_done,
            hotspot_date_only_done,
            updates_done,
            _row_count(conn),
        )

        elapsed = time.time() - cycle_start
        sleep_time = max(0.0, loop_seconds - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("UPI_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    run_live_mutations(loop_seconds=5)
