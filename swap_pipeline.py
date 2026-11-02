# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "web3>=7",
#     "psycopg2-binary",
# ]
# ///
"""
Extract Uniswap V3 Swap events for three pools over a block range and load them into Postgres.

Usage:
    uv run swap_pipeline.py --from 20000000 --to 20000199
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from web3 import Web3

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("swaps")

# ---------- Config ----------

RPC_URL = os.getenv("RPC_URL", "https://ethereum-rpc.publicnode.com")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "postgres"),
    dbname=os.getenv("DB_NAME", "crypto"),
)

POOLS = {
    "0x2dF380544B88AdB3ad0A94100dcC45fd705aAE2d": "XDP/USDC",
    "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640": "ETH/USDC",
    "0x0E4831319A50228B9e450861297aB92dee15B44F": "WBTC/USDC",
}

# Uniswap V3 pool Swap event
SWAP_ABI = [
    {
        "anonymous": False,
        "name": "Swap",
        "type": "event",
        "inputs": [
            {"indexed": True, "name": "sender", "type": "address"},
            {"indexed": True, "name": "recipient", "type": "address"},
            {"indexed": False, "name": "amount0", "type": "int256"},
            {"indexed": False, "name": "amount1", "type": "int256"},
            {"indexed": False, "name": "sqrtPriceX96", "type": "uint160"},
            {"indexed": False, "name": "liquidity", "type": "uint128"},
            {"indexed": False, "name": "tick", "type": "int24"},
        ],
    }
]

SWAP_TOPIC = Web3.to_hex(
    Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)")
)

# int256 / uint160 / uint128 overflow BIGINT, so they go in NUMERIC(78,0).
# Primary key on (tx_hash, log_index) makes reruns idempotent.
DDL = """
CREATE TABLE IF NOT EXISTS swaps (
    pool_address  TEXT          NOT NULL,
    block_number  BIGINT        NOT NULL,
    tx_hash       TEXT          NOT NULL,
    log_index     INTEGER       NOT NULL,
    sender        TEXT          NOT NULL,
    recipient     TEXT          NOT NULL,
    amount0       NUMERIC(78,0) NOT NULL,
    amount1       NUMERIC(78,0) NOT NULL,
    sqrtPriceX96  NUMERIC(78,0) NOT NULL,
    liquidity     NUMERIC(78,0) NOT NULL,
    tick          INTEGER       NOT NULL,
    timestamp     TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS swaps_pool_block_idx ON swaps (pool_address, block_number);
"""

INSERT_SQL = """
INSERT INTO swaps (
    pool_address, block_number, tx_hash, log_index, sender, recipient,
    amount0, amount1, sqrtPriceX96, liquidity, tick, timestamp
) VALUES %s
ON CONFLICT (tx_hash, log_index) DO NOTHING
RETURNING tx_hash
"""


# ---------- Helpers ----------

def with_retry(fn, *args, **kwargs):
    """Call fn, retrying with exponential backoff (2s, 4s, 8s...)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.error("Giving up after %d attempts: %s", attempt, e)
                raise
            wait = 2 ** attempt
            log.warning("Attempt %d/%d failed: %s. Retrying in %ds", attempt, MAX_RETRIES, e, wait)
            time.sleep(wait)


_ts_cache = {}


def block_timestamp(w3, block_number):
    """One RPC call per block, cached so swaps in the same block share it."""
    if block_number not in _ts_cache:
        block = with_retry(w3.eth.get_block, block_number)
        _ts_cache[block_number] = datetime.fromtimestamp(block["timestamp"], tz=timezone.utc)
    return _ts_cache[block_number]


# ---------- Extract + Transform ----------

def extract_chunk(w3, event, pool, start, end):
    logs = with_retry(
        w3.eth.get_logs,
        {"address": pool, "fromBlock": start, "toBlock": end, "topics": [SWAP_TOPIC]},
    )
    rows = []
    for raw in logs:
        ev = event.process_log(raw)
        a = ev["args"]
        rows.append((
            pool,
            ev["blockNumber"],
            Web3.to_hex(ev["transactionHash"]),
            ev["logIndex"],
            a["sender"],
            a["recipient"],
            int(a["amount0"]),
            int(a["amount1"]),
            int(a["sqrtPriceX96"]),
            int(a["liquidity"]),
            int(a["tick"]),
            block_timestamp(w3, ev["blockNumber"]),
        ))
    return rows


# ---------- Load ----------

def load(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        inserted = execute_values(cur, INSERT_SQL, rows, page_size=500, fetch=True)
    conn.commit()
    return len(inserted)


# ---------- Run ----------

def parse_args():
    p = argparse.ArgumentParser(description="Extract Uniswap V3 swaps into Postgres")
    p.add_argument("--from", dest="from_block", type=int, required=True)
    p.add_argument("--to", dest="to_block", type=int, required=True)
    args = p.parse_args()
    if args.from_block > args.to_block:
        p.error("--from must be <= --to")
    return args


def run():
    args = parse_args()

    w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        raise RuntimeError(f"Cannot reach RPC at {RPC_URL}")
    log.info("Connected to RPC, chain id %d", w3.eth.chain_id)

    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

    for pool, name in POOLS.items():
        event = w3.eth.contract(address=pool, abi=SWAP_ABI).events.Swap()
        found = inserted = 0
        for start in range(args.from_block, args.to_block + 1, CHUNK_SIZE):
            end = min(start + CHUNK_SIZE - 1, args.to_block)
            rows = extract_chunk(w3, event, pool, start, end)
            n = load(conn, rows)
            found += len(rows)
            inserted += n
            log.debug("%s blocks %d-%d: %d swaps, %d new", name, start, end, len(rows), n)
        log.info("%s: %d swaps found, %d new rows, %d already in DB",
                 name, found, inserted, found - inserted)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pool_address, COUNT(*)
            FROM swaps
            WHERE block_number BETWEEN %s AND %s
            GROUP BY pool_address
            """,
            (args.from_block, args.to_block),
        )
        counts = dict(cur.fetchall())
    conn.close()

    print(f"\nRow counts in swaps for blocks {args.from_block}-{args.to_block}:")
    for pool, name in POOLS.items():
        print(f"  {name:<10} {pool}  {counts.get(pool, 0)}")


if __name__ == "__main__":
    try:
        run()
    except Exception:
        log.exception("Pipeline failed")
        sys.exit(1)
