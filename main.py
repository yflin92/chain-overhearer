"""
main.py — chain-overhearer entry point.

Polls Ethereum and Base blocks, detects human-readable English/Chinese
messages in transaction calldata, and posts them to Twitter.

Usage:
    # Normal mode (all chains, resume from state.json):
    python main.py

    # Dry-run from a specific block on one chain (no tweets posted):
    python main.py --dry-run --chain base --from-block 27500000
"""

import argparse
import asyncio
import logging
import os

from dotenv import load_dotenv

from chain import CHAINS, explorer_url, poll_chain
from detector import extract_message
from twitter import Tweet, TwitterPoster

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def process_chain(
    chain_name: str,
    poster: TwitterPoster | None,
    start_block: int | None = None,
    dry_run: bool = False,
) -> None:
    """
    Process transactions from chain_name.

    dry_run=True  → log qualifying messages to stdout, never post to Twitter.
    start_block   → override state.json and begin from this block number.
    poster=None   → implies dry_run (safety guard).
    """
    mode = "[DRY RUN] " if dry_run else ""
    logger.info(f"{mode}Starting chain processor: {chain_name}")

    async for tx_hash, chain, calldata in poll_chain(chain_name, start_block=start_block):
        result = extract_message(calldata)
        if result is None:
            continue

        text, lang = result
        url = explorer_url(chain, tx_hash)

        if dry_run or poster is None:
            logger.info(
                f"[DRY RUN] [{chain}] lang={lang} tx={tx_hash[:12]}…\n"
                f"  message : {text[:200]!r}\n"
                f"  url     : {url}"
            )
        else:
            poster.post(
                Tweet(message=text, url=url, lang=lang, chain=chain, tx_hash=tx_hash)
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="chain-overhearer: tweet on-chain messages from EVM transactions"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log qualifying messages without posting to Twitter",
    )
    parser.add_argument(
        "--chain",
        choices=list(CHAINS),
        default=None,
        metavar="CHAIN",
        help=f"Only process this chain (choices: {', '.join(CHAINS)}). Default: all chains.",
    )
    parser.add_argument(
        "--from-block",
        type=int,
        default=None,
        metavar="BLOCK",
        help="Override the starting block number (ignores state.json for this run).",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    # Twitter env vars are only required in live mode
    twitter_vars = [
        "TWITTER_BEARER_TOKEN",
        "TWITTER_API_KEY",
        "TWITTER_API_SECRET",
        "TWITTER_ACCESS_TOKEN",
        "TWITTER_ACCESS_SECRET",
    ]
    required = ["ALCHEMY_API_KEY"] + ([] if args.dry_run else twitter_vars)
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")

    poster = None if args.dry_run else TwitterPoster()

    chain_names = [args.chain] if args.chain else list(CHAINS)

    if args.dry_run:
        logger.info(
            f"DRY RUN — chains: {', '.join(chain_names)}"
            + (f", from block: {args.from_block}" if args.from_block else "")
        )
    else:
        logger.info(f"chain-overhearer running on chains: {', '.join(chain_names)}")

    tasks = [
        asyncio.create_task(
            process_chain(
                chain_name,
                poster=poster,
                start_block=args.from_block,
                dry_run=args.dry_run,
            )
        )
        for chain_name in chain_names
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
