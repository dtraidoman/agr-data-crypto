from __future__ import annotations

import asyncio
import logging
import os

import httpx
from redis.asyncio import Redis

log = logging.getLogger("watchdog")

WORKFLOWS = {"alpha": "collect-a.yml", "bravo": "collect-b.yml"}


async def dispatch(client: httpx.AsyncClient, repo: str, token: str, workflow: str) -> bool:
    response = await client.post(
        f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        json={"ref": "main"},
        timeout=30,
    )
    if response.status_code not in {201, 204}:
        log.error("dispatch %s failed: %s %s", workflow, response.status_code, response.text[:300])
        return False
    log.info("dispatched %s", workflow)
    return True


async def amain() -> int:
    redis_url = os.getenv("COLLECTOR_REDIS_URL", "")
    repo = os.getenv("GITHUB_REPOSITORY", "")
    token = os.getenv("GITHUB_TOKEN", "")
    if not redis_url:
        log.error("COLLECTOR_REDIS_URL is not set")
        return 2
    redis = Redis.from_url(redis_url, decode_responses=True, socket_timeout=10)
    try:
        alive = {role: bool(await redis.exists(f"hb:{role}")) for role in WORKFLOWS}
    finally:
        await redis.aclose()
    log.info("heartbeats: %s", alive)
    missing = [role for role, ok in alive.items() if not ok]
    if not missing:
        return 0
    if not repo or not token:
        log.error("collector heartbeat missing for %s and dispatch credentials are absent", missing)
        return 1
    if len(missing) == len(alive):
        log.warning("no collector heartbeat; dispatching both collectors")
    failures = 0
    async with httpx.AsyncClient() as client:
        for role in missing:
            if not await dispatch(client, repo, token, WORKFLOWS[role]):
                failures += 1
    return 1 if failures else 0


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(asyncio.run(amain()))


if __name__ == "__main__":
    main()
