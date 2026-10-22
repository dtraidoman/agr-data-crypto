#!/usr/bin/env python3
"""Out-of-band watchdog: revives collectors when both heartbeats disappear.

Cron is unreliable in both directions: it can fire late *and* silently stop
firing. This job is the only component that assumes nothing about the other
jobs; it checks Redis and, if the fleet is dead, dispatches the workflows
directly through the GitHub API.

Deliberately dependency-light: redis + httpx only, no collector imports, so it
starts in seconds even if the main package is broken.
"""

from __future__ import annotations

import os
import sys
import time

import httpx
import redis

ROLES = ("alpha", "bravo")
EXCHANGES = ("binance", "bybit")
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
WORKFLOWS = {"alpha": "collect-a.yml", "bravo": "collect-b.yml"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return default if not raw else int(raw)


def alive(ttl_ms: int) -> bool:
    # -2: no key, -1: key without TTL; both mean "not a live heartbeat".
    return ttl_ms is not None and ttl_ms > 0


def send_telegram(text: str) -> None:
    token = os.getenv("COLLECTOR_TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("COLLECTOR_TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return
    try:
        httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text[:4000], "disable_web_page_preview": True},
            timeout=10.0,
        )
    except httpx.HTTPError as exc:
        print(f"telegram failed: {exc}")


def dispatch(repo: str, token: str, role: str) -> bool:
    workflow = WORKFLOWS[role]
    try:
        response = httpx.post(
            f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json={"ref": os.getenv("GITHUB_REF_NAME", "main")},
            timeout=20.0,
        )
    except httpx.HTTPError as exc:
        print(f"dispatch {workflow} failed: {exc}")
        return False
    if response.status_code >= 400:
        print(f"dispatch {workflow} -> {response.status_code}: {response.text[:200]}")
        return False
    print(f"dispatched {workflow}")
    return True


def main() -> int:
    url = os.getenv("COLLECTOR_REDIS_URL", "").strip()
    if not url:
        # A monitoring job that cannot monitor must not look green: a missing
        # secret would otherwise disable the watchdog silently.
        print("COLLECTOR_REDIS_URL is not set; watchdog cannot evaluate health")
        return 2

    try:
        client = redis.from_url(url, decode_responses=True, socket_timeout=5.0, socket_connect_timeout=5.0)
        client.ping()
        ttl = {role: client.pttl(f"hb:{role}") for role in ROLES}
    except redis.RedisError as exc:
        # Redis being down is an incident, but restarting collectors will not
        # fix it. Report and stop, with a non-zero code so the failure is
        # visible in the Actions tab.
        print(f"redis unreachable: {exc}")
        send_telegram(f"[watchdog] Redis unreachable: {exc}")
        return 1

    state = {role: alive(value) for role, value in ttl.items()}
    print(f"heartbeats: {state} (pttl={ttl})")

    stale_minutes = env_int("WATCHDOG_STALE_MINUTES", 15)
    stale_ms = stale_minutes * 60_000
    now = int(time.time() * 1000)

    stale_streams: list[str] = []
    if any(state.values()):
        # One MGET instead of SCAN + N GETs: every command counts on the free tier.
        keys = [f"cursor:kl:{exchange}:{symbol}" for exchange in EXCHANGES for symbol in SYMBOLS]
        try:
            for key, value in zip(keys, client.mget(keys), strict=True):
                if not value:
                    continue
                age = now - int(value)
                if age > stale_ms:
                    stale_streams.append(f"{key.removeprefix('cursor:kl:')} (+{age // 60_000}m)")
        except redis.RedisError as exc:
            print(f"cursor read failed: {exc}")

    if stale_streams:
        print(f"stale cursors: {stale_streams}")
        if client.set("alert:watchdog:stale", "1", nx=True, ex=1800):
            send_telegram(
                "[watchdog] cursors stopped advancing for "
                f">{stale_minutes}m: {', '.join(stale_streams[:10])}"
            )

    if any(state.values()):
        print("at least one collector is alive; nothing to dispatch")
        return 0

    # Nothing is running. Dispatch both roles; the concurrency group prevents
    # duplicate runs if cron fires at the same moment.
    token = os.getenv("GITHUB_TOKEN", "").strip()
    repo = os.getenv("GITHUB_REPOSITORY", "").strip()
    if not token or not repo:
        print("GITHUB_TOKEN / GITHUB_REPOSITORY missing; cannot dispatch")
        return 2

    if not client.set("alert:watchdog:dispatch", "1", nx=True, ex=600):
        print("a dispatch already happened in the last 10 minutes; skipping")
        return 0

    results = [dispatch(repo, token, role) for role in ROLES]
    send_telegram(
        "[watchdog] both collectors were dead; "
        + ("restarted " + ", ".join(r for r, ok in zip(ROLES, results, strict=True) if ok) if any(results) else "restart FAILED")
    )
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
