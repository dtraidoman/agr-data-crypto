from __future__ import annotations

import logging

import httpx

log = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class Notifier:
    """Telegram alerts, deduplicated through the state store when asked.

    Alerts are optional: a missing token turns every call into a no-op, so the
    collector never depends on the alerting path.
    """

    def __init__(self, token: str = "", chat_id: str = "", state=None) -> None:
        self._token = token
        self._chat_id = chat_id
        self._state = state

    @property
    def enabled(self) -> bool:
        return bool(self._token and self._chat_id)

    async def send(self, text: str, *, kind: str = "info", dedupe_ttl_ms: int = 0) -> bool:
        if not self.enabled:
            log.debug("notifier disabled, dropping [%s] %s", kind, text)
            return False
        if dedupe_ttl_ms and self._state is not None and not self._state.once(f"alert:last:{kind}", dedupe_ttl_ms):
            log.info("alert [%s] suppressed as duplicate", kind)
            return False
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    TELEGRAM_API.format(token=self._token),
                    json={
                        "chat_id": self._chat_id,
                        "text": text[:4000],
                        "disable_web_page_preview": True,
                    },
                )
            if response.status_code >= 400:
                log.warning("telegram error %s: %s", response.status_code, response.text[:200])
                return False
            return True
        except httpx.HTTPError as exc:
            log.warning("telegram unreachable: %s", exc)
            return False
