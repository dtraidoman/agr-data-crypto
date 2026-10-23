from __future__ import annotations

import logging

import httpx

log = logging.getLogger(__name__)


class Notifier:
    def __init__(self, token: str, chat_id: str, client: httpx.AsyncClient) -> None:
        self._token = token
        self._chat_id = chat_id
        self._client = client

    @property
    def enabled(self) -> bool:
        return bool(self._token and self._chat_id)

    async def send(self, text: str) -> None:
        if not self.enabled:
            return
        try:
            await self._client.post(
                f"https://api.telegram.org/bot{self._token}/sendMessage",
                json={"chat_id": self._chat_id, "text": text[:3900]},
                timeout=10,
            )
        except Exception as exc:
            # Do not log the exception: httpx includes the request URL, which
            # contains the Telegram bot token.
            log.warning("telegram notification failed (%s)", type(exc).__name__)
