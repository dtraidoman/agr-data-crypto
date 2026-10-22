from __future__ import annotations

import logging

from ..config import Settings
from .base import Sink
from .memory import MemorySink
from .supabase import SupabaseSink

log = logging.getLogger(__name__)


def build_sink(settings: Settings) -> Sink:
    if settings.dry_run or not settings.supabase_url or not settings.supabase_key:
        log.warning("Supabase is not configured: using in-memory sink (no persistence)")
        return MemorySink()
    return SupabaseSink(settings.supabase_url, settings.supabase_key, settings.supabase_table)


__all__ = ["MemorySink", "Sink", "SupabaseSink", "build_sink"]
