"""Telegram's own send/edit timestamp for a delivered message (``SendResult.raw_response``)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Optional


def telegram_time_raw(msg: Any, *, edited: bool = False) -> Optional[dict]:
    """``{"telegram_message_time": <UTC ISO>}`` from Telegram's own ``Message.date`` (``edit_date`` for
    an edit); None when the API result carries none (callers must then fail closed, never invent one)."""
    if isinstance(msg, dict) and isinstance(msg.get("result"), dict):
        msg = msg["result"]  # raw Bot API envelope: {"ok": true, "result": <Message>}
    field = "edit_date" if edited else "date"
    value = msg.get(field) if isinstance(msg, dict) else getattr(msg, field, None)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            value = datetime.fromtimestamp(value, tz=UTC)
        except (OverflowError, OSError, ValueError):  # nan/inf/out of range
            return None
    if not isinstance(value, datetime) or value.tzinfo is None:
        return None
    return {"telegram_message_time": value.astimezone(UTC).isoformat()}
