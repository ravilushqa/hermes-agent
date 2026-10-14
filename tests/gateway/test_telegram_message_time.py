"""The Telegram adapter reports Telegram's own Message.date / edit_date on successful sends and edits."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.telegram.adapter import TelegramAdapter
from plugins.platforms.telegram.telegram_message_time import telegram_time_raw as _telegram_time_raw

SENT = datetime(2026, 10, 13, 9, 31, 7, tzinfo=UTC)
EDITED = datetime(2026, 10, 13, 9, 31, 42, tzinfo=UTC)


@pytest.fixture
def adapter():
    a = TelegramAdapter(PlatformConfig(enabled=True, token="fake-token"))
    a._bot = MagicMock()
    return a


@pytest.mark.parametrize("msg, edited, want", [
    (SimpleNamespace(date=SENT), False, SENT.isoformat()),
    (SimpleNamespace(date=SENT, edit_date=EDITED), True, EDITED.isoformat()),
    ({"date": int(SENT.timestamp())}, False, SENT.isoformat()),
    (SimpleNamespace(date=SENT), True, None),            # an edit with no edit_date: do not reuse the send time
    (SimpleNamespace(date=datetime(2026, 10, 13)), False, None),  # naive
    ({"ok": True, "result": {"message_id": 1, "date": int(SENT.timestamp())}}, False, SENT.isoformat()),
    ({"ok": True, "result": {"message_id": 1, "date": 1, "edit_date": int(EDITED.timestamp())}}, True, EDITED.isoformat()),
    ({"ok": True, "result": {"message_id": 1, "date": 1}}, True, None),
    ({"date": float("nan")}, False, None), ({"date": float("inf")}, False, None), ({"date": 10**30}, False, None),
    ({"date": -10**30}, True, None),
    (True, True, None), (None, False, None), (MagicMock(), False, None),
])
def test_time_comes_only_from_telegram_fields(msg, edited, want):
    raw = _telegram_time_raw(msg, edited=edited)
    assert (raw or {}).get("telegram_message_time") == want


def test_single_chunk_send_reports_message_date(adapter):
    adapter._send_chunk_with_retries = AsyncMock(return_value=(SimpleNamespace(message_id=42, date=SENT), False))
    adapter._retrigger_typing = AsyncMock()
    res = asyncio.run(adapter._send_chunks("1001", ["hi"], [], None, None, ()))
    assert res.success and res.message_id == "42"
    assert res.raw_response["telegram_message_time"] == SENT.isoformat()


def test_finalize_edit_reports_edit_date(adapter):
    adapter._bot.edit_message_text = AsyncMock(return_value=SimpleNamespace(message_id=42, date=SENT, edit_date=EDITED))
    res = asyncio.run(adapter.edit_message("1001", "42", "final text", finalize=True))
    assert res.success and res.raw_response["telegram_message_time"] == EDITED.isoformat()


def test_rich_edit_unwraps_the_raw_api_envelope(adapter):
    envelope = {"ok": True, "result": {"message_id": 42, "date": 1, "edit_date": int(EDITED.timestamp())}}
    adapter._bot.do_api_request = AsyncMock(return_value=envelope)
    adapter._record_rich_sent = AsyncMock()
    res = asyncio.run(adapter._try_edit_rich("1001", "42", "final"))
    assert res.success and res.raw_response["telegram_message_time"] == EDITED.isoformat()
