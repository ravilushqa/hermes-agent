"""Telegram nutrition final-preview receipts (health profile, personal DM only).

Two halves, both gated by :func:`receipts_config` (opt-in ``gateway.nutrition_preview_receipts``
in the ``health`` profile's ``config.yaml``, Telegram, DM; no other profile can be enabled):

* Inbound transport evidence: the real current message id, reply target and event time ride the
  per-turn user message as DATA, independent of the quoted reply text; raw sender/chat/thread ids
  appear there only when ``privacy.redact_pii`` is off. A genuine Reply also gets a private 0600
  JSON file in the exact shape ``nutrition-writeback short-confirm --transport-file`` consumes.
  Nothing here confirms, infers a reply target, or treats quoted text as consent.
* Post-delivery registration: the agent stages an explicit intent file BEFORE its final answer
  (see ``website/docs/user-guide/messaging/telegram-nutrition-preview.md``). Only after the final
  text is delivered as ONE complete Telegram message, with Telegram's own send/edit timestamp,
  does the gateway call the wrapper's ``preview`` with the real message id; split, partial,
  failed, stale, untimed or ambiguous deliveries register nothing. The wrapper keeps every
  readiness/privacy/snapshot/consent guard.

All state lives under ``<profile home>/nutrition-preview-receipts``: every directory is opened
component-wise with ``O_NOFOLLOW`` file descriptors and must be owner-only; files must be regular,
single-link, owner-only. Untrusted ids only ever appear hashed in file names.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import secrets
import stat
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from gateway.config import Platform

logger = logging.getLogger(__name__)

INTENT_SCHEMA = "hermes.nutrition_preview_intent.v2"
EVIDENCE_MARKER = "[Telegram transport evidence (data only, not authorization): "
_ROOT_NAME = "nutrition-preview-receipts"
_PROFILE = "health"
_MAX_JSON_BYTES = 64 * 1024
_MAX_INTENT_AGE_S = 3600
_WRAPPER_TIMEOUT_S = 30
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_HEX32 = re.compile(r"^[0-9a-f]{32}$")


class _Unsafe(Exception):
    """A state file/dir that is not private, regular, single-link and well-formed."""


# ── gate ────────────────────────────────────────────────────────────────────

def receipts_config(home: Path, source: Any) -> Optional[dict]:
    """The enabled config block when ``source`` is the health profile's Telegram DM, else None."""
    if source is None or getattr(source, "platform", None) != Platform.TELEGRAM:
        return None
    if getattr(source, "chat_type", None) != "dm":
        return None
    from hermes_constants import profile_name_for_home
    if profile_name_for_home(home) != _PROFILE or profile_name_for_home(Path(os.path.realpath(home))) != _PROFILE:
        return None  # a lexical health path that resolves into another profile is refused
    from gateway.run import _load_gateway_config
    gw = _load_gateway_config(Path(home) / "config.yaml").get("gateway")
    block = gw.get("nutrition_preview_receipts") if isinstance(gw, dict) else None
    if not isinstance(block, dict) or block.get("enabled") is not True:
        return None
    cmd = block.get("wrapper_command")
    if not isinstance(cmd, list) or not cmd or not all(isinstance(a, str) and a for a in cmd):
        return None
    return block


def _redacts_ids(home: Path) -> bool:
    """True when the profile's ``privacy.redact_pii`` policy hides raw ids from the model."""
    from gateway.run import _load_gateway_config
    from gateway.session import _should_redact_pii
    privacy = _load_gateway_config(Path(home) / "config.yaml").get("privacy")
    enabled = bool(privacy.get("redact_pii")) if isinstance(privacy, dict) else False
    return _should_redact_pii(Platform.TELEGRAM, enabled)


# ── private state: fd-relative, no symlink traversal ────────────────────────

def _check_ancestors(real_home: Path) -> None:
    """Every ancestor of the (already resolved) home is a dir only root/us can rename within."""
    for p in (real_home, *real_home.parents):
        st = os.stat(p)
        if st.st_uid not in (0, os.geteuid()) or (st.st_mode & 0o022 and not st.st_mode & stat.S_ISVTX):
            raise _Unsafe("profile home ancestor is writable by others")


def _open_dir(home: Path, *names: str, create: bool = True) -> int:
    """fd of ``<home>/nutrition-preview-receipts/<names...>``; each component is NOFOLLOW, owned by
    us, mode 0700 (created so when missing). Caller closes the fd."""
    real = Path(os.path.realpath(home))
    _check_ancestors(real)
    fd = os.open(real, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for name in (_ROOT_NAME, *names):
            if create:
                try:
                    os.mkdir(name, 0o700, dir_fd=fd)
                except FileExistsError:
                    pass
            nxt = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = nxt
            st = os.fstat(fd)
            if st.st_uid != os.geteuid() or st.st_mode & 0o077:
                raise _Unsafe("state directory is not private")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _read_json_at(dfd: int, name: str) -> dict:
    """Parsed object from a regular, single-link, owner-only file; _Unsafe on anything else."""
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dfd)
    try:
        st = os.fstat(fd)
        if (not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_uid != os.geteuid()
                or st.st_mode & 0o077 or st.st_size > _MAX_JSON_BYTES):
            raise _Unsafe("state file is not a private regular file")
        raw = os.read(fd, _MAX_JSON_BYTES + 1)
    finally:
        os.close(fd)
    try:
        data = json.loads(raw.decode("utf-8"))
    except ValueError as exc:
        raise _Unsafe("state file is not valid JSON") from exc
    if not isinstance(data, dict):
        raise _Unsafe("state file is not a JSON object")
    return data


def _write_json_at(dfd: int, name: str, data: dict, *, exclusive: bool = False, mode: int = 0o600) -> None:
    tmp = f".{secrets.token_hex(6)}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=dfd)
    try:
        os.write(fd, json.dumps(data, ensure_ascii=False, sort_keys=True).encode("utf-8"))
        os.fchmod(fd, mode)
    finally:
        os.close(fd)
    if exclusive:
        try:
            os.link(tmp, name, src_dir_fd=dfd, dst_dir_fd=dfd, follow_symlinks=False)
        finally:
            os.unlink(tmp, dir_fd=dfd)
    else:
        os.replace(tmp, name, src_dir_fd=dfd, dst_dir_fd=dfd)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _ident(value: Any) -> str:
    """Literal identifier text (never normalised: transport ids are compared byte for byte)."""
    return "" if value is None else str(value)


def binding_token(session_key: str, sender_id: str, chat_id: str, thread_id: str) -> str:
    """Opaque digest of the conversation a turn belongs to; what a staged intent binds to, so the
    model never has to carry raw user/chat ids."""
    return _sha("\x1f".join((session_key, sender_id, chat_id, thread_id)))


# ── inbound transport evidence ──────────────────────────────────────────────

def _iso(ts: Any) -> Optional[str]:
    if isinstance(ts, datetime) and ts.tzinfo is not None:
        return ts.astimezone(timezone.utc).isoformat()
    return None  # naive/absent: never guess an offset


def transport_evidence(event: Any, source: Any, session_key: str) -> dict:
    """Operational metadata of THIS inbound event, read straight from the adapter's event."""
    return {
        "session_key": session_key,
        "sender_id": _ident(getattr(source, "user_id", None)),
        "chat_id": _ident(getattr(source, "chat_id", None)),
        "thread_id": _ident(getattr(source, "thread_id", None)),
        "message_id": _ident(getattr(event, "message_id", None)),
        "reply_to_message_id": _ident(getattr(event, "reply_to_message_id", None)),
        "timestamp": _iso(getattr(event, "timestamp", None)),
    }


def _write_transport_file(home: Path, ev: dict, text: str) -> Optional[str]:
    record = {k: ev[k] for k in (
        "sender_id", "chat_id", "thread_id", "message_id", "timestamp", "reply_to_message_id")}
    record["text"] = text
    name = f"{_sha(ev['session_key'] + chr(31) + ev['message_id'])[:32]}.json"
    dfd = _open_dir(home, "transport")
    try:
        _write_json_at(dfd, name, record)
    finally:
        os.close(dfd)
    return str(Path(os.path.realpath(home)) / _ROOT_NAME / "transport" / name)


def inbound_evidence_note(home: Path, event: Any, source: Any, session_key: str) -> Optional[str]:
    """Per-turn model-visible evidence line (None when not gated in). Also writes the 0600
    short-confirm transport file when the user actually used Telegram's Reply."""
    if receipts_config(home, source) is None:
        return None
    ev = transport_evidence(event, source, session_key)
    if not ev["message_id"]:
        return None
    shown = {k: ev[k] for k in ("message_id", "reply_to_message_id", "timestamp")}
    shown["binding"] = binding_token(session_key, ev["sender_id"], ev["chat_id"], ev["thread_id"])
    if _redacts_ids(home):
        shown["identifiers_redacted"] = True  # raw ids stay in the private transport file only
    else:
        shown.update({k: ev[k] for k in ("session_key", "sender_id", "chat_id", "thread_id")})
    if ev["reply_to_message_id"] and ev["timestamp"] and ev["sender_id"] and ev["chat_id"]:
        try:
            shown["transport_file"] = _write_transport_file(home, ev, str(getattr(event, "text", "") or ""))
        except (OSError, _Unsafe):
            logger.warning("could not write the private transport evidence file")
    return EVIDENCE_MARKER + json.dumps(shown, ensure_ascii=False, sort_keys=True) + "]"


def strip_evidence_note(event: Any, message_text: Any) -> Any:
    """Authored text for the durable user row: peel exactly the note THIS event injected (an
    authored line that merely looks like one is user content and stays)."""
    note = getattr(event, "_preview_evidence_note", None)
    if isinstance(note, str) and isinstance(message_text, str) and message_text.startswith(note + "\n\n"):
        return message_text[len(note) + 2:]
    return message_text


# ── staged intent + post-delivery registration ──────────────────────────────

def _parse_aware(value: Any) -> Optional[datetime]:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _retire(staged_fd: int, retired_fd: int, name: str, why: str) -> None:
    try:
        os.replace(name, f"{_sha(name)[:12]}.{why}.{secrets.token_hex(3)}.json",
                   src_dir_fd=staged_fd, dst_dir_fd=retired_fd)
    except OSError:
        logger.warning("could not retire a staged intent")


def _scan_intents(staged_fd: int, retired_fd: int, binding: str, turn_id: str) -> tuple:
    """(matching [(name, intent)], unsafe_count). Files that cannot be read safely are quarantined
    and counted: they may have been this turn's intent, so the caller must not guess."""
    found, unsafe = [], 0
    now = datetime.now(timezone.utc)
    for name in sorted(os.listdir(staged_fd)):
        if name.startswith(".") or not name.endswith(".json"):
            continue
        try:
            data = _read_json_at(staged_fd, name)
        except (OSError, _Unsafe):
            unsafe += 1
            _retire(staged_fd, retired_fd, name, "unreadable")
            continue
        if data.get("schema") != INTENT_SCHEMA:
            continue
        if (data.get("binding"), _ident(data.get("inbound_message_id"))) != (binding, turn_id):
            continue
        exp = _parse_aware(data.get("expires_at"))
        if exp is None or exp < now or exp > now + timedelta(seconds=_MAX_INTENT_AGE_S):
            _retire(staged_fd, retired_fd, name, "expired_or_unbounded")
            continue
        found.append((name, data))
    return found, unsafe


def _intent_text_ok(data: dict, delivered_text: str) -> bool:
    want = str(data.get("final_text_sha256") or "").lower()
    if not _HEX64.match(want):
        return False
    return want in {_sha(delivered_text), _sha(delivered_text.strip())}


def _intent_fields_ok(data: dict) -> bool:
    ver = data.get("draft_version")
    return (isinstance(data.get("draft_id"), str) and bool(data["draft_id"])
            and isinstance(ver, int) and not isinstance(ver, bool) and ver >= 1
            and bool(_HEX64.match(str(data.get("snapshot_hash") or "").lower())))


def _run_wrapper(argv: list) -> tuple:
    """Seam for tests: run the installed wrapper, return (returncode, stdout, stderr)."""
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=_WRAPPER_TIMEOUT_S, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def _receipt_key(session_key: str, turn_id: str, draft_id: str) -> str:
    """One registration attempt per turn+draft, whatever message id a retry would deliver."""
    return _sha("\x1f".join((session_key, turn_id, draft_id)))[:32]


def _claim_receipt(rcpt_fd: int, key: str, record: dict) -> bool:
    try:
        _write_json_at(rcpt_fd, f"{key}.json", record, exclusive=True)
    except FileExistsError:
        return False
    return True


def _finish_receipt(home: Path, rcpt_fd: int, key: str, record: dict, status: str, **extra: Any) -> dict:
    record = {**record, "status": status, **extra}
    name = f"{key}.json"
    _write_json_at(rcpt_fd, name, record, mode=0o400)  # read-only operational record
    record["operational_link"] = str(Path(os.path.realpath(home)) / _ROOT_NAME / "receipts" / name)
    return record


def _delivery_rejection(intent: dict, text: str, mid: str, continuation: tuple, partial: bool,
                        ambiguous: bool, sent_at: Optional[datetime]) -> Optional[str]:
    if not mid:
        return "no delivered message id"
    if continuation or partial:
        return "final preview was split across messages"
    if ambiguous:
        return "delivery outcome uncertain"
    if sent_at is None:
        return "no Telegram send/edit timestamp for the delivered message"
    if not _intent_fields_ok(intent):
        return "staged intent is malformed"
    if not _intent_text_ok(intent, text):
        return "delivered text differs from the staged preview"
    return None


def _wrapper_error(err: str) -> str:
    try:
        msg = json.loads(err).get("error")
    except (ValueError, AttributeError):
        msg = None
    return str(msg or "wrapper refused the preview")[:200]


def _registered_preview_id(out: str, intent: dict) -> Optional[str]:
    """The wrapper's preview id, only if its reply is a success object bound to THIS draft."""
    try:
        reply = json.loads(out)
    except ValueError:
        return None
    if not isinstance(reply, dict) or reply.get("ok") is not True or reply.get("action") != "preview":
        return None
    pid = reply.get("preview_id")
    if not isinstance(pid, str) or not _HEX32.match(pid):
        return None
    if (reply.get("client_record_id") != intent["draft_id"]
            or reply.get("client_record_version") != intent["draft_version"]
            or str(reply.get("snapshot_hash") or "").lower() != str(intent["snapshot_hash"]).lower()):
        return None
    return pid


def _run_registration(cfg: dict, ev: dict, intent: dict, mid: str, sent_at: datetime) -> tuple:
    """(status, extra) of the wrapper ``preview`` call."""
    argv = [*cfg["wrapper_command"], "preview",
            "--id", intent["draft_id"], "--confirmer", str(cfg.get("confirmer") or "telegram-user"),
            "--user-id", ev["sender_id"], "--chat-id", ev["chat_id"], "--thread-id", ev["thread_id"],
            "--preview-message-id", mid, "--preview-sent-at", sent_at.isoformat(),
            "--expected-version", str(intent["draft_version"]),
            "--expected-snapshot-hash", str(intent["snapshot_hash"]).lower(),
            "--ttl-seconds", str(int(cfg.get("ttl_seconds") or 900))]
    try:
        rc, out, err = _run_wrapper(argv)
    except (OSError, subprocess.SubprocessError) as exc:
        return "uncertain", {"reason": f"wrapper call failed: {type(exc).__name__}"}
    if rc != 0:
        return "wrapper_rejected", {"reason": _wrapper_error(err)}
    pid = _registered_preview_id(out, intent)
    if pid is None:
        # rc 0 but no verifiable reply: the preview may exist, so do not claim success or retry.
        return "uncertain", {"reason": "wrapper reply was not a valid preview registration"}
    return "registered", {"preview_id": pid, "preview_sent_at": sent_at.isoformat(), "registered_at": time.time()}


def register_delivered_final(
    home: Path, event: Any, source: Any, session_key: str, final_text: str, *,
    message_id: Any, sent_at: Any = None, continuation_ids: tuple = (), partial: bool = False,
    delivery_ambiguous: bool = False,
) -> Optional[dict]:
    """Register the wrapper preview for a final that reached Telegram as ONE complete message.

    ``sent_at`` is Telegram's own ``Message.date`` / ``edit_date`` as reported by the adapter.
    Returns the finished receipt, or None when nothing applied (gated out, no staged intent, or
    this turn+draft was already handled). Rejections are recorded as receipts too.
    """
    cfg = receipts_config(home, source)
    if cfg is None:
        return None
    turn_id = _ident(getattr(event, "message_id", None))
    ev = transport_evidence(event, source, session_key)
    if not turn_id or not ev["sender_id"] or not ev["chat_id"]:
        return None
    try:
        return _register(home, cfg, event, ev, turn_id, final_text, _ident(message_id), _parse_aware(sent_at),
                         tuple(continuation_ids), partial, delivery_ambiguous)
    except (OSError, _Unsafe):
        logger.warning("preview receipt state is not usable; nothing registered", exc_info=True)
        return None


def _register(home, cfg, event, ev, turn_id, final_text, mid, sent_at, continuation, partial, ambiguous):
    binding = binding_token(ev["session_key"], ev["sender_id"], ev["chat_id"], ev["thread_id"])
    staged_fd, retired_fd, rcpt_fd = (_open_dir(home, d) for d in ("staged", "retired", "receipts"))
    try:
        intents, unsafe = _scan_intents(staged_fd, retired_fd, binding, turn_id)
        if not intents and not unsafe:
            return None
        base = {"session_key": ev["session_key"], "turn_id": turn_id, "message_id": mid, "binding": binding}
        if len(intents) > 1 or unsafe:
            for name, _ in intents:
                _retire(staged_fd, retired_fd, name, "ambiguous")
            key = _receipt_key(ev["session_key"], turn_id, "ambiguous")
            reason = "multiple staged intents for one turn" if len(intents) > 1 else "unreadable staged file present"
            if not _claim_receipt(rcpt_fd, key, {**base, "status": "registering"}):
                return None
            return _finish_receipt(home, rcpt_fd, key, base, "rejected", reason=reason)
        name, intent = intents[0]
        base.update(draft_id=intent.get("draft_id"), draft_version=intent.get("draft_version"),
                    snapshot_hash=str(intent.get("snapshot_hash") or "").lower())
        key = _receipt_key(ev["session_key"], turn_id, str(intent.get("draft_id")))
        if not _claim_receipt(rcpt_fd, key, {**base, "status": "registering"}):
            return None  # this turn+draft already has a receipt: never a second registration
        reason = _delivery_rejection(intent, final_text, mid, continuation, partial, ambiguous, sent_at)
        _retire(staged_fd, retired_fd, name, "rejected" if reason else "consumed")
        if reason:
            return _finish_receipt(home, rcpt_fd, key, base, "rejected", reason=reason)
        status, extra = _run_registration(cfg, ev, intent, mid, sent_at)
        return _finish_receipt(home, rcpt_fd, key, base, status, **extra)
    finally:
        for fd in (staged_fd, retired_fd, rcpt_fd):
            os.close(fd)


def _snapshot(mid: Any, text: str, sent_at: Any, ambiguous: bool) -> dict:
    return {"message_id": str(mid), "text": text, "sent_at": sent_at, "ambiguous": ambiguous}


def streamed_final_snapshot(consumer: Any, final_text: str) -> Optional[dict]:
    """Delivery facts of a streamed/edited final, or None when it is not one clean message."""
    if consumer is None or getattr(consumer, "_turn_split_delivery", False):
        return None
    mid = getattr(consumer, "message_id", None)
    if not mid or not getattr(consumer, "final_content_delivered", False):
        return None
    matcher = getattr(consumer, "delivered_final_matches", None)
    if not callable(matcher) or matcher(final_text) is not True:
        return None
    # delivered_final_matches also accepts an EARLIER segment/commentary; the bound message must
    # be the one whose recorded turn-final payload IS the final text.
    recorded = getattr(consumer, "_delivered_final_text", None)
    if recorded is None or recorded.strip() != consumer._display_payload(final_text).strip():
        return None
    times = getattr(consumer, "_platform_message_times", None) or {}
    return _snapshot(mid, final_text, times.get(str(mid)), bool(getattr(consumer, "_delivery_ambiguous", False)))


def edited_final_snapshot(consumer: Any, edit_result: Any, content: str) -> Optional[dict]:
    """Delivery facts after the gateway edited the streamed message in place to ``content``."""
    raw = getattr(edit_result, "raw_response", None)
    mid = getattr(consumer, "message_id", None)
    if (not mid or mid == "__no_edit__" or getattr(consumer, "_turn_split_delivery", False)
            or getattr(edit_result, "continuation_message_ids", ()) or getattr(consumer, "_delivery_ambiguous", False)
            or (isinstance(raw, dict) and (raw.get("partial_overflow") or raw.get("skipped")))):
        return None
    sent_at = raw.get("telegram_message_time") if isinstance(raw, dict) else None
    return _snapshot(mid, content, sent_at, False)


# ── runner-facing entry points (profile home resolved from the source, never ambient) ──

def _home_for(runner: Any, source: Any) -> Optional[Path]:
    resolve = getattr(runner, "_resolve_profile_home_for_source", None)
    return Path(resolve(source)) if callable(resolve) else None


def prepend_evidence(runner: Any, event: Any, source: Any, session_key: Optional[str], message_text: str) -> str:
    """Outermost per-turn prefix on the API-bound user message (never the cached system prompt).
    The injected note is remembered on the event so only it is stripped from the durable row."""
    home = _home_for(runner, source)
    if home is None or not session_key or not isinstance(message_text, str):
        return message_text
    note = inbound_evidence_note(home, event, source, session_key)
    if not note:
        return message_text
    event._preview_evidence_note = note
    return f"{note}\n\n{message_text}"


async def register_standard_final(runner: Any, event: Any, session_key: str, text: str, result: Any) -> None:
    """Hook for the normal-lane final send: runs only after a successful delivery."""
    if not getattr(result, "success", False):
        return
    import asyncio
    home = _home_for(runner, event.source)
    if home is None:
        return
    raw = getattr(result, "raw_response", None)
    raw = raw if isinstance(raw, dict) else {}
    await asyncio.to_thread(
        register_delivered_final, home, event, event.source, session_key, text,
        message_id=result.message_id, sent_at=raw.get("telegram_message_time"),
        continuation_ids=tuple(result.continuation_message_ids or ()),
        partial=bool(raw.get("partial_overflow")))


async def register_streamed_final(runner: Any, event: Any, source: Any, session_key: str,
                                  snapshot: Optional[dict]) -> None:
    """Hook for a streamed/edited final the consumer confirmed as one complete message."""
    import asyncio
    home = _home_for(runner, source)
    if home is None or not snapshot:
        return
    await asyncio.to_thread(
        register_delivered_final, home, event, source, session_key, snapshot["text"],
        message_id=snapshot["message_id"], sent_at=snapshot["sent_at"],
        delivery_ambiguous=snapshot["ambiguous"])
