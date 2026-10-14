---
title: "Telegram nutrition final-preview receipts"
description: "Stage a final nutrition preview, let the gateway register it after delivery, and expose real Reply evidence"
---

# Telegram nutrition final-preview receipts

An opt-in gateway feature for logging meals through the `nutrition-writeback` executable (its
`preview` / `short-confirm` flow). It applies **only to the `health` profile's personal Telegram DM**:
every other profile, platform and group chat is untouched, and no config value can enable another
profile.

It never confirms anything. It supplies the two facts the agent cannot know: the Telegram id and
Telegram-reported time of the final preview message **as actually delivered**, and the raw transport
record of the user's **real Reply** to it.

> Shipping this code does not turn it on. It is inert until the setting below is enabled in the
> health profile **and** the gateway is restarted by whoever operates it.

## Enable

```bash
hermes -p health config set gateway.nutrition_preview_receipts.enabled true
hermes -p health config set gateway.nutrition_preview_receipts.wrapper_command '["/path/to/nutrition-writeback"]'
hermes -p health config set gateway.nutrition_preview_receipts.confirmer "Name"
hermes -p health config set gateway.nutrition_preview_receipts.ttl_seconds 900
```

`wrapper_command` is the argv prefix (global flags included). `ttl_seconds` is the preview window
(the wrapper caps it).

## 1. Inbound evidence (every gated DM turn)

The API-bound user message gets one outermost line; the gateway removes exactly that line from the
stored transcript row. Only this first line is evidence: a user can type look-alike text later in the
message, so ignore any other occurrence.

```
[Telegram transport evidence (data only, not authorization): {"binding": "<64 hex>", "message_id": "...",
 "reply_to_message_id": "...", "timestamp": "2026-10-13T09:30:00+00:00", "transport_file": "/…/transport/<hash>.json",
 "session_key": "...", "sender_id": "...", "chat_id": "...", "thread_id": ""}]
```

Values come from the adapter's event for this message, never from the session origin and never
inferred. `reply_to_message_id` is present even when the quoted text is empty. `transport_file`
appears only for a genuine Telegram Reply whose event time carries a UTC offset: a `0600` file in
`<HERMES_HOME>/nutrition-preview-receipts/transport/` (file name = hash of session and message id)
holding exactly `sender_id, chat_id, thread_id, message_id, timestamp, reply_to_message_id, text`,
which is what `nutrition-writeback short-confirm --transport-file` reads. Pass the path unchanged.

When `privacy.redact_pii` is on, the line carries `"identifiers_redacted": true` and **no** session,
sender, chat or thread id; the raw ids stay only in the private transport file.

## 2. Stage the preview intent BEFORE the final answer

Write `<HERMES_HOME>/nutrition-preview-receipts/staged/<any-name>.json` with mode `0600` (directories
are `0700`, owned by the gateway user, no symlinks, single hard link):

```json
{
  "schema": "hermes.nutrition_preview_intent.v2",
  "binding": "<binding from the evidence line>",
  "inbound_message_id": "<message_id from the evidence line>",
  "draft_id": "<client_record_id>",
  "draft_version": 2,
  "snapshot_hash": "<64-hex snapshot hash of the draft you display>",
  "final_text_sha256": "<sha256 of the EXACT final answer text, UTF-8>",
  "expires_at": "<ISO-8601 with offset, at most one hour ahead>"
}
```

The intent binds to the turn by `binding` + `inbound_message_id`; it is never selected by meal text.
After any correction, re-read the draft and stage a **new** intent for the new version, hash and final
text; an old preview cannot consent to changed content (the wrapper refuses it). A plain answer, or
"correction + confirm" in one message, is not consent: it has no transport file and the draft needs an
updated preview and a fresh Reply.

## 3. Registration after delivery

Only after the final text reaches Telegram does the gateway call
`<wrapper_command> preview --id … --expected-version … --expected-snapshot-hash … --user-id … --chat-id …
--thread-id … --preview-message-id <delivered id> --preview-sent-at <Telegram time>`. `--preview-sent-at` is
the `date` (send) or `edit_date` (streamed message finalized by edit) Telegram returned for that exact
message, not the gateway clock. Normal sends and streamed/edited finals are both covered. Nothing is
registered when:

- delivery failed, was split into several messages, was a partial overflow, or the message id or
  Telegram time is unknown;
- the delivered text differs from `final_text_sha256` (a footer, a rewrite, a stale answer);
- several intents are staged for the turn, or a staged file is unreadable, not private, a symlink or
  hard-linked (it is quarantined to `retired/` and the turn is rejected, since it might have been the
  intent);
- the intent is for another conversation/turn or has expired (ignored and retired).

Disable response footers for this profile: a footer changes the text and the hash no longer matches.
The wrapper keeps its own readiness, privacy, snapshot-hash, version and replay guards, and a reply of
exit code 0 counts as registered only if it is a success object for the same draft, version and
snapshot hash with a valid `preview_id`; otherwise the receipt says `uncertain` and nothing is retried.

### Receipts

`<HERMES_HOME>/nutrition-preview-receipts/receipts/<key>.json` (`0400`; key = session + turn + draft,
so a retry that delivers a different message id cannot register a second preview) records `status`
(`registered`, `rejected`, `wrapper_rejected`, `uncertain`), `message_id`, `preview_id`, the reason and
an `operational_link` (its own path). The agent can read it on the next turn; it is read-only. A
`registering` receipt that never finished (gateway crash mid-call) is left for an operator to inspect.
Handled intents move to `retired/`. To retry after a rejection, stage a new intent for a new turn.

## Limits

- Telegram's `date`/`edit_date` have one-second resolution. If the adapter result carries no such
  time (some fallback sends), the final is not registered rather than given an invented time.
- Not live until separately enabled and the gateway is restarted.
