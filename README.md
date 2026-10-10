# Codex Voice for Home Assistant

## Computer fork — Echo Dot 2 / Deutsch

Dieser Fork ergänzt Phase A/B des Computer-Anforderungskatalogs: inhaltlose
Diagnose-Traces, gerätegebundene Kurzzeit-Sitzungen (2–60 Minuten), getrennte
Benutzerkontexte, „neues Gespräch“, begrenzte Wiederherstellung nach Bridge-
Neustart und Schutz vor Replay/konkurrierenden Aufrufen. Assist mit hassmic,
Wyoming `small-int8` Deutsch und Piper bleibt der verwendete Pfad. Langzeitmemory
ist nicht aktiviert; keine Echo-/Firmwareänderungen wurden vorgenommen.

- [Audit und tatsächliche ID-Kette](docs/computer-audit.md)
- [JSON-Hotfix für Home Assistant 2026.10](docs/json-serialization-fix.md)
- [Installation, Konfiguration und Rollback](docs/installation.md)
- [Meilensteine, Testnachweise und offene Hardware-Abnahme](docs/phase-ab-report.md)
- [Datenschutz](docs/memory-and-privacy.md), [Fehlerdiagnose](docs/troubleshooting.md)
- [Konkrete Folgeveränderungen für Tools, Memory und STT](docs/future-changes.md)

Domain `codex_voice` ist parallel zur bestehenden OpenAI Codex Conversation
(`codex_conversation`) installierbar. Bestehende Fork-Profile bleiben bis zur
expliziten Aktivierung im bisherigen ID-Modus. Die ursprüngliche Dokumentation
unterhalb betrifft teilweise ThirdReality und Spanisch; sie ist kein Auftrag
zur Verwendung dieser Realtime-Strecke auf dem Echo.

Codex Voice is an unofficial Home Assistant integration that exposes a
Conversation agent, an experimental text-to-speech adapter, an experimental
speech-to-text adapter, and an experimental direct realtime transport
backed by a user's existing ChatGPT/Codex login. The recommended Assist
configuration uses Home Assistant's native Wyoming integration for local
faster-whisper STT and local Piper TTS, with Codex Voice providing the
Conversation stage between them.

> [!WARNING]
> This project relies on experimental Codex App Server interfaces. It can
> break when Codex changes, is not an official OpenAI or Home Assistant
> integration, and is not a substitute for the stable, separately billed
> OpenAI API. ChatGPT plan availability, quotas, and usage policies still
> apply. The project never converts, exports, or presents OAuth credentials as
> an API key.

## Architecture

Home Assistant and the ChatGPT login are deliberately separated:

```text
ThirdReality: "Okay Nabu"
  -> Home Assistant Assist pipeline
     -> Wyoming faster-whisper STT (local)
     -> Codex Voice Conversation (ChatGPT OAuth)
     -> Wyoming Piper TTS (local service on external host, es_MX-ald-medium)
  -> speaker

ThirdReality: "Okay Computer"
  -> in-process stdlib client -> realtime wire v2 -> bridge
  -> explicit conversation_mode: native
  -> one native Codex App Server WebRTC voice thread -> speaker

No Home Assistant broker, transcript executor, or appendSpeech render handoff
participates in the Okay Computer session.
```

HACS installs only `custom_components/codex_voice`. The bridge must run on a
machine with the Codex CLI and a valid, file-backed `codex login`. It locates
the existing `auth.json`, links that credential into a mode-0700 temporary
Codex home, and forces file-backed CLI authentication so Codex refreshes the
original credential through the link. Normal Codex history, configuration,
apps, plugins, and MCP servers are not imported into voice sessions. The
bridge sends Home Assistant only text/audio results; Home Assistant never
receives the ChatGPT credential. Device-facing v2 returns only audio and
content-free lifecycle controls. The device never declares tools or receives
tool calls/results. The current reference client always requests native mode;
the bridge ignores any registered Home Assistant realtime-tool snapshot for
that session and creates one tool-free voice thread. With qualified device AEC,
this is the full-duplex/barge-in route. “Okay Nabu” remains the Home Assistant
Assist and home-control route. Older strict-v2 clients that omit
`conversation_mode` retain the prior automatic native/managed behavior for
wire compatibility.
External Wyoming service templates and smoke scripts are repository assets,
not part of the component-only HACS ZIP.

## Status

- Milestone 1: a standard Home Assistant pipeline using native Wyoming local
  STT and TTS around the Codex Voice Conversation entity. The recommended
  Mexican Spanish voice is Piper `es_MX-ald-medium`.
- New installations do not create the experimental Codex STT subentry by
  default. Existing subentries remain available for explicit diagnostics.
- The retained experimental Codex TTS entity progressively delivers realtime
  speech frames instead of waiting for the entire rendered response and remote
  cleanup, but it is not the recommended production TTS stage.
- Newly created Conversation profiles default to low reasoning effort and App
  Server's configurable `priority` tier; upgraded profiles preserve standard
  usage until reconfigured, and `standard` remains available when lower
  subscription usage matters more than latency.
- Automatic experimental STT-to-TTS session reuse is disabled: current
  realtime v3 sessions can begin assistant output before finite transcription
  completes.
- Milestone 2: an experimental ThirdReality v1.1.7 realtime client and strict
  binary wire protocol. “Okay Computer” explicitly starts native, tool-free,
  single-thread subscription voice.
  “Okay Nabu” keeps the official Home Assistant Assist flow. The client runs
  inside the existing Python voice process and requires no firmware flash or
  second device daemon. It remains an untrusted audio/control endpoint.
- Home Assistant tool authority remains available to the official Conversation
  flow and to older strict-v2 clients that omit `conversation_mode`. It is not
  attached to current “Okay Computer” sessions. The route-scoped device token
  cannot open the broker, and no tool schema, call, result, or Home Assistant
  credential is exposed on wire v2.
- Okay Computer is the native tool-free route; qualified installations enable
  full duplex for continuous microphone capture and barge-in. The safe package
  default remains turn-taking until the installation passes the reviewed
  static PulseAudio `module-echo-cancel` requirements with one explicit,
  allowlisted AEC engine, exact capture/playback routing, an explicit startup
  sink value aligned with the vendor media-player preference, a startup safety
  preflight, a sink-volume ceiling recheck before every response, and a fixed
  `paplay` stream volume with a safe 25% default and explicit 60% hard maximum.
  WebRTC remains the configuration and
  installer default; neither layer automatically falls back to another engine.
  The stock ThirdReality v1.1.7 module rejects WebRTC and Speex but loads Adrian,
  which passed the bounded reference-device echo and barge-in canaries at 25%.
  Every new installation—and every increase above a previously qualified
  level—still requires the documented physical qualification.
  The current native client flushes local playback immediately on qualified
  barge-in and requires correlated provider cancellation before same-socket
  resume; otherwise it safely starts a fresh session.
- Target Home Assistant version: 2026.8.0 or newer.
- Target Codex CLI version: 0.147.0 or newer.

The recommended Assist pipeline remains turn-based. Direct realtime is a
separate full-duplex-capable streaming PCM transport, not an STT/text/TTS
simulation. Home control intentionally stays on “Okay Nabu.”

## Quick start

### 1. Authenticate Codex on the bridge host

Install the official Codex CLI, then complete its normal login:

```bash
codex login
codex --version
```

The bridge asks Codex App Server to use that managed session. Do not copy
`auth.json` into Home Assistant and do not paste its contents into this
integration. It auto-detects `${CODEX_HOME}/auth.json` and then
`${HOME}/.codex/auth.json`. If the login file is elsewhere, point to the
existing file explicitly when starting the bridge:

```bash
export HA_CODEX_AUTH_FILE="/path/to/auth.json"
```

The bridge fails clearly when it cannot find a secure, file-backed login. It
does not support silently substituting an OpenAI API key or importing a
keyring-only login.

### 2. Run the bridge

Create separate long random tokens for Home Assistant and the realtime device,
and keep both private. From this repository:

```bash
export HA_CODEX_BRIDGE_TOKEN="replace-with-a-long-random-value"
export HA_CODEX_REALTIME_DEVICE_TOKEN="replace-with-another-random-value"
export HA_CODEX_BRIDGE_HOST="0.0.0.0"
uv run --extra bridge python -m bridge
```

The default port is `8787`. Binding beyond loopback is appropriate only on a
trusted LAN with firewall rules limiting access to Home Assistant and intended
realtime endpoints. Put the bridge behind TLS when the path is not fully
trusted.

### 3. Add reliable local STT

Install the official Whisper app on Home Assistant OS, or run the official
`wyoming-faster-whisper` server on a trusted host. Add it through Home
Assistant's **Wyoming Protocol** integration. See [reliable local
speech-to-text](docs/local-stt.md) for the tested external service and systemd
configuration.

### 4. Add reliable local TTS

Install the official Piper add-on (shown as an app in current Home Assistant
UI) on Home Assistant OS, or run the pinned `wyoming-piper==2.3.1` service on a
trusted external host. Add it through Home Assistant's native **Wyoming
Protocol** integration and select
`es_MX-ald-medium` for Mexican Spanish. The external service listens on
`tcp://HOST:10200` after its bind address is configured. Its supplied installer
pins and verifies that voice model, and the hardened service exposes the model
directory read-only.

On some virtualized x86-64 Home Assistant OS installations, the current
official Piper add-on may require the guest CPU model to expose x86-64-v2
instructions. This is specific to affected x86-64 virtualization setups, not a
general Piper limitation. Expose an appropriate CPU model when practical, or
use the supported external Wyoming service path. See [reliable local
text-to-speech](docs/local-tts.md).

### 5. Install through HACS

Until the repository is accepted into HACS defaults:

1. Open HACS and choose **Custom repositories**.
2. Add `https://github.com/AurelioB/ha-codex-voice` as an **Integration**.
3. Download **Codex Voice** and restart Home Assistant.
4. Add the Codex Voice integration.
5. Enter the bridge URL, such as `http://192.0.2.10:8787`, and the separate
   bridge token.

The config flow creates Conversation and subscription-backed TTS subentries.
In the recommended Assist pipeline, select the Wyoming faster-whisper entity
for STT, Codex Voice for Conversation, and the Wyoming Piper entity for TTS.
The Codex TTS entity remains available for explicit experimental use and
comparison.

For Mexican Spanish, set the pipeline and Conversation languages to `es-MX`,
select `es` for Wyoming faster-whisper STT, set Piper TTS language to `es_MX`,
and select voice `es_MX-ald-medium`. The bridge treats the pipeline locale as
trusted response-language context. The component leaves Home Assistant's
global interface language unchanged.

Conversation profiles also expose **Provide Home Assistant tools to realtime
voice** and **Realtime voice language**. Tool authority is off by default and
may be enabled on exactly one Conversation subentry. Its realtime language
defaults to `es-MX`. Only the Home Assistant LLM APIs selected on that subentry
are registered; enabling it does not give the speaker a Home Assistant token.
This authority supports the compatibility auto/managed wire route. It does not
override `conversation_mode: "native"` from the current ThirdReality client.

For the lowest latency on ordinary device-control commands, enable **Prefer
handling commands locally** on that pipeline. Home Assistant will handle
matching built-in intents locally and retain Codex Voice as the fallback for
open-ended conversation.

### 6. Optional ThirdReality realtime mode

Release assets include `thirdreality-realtime.zip` for the pinned Python-based
ThirdReality v1.1.7 client. It contains the guarded `sitecustomize.py`, the
stdlib-only `realtime_client` package, and a secret-free configuration example.
Follow the [device deployment, verification, and rollback
contract](device/thirdreality/README.md). `full_duplex` remains `false` by
default. Enabling it is fail-closed unless the reviewed static PulseAudio AEC
topology uses the exact configured allowlisted engine, exact source/sink
routing, current-process capture route, and configured 1–60% sink ceiling.
WebRTC is the default when `pulse_aec_method` or installer `--aec-method` is
omitted, with no automatic fallback. The observed stock v1.1.7 image instead
requires an explicit `adrian` selection; its WebRTC and Speex engines are not
compiled in and are rejected. The sink ceiling is checked again before every
response, and full-duplex `paplay` is pinned to that AEC sink with a fixed
stream volume no greater than the configured ceiling. The guarded installer
writes the chosen 1–60% sink value into the static PulseAudio block after the
AEC sink is created. The stock voice process later reapplies its persistent
Home Assistant media-player preference, so that setting must match; deferred
PulseAudio restore state alone is not reboot evidence. Adrian creating the
expected 16 kHz mono endpoints is only a topology canary. The reference device
passed a bounded 25% echo-residual and staged double-talk canary. The sink and
stream controls default to 25% and permit an explicit maximum of 60%; physical
echo and early/middle/late double-talk tests must pass at the configured
operational values on each installation before use or after an increase.

The deployment adds “Okay Computer” alongside “Okay Nabu”; it does not replace
the standard Assist path. The device configuration is root-owned and mode 0600,
and stores only the route-scoped realtime bearer—not the Home Assistant token
or the Codex OAuth credential. Deployment and rollback must preserve and verify
TCP ADB port 5555 on devices where it is the approved recovery path.

The reference client hardcodes `conversation_mode: "native"` in every strict-v2
start and requires the bridge to echo it in `started`. It is deliberately not a
device setting: Okay Computer cannot silently become a transcript/executor/TTS
pipeline because a Home Assistant broker happens to be connected.

That route-scoped token authorizes only a successfully negotiated v2 audio
session. It cannot connect to the Home Assistant tool broker. A broker may
still be maintained for compatibility clients, but the bridge ignores its
snapshot for an explicit native session.

Direct sessions can optionally select a realtime voice and a bounded session
prompt. The shipped disabled example uses `cove` and explicitly keeps Mexican
Spanish response language separate from a stable, natural Mexican accent;
omitting these keys preserves the provider defaults.

To preserve speech that reaches the microphone just before delayed local wake
activation, the overlay keeps at most the newest six 64 ms recorder frames in
RAM: 384 ms, or 12 KiB of 16 kHz mono PCM16. Only an “Okay Computer” wake can
consume that pre-roll. “Okay Nabu” discards it before starting official Assist,
and stop, mute, disconnect, and teardown paths clear it without forwarding it.

## Home Assistant controls

When the Home Assistant Assist LLM API is enabled for the Conversation
subentry, the model receives only tools selected by Home Assistant. Tool calls
are returned to the integration, executed inside Home Assistant, and sent back
to the same Codex turn. The bridge does not need a Home Assistant long-lived
access token.

The current Okay Computer route does not use this authority. It explicitly
requests one native, tool-free WebRTC voice thread, while Okay Nabu uses the
official Assist flow for Home Assistant controls.

For compatibility with older strict-v2 clients that omit `conversation_mode`,
direct realtime can use the same authority only after a second, explicit
opt-in on exactly one Conversation subentry. Home Assistant captures that subentry's
rendered instructions, `es-MX`-by-default realtime locale, and selected LLM API
tool schemas, then registers a bounded generation over
`/v1/home-assistant/tools` with the primary bridge token. Each legacy-auto
session captures one immutable generation. With strict wire v2 and App Server
v3, that snapshot activates a two-thread path: a tool-free realtime frontend
emits an
identified raw v3 user turn, and a separate executor thread alone owns the
selected tools. The bridge forwards only allowlisted, size-bounded executor
calls to Home Assistant and fails closed if the authority changes, disconnects,
times out, or returns an invalid result. A tool request from the frontend,
another thread, a stale turn, or a turn already tombstoned by interruption is
rejected with `do_not_retry` and never reaches Home Assistant. The device token
cannot use the broker, and the device wire remains audio/control only.

After the executor turn completes, the bridge selects its final answer and
sends a single UTF-8 prefix of at most 500 bytes to the frontend with
`thread/realtime/appendSpeech`. Frontend PCM is dropped until
`session.context.appended` acknowledges that append and a new, session-unique
assistant `turn.created` identifies and arms the render for the current bridge
generation. The first authorized non-silent PCM begins its speaking epoch, and
only the matching `turn.done` releases the one-render slot.
Unsolicited, direct, replayed, or stale frontend audio therefore cannot reach
the speaker. A new utterance invalidates the previous generation immediately;
the bridge requests provider cancellation only after an identified assistant
render has actually started, never against an idle or merely pending session.
The local generation gate, not cancellation confirmation, is authoritative.

Barge-in before an executor tool dispatch tombstones and interrupts that turn;
a late tool request is rejected. Raw user and assistant turn IDs remain claimed
for the socket lifetime, so provider replay cannot execute or render twice.
Once a Home Assistant call has been dispatched, the bridge does not cancel or
replay the ambiguous side effect. It lets the old executor turn finish,
discards its stale speech result, and starts the newest queued request
afterward.

Each executor turn is bounded by the configured bridge request timeout. A
failed terminal or missing completion produces a generic device error instead
of leaving the microphone session apparently stuck. On socket teardown, the
bridge tombstones and interrupts any active executor turn while its event
consumer is still present, then deletes both owned threads through tracked,
cancellation-shielded cleanup.

The broker uses Home Assistant's supported `conversation` assistant exposure
namespace, so its entity-dependent tools match the entities exposed to the
official Conversation flow. Tool execution and every transport layer have
nested deadlines. Unknown outcomes are explicitly marked `do_not_retry`, trip
a session circuit breaker, and are never retried by the bridge. If the provider
does not produce audible output or a correctly correlated terminal lifecycle
event within 20 seconds after accepting a tool result, the realtime session
terminates instead of remaining stuck.

Codex threads use a required named permission profile that exposes only minimal
runtime paths. The default bridge command also disables shell, web, plugins,
apps, MCP servers, hooks, command-environment inheritance, and interactive
approvals. The bridge fails closed if that profile is missing or inactive.
It also audits App Server's effective configuration layers at startup and
rejects any configured MCP server.
Each App Server also runs with private temporary `HOME` and `CODEX_HOME`
directories containing only a link to the managed login. Threads are persisted
only inside that isolated home, then deleted with `thread/delete` when their
bridge-managed lifetime ends. This keeps ordinary Codex history and locally
installed apps, including automatically discovered MCP sidecars, outside voice
sessions and prevents finished threads from lingering in App Server's idle
cache. An authority-enabled strict-v2/v3 session owns both its speech frontend
and executor thread; teardown deletes both independently.

No OAuth secret is copied into Home Assistant or this repository. The isolated
home contains a link to the source credential so refreshes remain owned by the
Codex CLI on the bridge host.

Outbound Conversation start and tool-result events use Home Assistant's
canonical JSON serializer. Nested `date`, `time`, and `datetime` values from
speech slots or tool results therefore cross the bridge in their ISO forms;
unsupported values fail before transmission with a data-safe protocol error.

Running the bridge as a dedicated OS user or container remains recommended as
defense in depth.

## Realtime protocol

`ws://BRIDGE/v1/realtime` supports the legacy JSON/base64 v1 protocol and a
strict device-facing v2 protocol. The shipped ThirdReality client negotiates
v2 with `conversation_mode: "native"`, streams binary 16 kHz mono PCM16 input,
and receives binary 24 kHz mono PCM16 output between explicit, monotonic
speaking-epoch controls. The `started` acknowledgement must echo native mode.
The bridge ignores any Home Assistant broker snapshot and connects microphone
and speaker audio to one native App Server WebRTC voice thread. There is no
completed-transcript gate, executor thread, or `appendSpeech` render step. V2
exposes no transcripts, raw provider events, tool calls, or tool results.

Omitting `conversation_mode` preserves the previous automatic route for older
strict-v2 clients: App Server v3 plus a captured broker snapshot selects the
isolated managed executor; otherwise it selects native. The current reference
client never omits the field.

The client keeps startup and fallback audio in bounded 64 KiB queues (2.048 s
at its input format). After a cold handshake, it transfers a queued backlog at
no more than 2× capture rate until it catches up, then resumes realtime pacing.
Direct-wake pre-roll is counted inside both bounds and is trimmed or omitted as
needed to preserve at least 32 KiB (1.024 s) for live post-wake PCM. The default
64 KiB queues retain the full 12 KiB pre-roll and more than that minimum
headroom.

The bridge separately caps v2 live WebRTC input at 2,250 ms without reducing
the finite STT adapter's whole-utterance capacity. These bounds prevent an
unlimited stale backlog; they do not eliminate the cold handshake or provider
response latency. Device playback buffering is limited to 48 KiB, about 1.024
s at the output format.

The bridge advertises `local_flush: true`, `remote_cancel: false`, and
`same_session_interrupt_ack: true`. The first two fields still forbid assuming
that a local flush truncated provider output. In qualified full duplex, two
consecutive AEC-filtered speech frames can stop local playback before the
provider VAD boundary; the provider event independently reinforces that flush.
The current client identifies itself with
`User-Agent: ha-codex-voice-thirdreality/2`. In a legacy broker-managed session, an
interrupt invalidates the bridge-owned executor/output generation and returns
`fresh_session_required: false`, `remote_cancelled: false`, and
`continuation_safe: true`; this does not claim provider cancellation. A legacy
client receives the established fresh-session fallback and the socket closes.
In the current native path, same-socket continuation requires a
`response.cancelled` event correlated to the exact active response. See the
complete [v2 wire contract](protocol/realtime-wire-v2.md).

The retained experimental Codex TTS entity uses the authenticated
`POST /v1/synthesize/stream` route. It sends an EOF-terminated PCM16 WAV stream
through Home Assistant's TTS proxy as audio arrives; the finite
`POST /v1/synthesize` route remains available for older clients and diagnostics.
The component negotiates mono 16-bit WAV at 16 or 24 kHz and the bridge emits
the requested native rate, avoiding an extra bridge-side format mismatch.

The experimental Codex STT entity opens the authenticated
`/v1/transcribe/stream` WebSocket before consuming the microphone stream. It is
kept for protocol diagnostics and compatibility, not selected by the reliable
pipeline. Codex thread and WebRTC setup begin after the validated start message
while Home Assistant continues capture. Even after successful media delivery,
the conversational service is not guaranteed to emit a user transcript.

The bridge converts that narrow WebSocket protocol to a genuine WebRTC peer:
an active audio track carries media and the `oai-events` data channel carries
control events. It sends the offer to Codex App Server and applies the returned
answer. Historical subscription-authentication validation used realtime v3 on
Codex 0.146.0. The supported bridge now requires Codex 0.147.0 or newer for the
managed frontend controls. App Server's raw realtime WebSocket route is
deliberately not used because it required API-key authentication in the
historically tested release.

This subscription-backed App Server realtime surface is experimental and is
not the [documented OpenAI Realtime API WebRTC
interface](https://developers.openai.com/api/docs/guides/realtime-webrtc).
Native mode removes the artificial
transcript/executor/render sequence, but it cannot eliminate cold Codex thread
creation, WebRTC negotiation, network, service admission, or provider response
latency. A first wake may therefore still be slower than an already-open
ChatGPT voice session.

## Performance

On the measured i5-13600K host, a warm multilingual faster-whisper `base`
service transcribed the same non-sensitive reference WAV three times in 0.775,
0.617, and 0.599 seconds. The prior subscription-backed adapter timed out after
roughly 19 seconds in an observed physical run and also intermittently missed
clean input. These are small diagnostic samples, not latency guarantees.

On that host, the repository smoke probe measured Piper
`es_MX-ald-medium` to the first non-empty Wyoming PCM chunk. Across two service
restarts, cold first PCM ranged from 0.714 to 0.956 seconds and complete
synthesis from 0.824 to 1.072 seconds. Five warm requests reached first PCM in
0.025, 0.024, 0.044, 0.028, and 0.035 seconds: a 0.028-second median and
0.044-second maximum. Their median complete synthesis time was 0.116 seconds
for 2.949 to 3.367 seconds of audio. Three controlled Codex TTS requests for
the same text had a
2.025-second median to first audio (1.671 to 2.898 seconds), about 72 times
Piper's warm median at this provider boundary.

A separate physical Home Assistant call changed the ThirdReality
`media_player` state to playing at 0.018097 seconds and back to idle at
3.564543 seconds, but actual audible onset was not instrumented. These small,
host-side and state-boundary samples are not latency guarantees.

A subsequent controlled self-acoustic Spanish canary traversed the physical
speaker and microphone, wake detection, local STT, Codex Conversation, Piper,
and response playback without errors. STT ended 6.590 seconds after pipeline
start, Codex Conversation took 1.734 seconds, the satellite entered responding
at 8.324 seconds, and it returned to idle at 13.919 seconds. The request was
recognized with the intended words and the response was non-empty `es-MX`.
These state boundaries still do not measure first audible sound.

A later physical ThirdReality canary completed local recognition 0.497 seconds
after VAD ended and reached its Conversation/TTS result 7.703 seconds after
pipeline start, including capture. The answer played and the satellite returned
to idle.

Streaming Codex TTS exposed first PCM 4.222 seconds before the equivalent
finite bridge response in one earlier probe. That historical comparison is
between two Codex adapter modes, not between Codex and Piper.

A controlled acoustic A/B on the same ThirdReality device had reduced the old
adapter's STT completion from 9.519 to 6.138 seconds, but that tuning could not
remove the backend's missing-transcript failure mode. It is retained only as
historical diagnostic evidence.

Automatic STT-to-TTS session reuse is deliberately disabled in the bundled
component. Live v3 validation observed genuine assistant output before the
user transcript completed, and the tagged Frameless Bidi client protocol has
no supported response-cancel message. Both the component and bridge therefore
disable ticket issuance; a handoff-shaped diagnostic request is validated but
takes the isolated cold path. When both experimental Codex speech entities are
selected, TTS starts in a fresh thread and session. The recommended local Piper
stage does not use this remote handoff mechanism.

Always-on remote prewarming is not enabled: there is no provider hook before a
wake word, an idle WebRTC peer still sends silent RTP, App Server does not
document idle sessions as quota-neutral, and speculative sessions would occupy
the single speech lane. See [performance and ThirdReality
tuning](docs/performance.md) for measurement scope, handoff privacy and
fallback behavior, safe device settings, and the firmware canary decision.

## Development

See the [development workflow](docs/development.md) for the focused pytest,
disposable local Home Assistant, and staged production SSH deployment loops.

```bash
uv sync --extra test --extra lint
uv run ruff check .
uv run ruff format --check .
uv run pytest tests/component
uv run pytest -p no:homeassistant tests/bridge
```

The integration follows current Home Assistant entity and config-subentry
patterns. CI runs Ruff, tests, hassfest, and HACS validation. Live tests are
opt-in and must never print OAuth tokens or recorded audio.

## Removal

1. Remove the Codex Voice integration from Home Assistant under **Settings →
   Devices & services**.
2. Remove Codex Voice from HACS and restart Home Assistant.
3. Stop and disable the separately running bridge process if no other client
   uses it, then delete its dedicated bridge token.
4. If they were installed only for this pipeline, remove the Wyoming
   integrations and stop the faster-whisper and Piper services separately.
5. If direct ThirdReality mode was installed, follow the device rollback
   contract, remove the route-scoped token from the bridge, and verify the
   approved TCP ADB port 5555 recovery path before and after the voice restart.

## Known limitations

- Subscription-backed audio depends on an experimental Codex realtime
  conversation feature and consumes ChatGPT subscription availability, not
  OpenAI Platform API quota. It is not the stable OpenAI Audio or Realtime API,
  and Codex CLI/App Server upgrades may change it.
- The reliable pipeline's STT is local faster-whisper, not OpenAI-hosted and
  does not consume ChatGPT quota. ChatGPT subscription OAuth does not expose a
  supported standalone transcription endpoint. The retained Codex STT adapter
  can connect successfully without producing a transcript.
- Recommended Assist TTS is local Piper over Wyoming and does not use the
  ChatGPT subscription speech lane. The retained Codex TTS entity is a bounded
  realtime-conversation compatibility session, not OpenAI's separately billed
  `/v1/audio/*` endpoint. Its live voice model may paraphrase or expand text
  instead of reading it verbatim. Do not use that experimental entity for
  safety-critical or legally exact announcements.
- The Codex subscription realtime surface is admitted one speech session at a
  time. Overlapping experimental Codex STT, TTS, or realtime requests fail
  immediately as busy so a caller can retry, rather than occupying the bridge
  until a timeout. Wyoming STT does not use that lane.
- HACS cannot install the bridge process. Run it separately or use the future
  add-on/container packaging.
- The pinned ThirdReality v1.1.7 overlay defaults to direct realtime
  turn-taking. Full duplex is an explicit post-qualification option that
  requires a repository-reviewed PulseAudio AEC fragment, an exact configured
  engine match, exact process routing, a preflight plus per-response sink
  ceiling, and a fixed `paplay` stream cap with a 25% default and 60% hard
  maximum. WebRTC is the
  fail-closed default and is never automatically replaced. Stock v1.1.7 rejects
  WebRTC and Speex and must explicitly select Adrian. The reference device
  passed bounded echo-residual and staged barge-in canaries at 25%, but a
  syntactically valid Adrian topology is not proof for another installation;
  do not enable it there before physical double-talk and rollback canaries pass
  at the configured sink and stream values. Values above the previously
  qualified 25% reference require new physical evidence, up to the 60% maximum.
- “Okay Computer” remains an untrusted, native, tool-free audio/control client
  and cannot control Home Assistant. Use “Okay Nabu” for exposed Home Assistant
  entities and tools. The separately registered realtime authority exists only
  for older strict-v2 clients that omit `conversation_mode`.
- ThirdReality firmware 1.01.07 normally withholds microphone audio until its
  wake confirmation sound finishes, then may block the microphone thread for
  up to two seconds while updating the LED. The optional pinned overlay uses an
  LED-only acknowledgement, makes microphone forwarding effective as soon as
  the local Assist request and music-duck calls complete, and dispatches
  serialized LED updates off the microphone thread. Applying the overlay,
  selecting aggressive finished-speaking detection, or raising hardware
  microphone gain are
  device-side changes with explicit accuracy, compatibility, and clipping
  acceptance checks.
- The official ThirdReality v1.2 firmware is a substantial Python-to-C++
  rewrite, but the target has one boot/system/recovery set rather than A/B
  slots. Do not flash the sole production speaker. Test only on a spare after
  capturing the actual partitions, data, boot environment, and device security
  state, then physically rehearsing a full-image downgrade and restoration.
  v1.2.1 also enables unauthenticated root ADB over TCP port 5555 and ships
  password-authenticated root SSH with a documented default. Isolate and
  harden both services, then verify the ports after reboot and updates. The
  tagged updater disables TLS peer and hostname verification, and a locally
  calculated SHA-256 identifies bytes but does not authenticate their
  publisher. No production flash is allowed without independently
  authenticated provenance.

## License

MIT. See [LICENSE](LICENSE).
