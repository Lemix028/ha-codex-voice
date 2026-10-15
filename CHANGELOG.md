# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and releases use semantic versioning.

## [Unreleased]

## [0.8.0] - 2026-10-12

### Added

- Single-turn-first for new profiles: deterministic follow-up/question/explicit
  continuation selection, bounded RAM context and one fresh App Server turn.
- Local light shortcuts through existing HA intents: "Computer, Licht" toggles
  exposed room lights; on/off and safe native commands acknowledge with "Okay."
  Ambiguous targets ask locally; errors remain final and are never retried.
- Separate context, local intent, tool, model/bridge-wait and total timing fields,
  with content-free diagnostics and observable App Server turn counts.
- Default-off curated/export knowledge repository and separate web capability
  policy; neither is registered as a tool or granted account/network access.
- Synthetic regression coverage for native HA ChatLogs, recognizer, room scope,
  privacy, authority, topic changes, replay and fresh-thread cleanup.

### Fixed

- Dispose fresh threads even if the started handshake is cancelled, and during
  shutdown; reject unregistered, repeated or excess dynamic tool calls.
- Enforce actual text lengths and integer budgets; reject incompatible knowledge
  schemas without silently recreating versioned tables.
- Prevent a new light command or statement from answering a stale entity question;
  honor disabled context caching and clear rendered home context on new topics.

### Compatibility

- Profiles without `context_mode` preserve v0.7.0 behavior. Adaptive profiles
  require a matching bridge and never persist session metadata.
- No firmware or live deployment changes. Green Echo-ring feedback remains
  optional and unsupported by the inspected hassmic RGB/control surface.

## [0.7.0] - 2026-10-10

### Added

- Computer Assist fork: per-device and known-user sessions, configurable 2–60
  minute idle lifetime, bounded queues and RAM continuity context, explicit reset.
- Optional HA Store metadata checkpoints, truthful lost/partial context notice,
  periodic TTL cleanup and authenticated bridge reset endpoint.
- Content-free Conversation diagnostic ring and read-only HA pipeline trace exporter.
- German/English session settings, rollout/rollback/privacy documentation and
  a concrete Tools/Memory/STT follow-up design with 30 synthetic German sentences.

### Fixed

- Reject duplicate Conversation request IDs, duplicate tool IDs and failed
  terminal responses; cap HA tool execution and never automatically retry an
  uncertain side effect. Retire cancelled/failed session bindings.
- Preserve the byte-verified Piper lock asset's LF endings on Windows checkouts.

### Compatibility and privacy

- Keep the HA Assist/Whisper/Piper path and HA tool authority. Existing profiles
  retain ID-only behavior until continuity is enabled. No ConfigEntry migration.
- No long-term memory, added transcript/audio store, firmware change
  or production deployment. Hardware acceptance gates remain open; see report.

## [0.6.1] - 2026-10-10

### Fixed

- Convert Home Assistant 2026.10 Probatio tool schemas with the matching codec;
  preserve legacy Voluptuous support and declare its dependency explicitly.
- Normalize supported HA values with content-free path/type diagnostics and
  strict rejection of unsupported values; retain typed ToolResult error flags.
- Add normal Assist JSON regression tests and HA 2026.10 compatibility CI.

## [0.6.0] - 2026-08-10

### Added

- Add an explicit `conversation_mode: "native"` device-wire selection for a
  single, tool-free Codex realtime speech session with provider VAD and
  interruption.

### Changed

- Make the ThirdReality realtime client always request native conversation and
  require the bridge to echo that selection before microphone audio can flow.
- Keep the Home Assistant tool broker available for the standard Assist route
  while ignoring it for explicitly native device conversations.

### Fixed

- Prevent an attached Home Assistant tool authority from silently converting
  ThirdReality native voice into transcript, executor turn, and
  `appendSpeech` stages.
- Reject the legacy device `speech` control in explicit native mode so no
  synthetic `appendSpeech` operation can enter that route.

## [0.5.4] - 2026-08-10

### Added

- Add a gateway-only development bridge path on port 18787 so the disposable
  Home Assistant container can exercise the checkout end to end without HACS,
  GitHub, or LAN exposure.
- Add authenticated local Core lifecycle verification with a dedicated
  development token while retaining an honest frontend-only onboarding mode.

### Changed

- Write the qualified ThirdReality AEC sink volume into the static PulseAudio
  block and require it to match the later vendor media-player preference.
- Recognize released method-only AEC blocks for explicit remove-and-reinstall
  migration while keeping method and volume changes fail-closed.

### Fixed

- Wait for Home Assistant `/api/config` to report `RUNNING` after production
  deployment restarts instead of accepting an early API response.
- Treat a restart POST disconnect as ambiguous and require an observed
  unavailable/non-running transition before accepting recovery.

## [0.5.3] - 2026-08-10

### Added

- Add a fast, localhost-only Docker workflow for checking and restarting the
  custom component against the pinned Home Assistant release.
- Add a hardened SSH deployment helper with bounded packaging, checksum-bound
  staging, rollback on installation failure, and a recoverable previous backup.

### Changed

- Make the ThirdReality realtime AEC sink ceiling and playback stream volume
  independently configurable, retaining safe 25% defaults with an explicit
  hard maximum of 60%.

## [0.5.2] - 2026-08-10

### Added

- Add a broker-managed realtime architecture for strict device wire v2 on
  Codex realtime v3. A tool-free speech frontend produces one canonical user
  transcript, a separate Home Assistant-aware executor owns tool calls, and
  only its completed final answer is explicitly rendered through
  `appendSpeech`.
- Add content-free, deduplicated provider event-shape tracing and absolute
  handshake/output smoke deadlines that cannot be extended by heartbeats.

### Changed

- Require Codex CLI 0.147.0 or newer for the isolated realtime path, disable
  delegation acknowledgement filler and irrelevant repository startup context,
  use client-managed handoffs, and gate output on both the explicit
  context-append acknowledgement and identified v3 assistant turn for the
  current bridge generation.
- Correlate microphone requests from the identified raw v3 user-turn lifecycle
  instead of identity-free transcript notifications. Limit each managed
  `appendSpeech` to one 500-byte UTF-8 context frame and serialize one render at
  a time through its matching `turn.done` boundary.
- Negotiate bridge-managed same-socket interruption with the updated
  ThirdReality client User-Agent. Older clients retain the established
  fresh-session fallback; the new client accepts the explicit
  `continuation_safe` acknowledgement without claiming provider cancellation.

### Fixed

- Reject frontend, foreign, stale, and post-interrupt tool calls without
  executing Home Assistant actions. Buffer bounded executor events that beat
  `turn/start`, answer abandoned early tool requests exactly once, and suppress
  stale finals and post-tool watchdogs across barge-in generations.
- Drop unsolicited frontend PCM until an authorized executor final is appended,
  and preserve the active tool-bearing turn through barge-in so an ambiguous
  Home Assistant side effect is never cancelled or retried.
- Avoid invalid `response.cancel` requests against idle or merely pending
  frontend sessions; cancellation is attempted only after an identified
  assistant render has actually started.
- Tombstone user and assistant turn IDs for the session, reject replayed or
  contradictory lifecycle events, and recheck output ownership while holding
  the device send lock so stale PCM cannot cross a barge-in boundary.
- Bound WebSocket shutdown and make bridge service restarts release active
  realtime sockets promptly. Track and shield the two-thread provider cleanup,
  interrupt active executor turns before deletion, and fail closed with a
  generic device error when an executor fails or misses its completion
  deadline. Executor timeout and socket teardown now also tombstone queued work
  and synchronize with an in-flight `turn/start` before thread disposal.

## [0.5.1] - 2026-08-10

### Fixed

- Defer the realtime Home Assistant tool snapshot until
  `EVENT_HOMEASSISTANT_STARTED` through Home Assistant's supported
  `async_at_started` lifecycle helper. A full boot can no longer capture the
  Assist LLM API before its lazy tool providers finish registering; unloading
  during startup cancels the pending one-shot listener, while runtime reloads
  still start immediately.

## [0.5.0] - 2026-08-09

### Fixed

- Continuously consume unwanted realtime model audio during finite speech
  transcription. This prevents the hardened WebRTC receive queue from
  overflowing before the user transcript arrives, while retaining the queue's
  fail-closed memory bound.
- Preserve retained speech-session safety while draining transcription audio:
  any assistant audio invalidates session reuse without discarding an otherwise
  valid STT result, and untracked handoff output still fails closed.
- Accept the pinned ThirdReality `default.pa` with or without a final newline in
  the guarded static AEC installer while preserving exact rollback text.
- Flush full-duplex playback from two consecutive AEC-filtered speech frames
  instead of waiting only for remote VAD. The local boundary keeps microphone
  audio flowing on the same socket while provider lifecycle events remain the
  authority for remote cancellation and safe session reuse.
- Scope each local barge-in request to the exact output epoch, reset detector
  state at every stop, interrupt, resume, and new response boundary, and make
  microphone admission atomic with interrupt/stop queue clearing. A stale
  request or pre-interrupt frame can no longer cross into a resumed response.
- Make terminal WebRTC transport failure outrank cancellation-replay queues so
  neither stale PCM nor data-channel events can be delivered after failure.

### Added

- Log only the exception class for unexpected streaming-transcription failures,
  keeping wire responses and diagnostics free of request content.
- Ship separately reviewable WebRTC, Speex, and Adrian PulseAudio
  `module-echo-cancel` fragments in the ThirdReality release asset.

### Changed

- Allow full-duplex deployments to explicitly select only `webrtc`, `speex`,
  or `adrian`, and require the installed module to match that selection exactly.
  WebRTC remains the omitted-value default and never falls back automatically.
- Use explicit Adrian installer and client configuration in active stock
  ThirdReality v1.1.7 examples. Hardware probing found that image rejects its
  uncompiled WebRTC and Speex engines, while Adrian loads with the exact pinned
  masters and `use_master_format=1` and creates 16 kHz mono endpoints. On the
  qualified reference device at 25%, a 5.531-second playback canary produced no
  false interruption across 86 live microphone frames (maximum peak 2 and
  integer RMS 0), and a staged double-talk canary flushed playback in 141 ms
  while keeping the same session. Each installed device must still pass the
  documented rollback, echo-rejection, and early/middle/late double-talk checks
  at no more than 25%.

## [0.4.1] - 2026-08-09

### Fixed

- Use Home Assistant's supported `conversation` exposure namespace for the
  realtime Assist LLM context. Entity-dependent tools such as
  `HassListAddItem` are now registered for the same exposed entities as the
  official Conversation flow instead of silently receiving an empty exposure
  set under the integration domain.
- Apply nested deadlines to the complete realtime tool transaction: 25 seconds
  for Home Assistant execution, 5 seconds for component result delivery, 35
  seconds for the bridge request/write/result exchange, 5 seconds for provider
  result delivery, and a 45-second App Server backstop. WebSocket send-lock and
  write stalls can no longer bypass the broker deadline.
- Preserve App Server fallback ownership until a tool response is actually
  written, retain `outcome_unknown` / `do_not_retry` semantics across the
  provider boundary, and trip a session circuit breaker after ambiguous
  authority failures so fresh provider IDs cannot amplify retries.
- Require audible provider output or a correctly correlated terminal lifecycle
  event within 20 seconds after a tool result is delivered. A wedged post-tool
  continuation now terminates with a bounded error instead of pinning the
  realtime socket indefinitely.

### Added

- Report content-free realtime tool readiness and transaction counters from
  authenticated bridge health, and log short one-way correlation labels for
  provider call, Home Assistant result, and provider delivery stages without
  recording tool names, arguments, results, or conversation content.

## [0.4.0] - 2026-08-09

### Added

- Add an outbound, Home Assistant-owned realtime tool broker. Exactly one
  explicitly opted-in Conversation subentry may register its rendered
  instructions, `es-MX`-by-default locale, and selected LLM API tool schemas;
  Home Assistant executes correlated calls without exposing broker traffic or
  credentials to the device.
- Add opt-in full duplex for the pinned ThirdReality v1.1.7 overlay, backed by
  guarded static PulseAudio `module-echo-cancel` assets using WebRTC AEC,
  exact-route startup verification, continuous capture, speech-start playback
  flush, and late-output quarantine. Turn-taking remains the default.
- Add correlation-gated same-session interruption. The bridge requests
  provider response cancellation and permits socket reuse only after a
  matching `response.cancelled` event; every ambiguous or timed-out case keeps
  the fresh-session fallback.
- Add a hardened external Wyoming Piper user-service template pinned to
  `wyoming-piper==2.3.1`, a bounded ONNX Runtime threading runner, a
  privacy-safe synthesis smoke test, and deployment guidance for
  `es_MX-ald-medium` on `tcp://HOST:10200`. Pin that voice to an immutable
  upstream revision, verify its size and SHA-256 before atomic installation and
  every service start, mount models read-only, and reject other voice downloads.

### Changed

- Keep realtime wire v2 device-facing authority limited to binary audio and
  content-free controls while allowing the bridge to bind an independently
  registered Home Assistant tool snapshot to a provider session. Device
  `tools` and `tool_result` messages remain invalid.
- Require full-duplex deployments to use explicit PulseAudio AEC source/sink
  names and a configured 1–25% qualification ceiling. Preflight verifies the
  static WebRTC-AEC topology, default routes, current-process capture route,
  and every sink channel before microphone audio leaves the device.
- Make the recommended “Okay Nabu” production pipeline local Wyoming
  faster-whisper STT, Codex Voice Conversation, and local Wyoming Piper TTS.
  Keep Codex TTS as an explicit experimental compatibility entity and preserve
  direct Codex realtime speech for “Okay Computer.”
- Document the official Piper add-on (shown as an app in current Home Assistant
  UI) as the simplest Home Assistant OS path and the external Wyoming service
  as a supported fallback for affected
  virtualized x86-64 guests that do not expose the x86-64-v2 instruction level.
  This compatibility caveat is not generalized to other deployments or
  architectures.

- Record repository smoke-probe timings to Piper's first non-empty Wyoming PCM
  chunk: across two restarts, cold first PCM from 0.714 to 0.956 seconds and
  completion from 0.824 to 1.072 seconds; five warm requests with a
  0.028-second median and 0.044-second maximum to first PCM; and 0.116-second
  median complete synthesis. Three
  controlled same-text Codex TTS requests had a 2.025-second median to first
  audio (1.671 to 2.898 seconds), about 72 times Piper's warm median at the
  provider boundary. A complete self-acoustic Spanish canary traversed the
  physical ThirdReality speaker and microphone, wake detection, local STT,
  Codex Conversation, Piper, and response playback without errors; the
  satellite entered responding at 8.324 seconds and returned idle at 13.919
  seconds from pipeline start. Actual audible onset was not instrumented.

### Fixed

- Recheck every AEC sink channel against the configured ceiling before each
  full-duplex response, and pin every `paplay` child to the reviewed AEC sink
  with a fixed linear stream volume no greater than 25%, so a later response
  cannot bypass the startup preflight. Compare raw PulseAudio volume units to
  the exact linear ceiling rather than trusting the rounded displayed percent;
  changing volume during active playback remains forbidden operator action.
- Correlate provider cancellation to the exact active response before returning
  `fresh_session_required: false` / `remote_cancelled: true`; completion,
  mismatched cancellation, send failure, and timeout cannot keep the session.
- Fail realtime tool calls closed on undeclared names, stale generations,
  duplicates, disconnects, timeout, oversized/non-JSON payloads, or ambiguous
  outcomes, while returning at most one result to each provider request.

### Security

- Keep the ThirdReality endpoint untrusted even when realtime tools are
  enabled: the route-scoped device bearer cannot open the primary-token
  `/v1/home-assistant/tools` broker, and wire v2 never exposes tool schemas,
  calls, results, transcripts, or raw provider payloads.
- Bound broker registration, tool count, schemas, arguments, results, pending
  calls, retired correlation state, and calls per provider session. Capture one
  immutable authority generation per session and fail closed if it changes.
- Require static, startup-ordered WebRTC AEC and both a startup and per-response
  volume check before full-duplex playback. A local playback flush or VAD event
  alone never claims remote cancellation.
- Keep Piper synthesis and model access local after Codex returns the
  Conversation text, bind the external service to loopback by default, and
  require operators to restrict the unauthenticated Wyoming port `10200` to
  Home Assistant when enabling LAN access.

## [0.3.0] - 2026-08-09

### Added

- Add a strict, content-private realtime wire protocol v2 and a standard-library
  ThirdReality v1.1.7 client that runs inside the existing voice process.
  “Okay Computer” selects direct subscription-backed chat voice while “Okay
  Nabu” preserves the official Home Assistant Assist and home-control flow.
- Add a release archive containing the guarded `sitecustomize.py`, realtime
  client package, secret-free disabled configuration example, and device
  deployment/rollback contract, with the repository's MIT license included in
  both standalone release archives.
- Advertise the `es-MX` Home Assistant speech locale and add optional bounded
  direct-device voice and prompt settings, with the example configured for a
  concise Mexican Spanish language and accent policy.

### Changed

- Bound device startup and fallback PCM to 64 KiB (2.048 s), transfer startup
  backlog at no more than 2× capture rate, cap v2 host input independently at
  2,250 ms, and bound device playback to 48 KiB (about 1.024 s). Finite STT
  retains its whole-utterance input capacity.
- Retain up to six idle recorder frames for Okay Computer only: a RAM-only,
  12 KiB/384 ms pre-roll that is discarded for Okay Nabu and all teardown
  paths. Trim or omit it for smaller queues so at least 32 KiB of live
  post-wake capacity remains. One physical regression canary captured and
  answered the previously failing 308 ms wake-to-command sample; it is a
  single-case validation, not a latency distribution.
- Keep the released realtime path turn-taking with microphone gating during
  output. Interruption flushes local playback and requires a fresh session with
  `remote_cancel: false`; acoustic echo cancellation, true full duplex, and
  barge-in remain future work. Device configuration rejects `full_duplex: true`
  and guarantees that every valid message-size bound can carry one fixed
  2,048-byte recorder frame.

### Fixed

- Serialize outbound Home Assistant Conversation start and tool-result events
  with Home Assistant's canonical JSON policy, preserving nested temporal
  values such as speech slots in ISO form and rejecting unsupported objects
  before transmission with a data-safe protocol error.
- Bind the route-scoped device bearer to a successfully negotiated v2 session
  before provider startup while retaining primary-token compatibility for
  legacy v1 clients.
- Release the single subscription speech lane promptly when a realtime device
  disconnects during thread or WebRTC startup, so official Home Assistant
  fallback cannot remain blocked behind an abandoned direct session.
- End speaking epochs only after terminal metadata and a bounded media-idle
  tail, and isolate pre-response audio by epoch and age so delayed WebRTC PCM is
  neither truncated nor replayed into a later response.
- Apply Home Assistant's trusted Assist pipeline locale to Codex conversation
  turns so a Mexican Spanish pipeline does not rely on implicit language
  detection alone.

### Security

- Add an optional route-scoped, v2-only realtime-device bearer, a root-owned
  mode-0600 device configuration contract, and a chat-only v2 boundary that
  exposes no transcripts, raw provider events, or tools. Device deployment
  preserves and verifies the approved TCP ADB port 5555 recovery path.

## [0.2.0] - 2026-08-09

### Added

- Add a hardened user-service template and deployment guide for the official
  Wyoming faster-whisper server, keeping the multilingual `base` model warm for
  Home Assistant's native streaming STT provider while suppressing the
  upstream INFO-level transcript log. The validated dependency stack and model
  revision are pinned, native inference is serialized, and the service can read
  only its isolated runtime directory within the user's home.
- Add a local Wyoming known-WAV smoke test that hides transcript text by default
  and an opt-in Codex WebRTC transcription probe that prints its transcript and
  consumes ChatGPT subscription quota.

### Changed

- Make the production Assist architecture use local Wyoming faster-whisper for
  a reliable finite STT boundary while retaining ChatGPT OAuth for Codex Voice
  Conversation and the experimental TTS/realtime paths. New integrations create
  Conversation and TTS subentries by default. Existing Codex STT subentries are
  preserved; users must manually select Wyoming in stored Assist pipelines, and
  the entity/setup UI describes Codex STT as an experimental diagnostic.
- Make the pinned ThirdReality v1.1.7 wake path use an LED-only acknowledgement:
  pre-arm microphone forwarding on the pinned microphone thread, queue the
  Home Assistant Assist start request, and duck music without playing the local
  confirmation cue. Dispatch serialized LED DBus calls on a bounded daemon
  worker so their two-second vendor timeout cannot stall microphone capture,
  coalescing an overloaded backlog toward the newest state. The overlay now
  requires an atomic match across four exact vendor bytecode hashes, patches the
  pinned ThirdReality subclass directly, and preserves transactional rollback
  while ensuring teardown wins startup races.

## [0.1.10] - 2026-08-09

### Changed

- Default newly created Conversation profiles to App Server's configurable
  `priority` service tier, while preserving `standard` for existing profiles
  that predate the option. Priority targets lower response latency and consumes
  more subscription availability.
- Align selectable reasoning efforts with the installed model catalog by
  adding `ultra`, removing `none`, and safely mapping legacy unsupported values
  to the latency-oriented `low` default.
- Add an opt-in guarded live-STT fragment completion path. The safe default
  remains 2 s because local WebRTC input drain is not remote recognition
  completion. A measured deployment may explicitly select 0.5–2 s; the shorter
  deadline is used only after successful input drain with a normalized,
  unity-gain live feed and no handoff. Completion diagnostics remain numeric
  and privacy-safe.
- Calibrate streaming STT incrementally with a bounded 600 ms analysis window
  and recognize speech-like quiet input without opening on digital silence,
  steady noise, or isolated clicks. This prevents quiet ThirdReality captures
  from being replayed only after EOF and removes the prior full-prefix rescan
  cost as utterances grow.
- Bound realtime-session shutdown to 5 s and private-thread disposal to one
  separate 5 s wall-clock budget. Delete and legacy unsubscribe fallback now
  share that budget, including a stalled App Server write, instead of adding a
  nominal 30 s failure tail before a retry or error can return.
- Pin a reversible, bytecode-guarded ThirdReality overlay for the tested v1.1.7
  client. It lets Home Assistant prepare the pipeline during the 0.399592 s
  wake cue without forwarding cue audio, and rejects stale callbacks after a
  run ends, disconnects, is cancelled, or is replaced. Transactional rollback
  and a bounded missing-EOF watchdog prevent mute, player, send, and cue
  failures from wedging later wakes. The global mpv-cache experiment is not
  included because its small observed difference was noisy and it also affected
  sustained music playback. Device launch guidance also disables Python bytecode
  writes so a permissive firmware umask cannot create a writable root import
  artifact.

### Fixed

- Retain constrained `appendText` synthesis for the official Home Assistant
  `tts.speak` path. A cold `appendSpeech` experiment timed out at 90.047 s with
  HTTP 504 and is not enabled.

## [0.1.9] - 2026-08-08

### Added

- Negotiate Home Assistant's native mono 16-bit WAV output at 16 or 24 kHz
  and incrementally resample realtime speech when 16 kHz is requested.
- Record privacy-safe successful-capture level and live-feed metrics for
  diagnosing microphone gain and recognition overlap without logging speech.

### Changed

- Feed bounded, one-time calibrated speech to realtime recognition while Home
  Assistant capture continues, retaining the complete raw utterance for a
  fresh normalized retry and buffering quiet or ambiguous captures until EOF.
- Default voice conversation turns to low reasoning effort to favor latency;
  the integration option remains configurable.
- Set the ThirdReality v1.2.1 production decision to no-go after confirming
  that the target is single-slot and the available files cannot provide exact
  rollback. A future spare-device canary remains conditional on authenticated
  provenance, complete device read-backs, and physically rehearsed recovery.
- Record end-to-end acoustic wake canaries at 700 ms and 300 ms command gaps,
  both completing STT, a local intent, TTS playback, and satellite recovery
  without pipeline errors.

### Fixed

- Use an explicit empty ICE-server configuration so the local
  subscription-backed WebRTC path does not wait five seconds for the default
  public STUN probe during every STT and TTS handshake.
- Bound missing-transcript retries to two attempts and each result wait to four
  seconds, reducing the penalty of failed or exceptionally quiet recognition.

## [0.1.8] - 2026-08-08

### Added

- Add observed-event boundary validation and a content-private live probe for
  the dormant STT-to-TTS handoff experiment.
- Document measured latency boundaries, ThirdReality tuning acceptance checks,
  firmware backup and rollback, and the requirement to preserve TCP ADB on the
  measured device.

### Changed

- Start the Codex thread and WebRTC handshake when streaming STT capture opens,
  overlapping remote setup with the finite Home Assistant microphone capture.
- Keep STT-to-TTS ticket issuance disabled in both the component and bridge
  after live v3 validation observed assistant output before finite
  transcription completed.

### Fixed

- Make retained-session expiry, replacement, cancellation, remote invalidation,
  component unload, and successful consumption converge on bounded, exactly-once
  session and thread cleanup.
- Invalidate dormant reuse on observed assistant audio or output at checked
  STT-to-TTS boundaries, including simultaneous receiver and claim races.
- Preserve a valid STT transcript when unexpected assistant output invalidates
  the reuse experiment, then close the unsafe session and use fresh TTS.

## [0.1.7] - 2026-08-08

### Added

- Stream mono 24 kHz PCM WAV audio from the bridge and Home Assistant TTS
  entity as realtime speech frames arrive, while retaining the finite
  synthesis endpoint for compatibility.
- Emit numeric-only, privacy-safe stage timings for disposable STT and TTS
  attempts so handshake, recognition, first-audio, tail, and cleanup latency
  can be measured without logging content or identifiers.

### Changed

- Remove redundant queued STT silence by default and finalize transcript
  fragments relative to meaningful audio rather than optional padding.
- Advertise WAV as the native TTS output while allowing Home Assistant to
  convert explicitly requested alternate formats.

## [0.1.6] - 2026-08-08

### Fixed

- Remove only confidently silent prefixes longer than two seconds before
  pacing finite STT audio over WebRTC, while retaining 320 milliseconds of
  pre-roll and preserving quiet or ambiguous early speech.
- Recompute transcription feed deadlines after trimming, preventing long
  post-command waits caused by replaying the satellite's leading silence in
  real time.

## [0.1.5] - 2026-08-08

### Fixed

- Normalize unusually quiet microphone audio with bounded, clipping-safe gain,
  allowing low-level ThirdReality captures to reach the realtime recognizer.
- Assemble realtime v3 user transcript fragments and finish finite STT input
  after the complete clip and a quiet period when no terminal event arrives.
- Treat a valid user transcript as authoritative when the remote recognizer
  stops pulling trailing silence, instead of hanging on a local input-drain
  marker that may never complete.
- Preserve transcript events while observing audio drain, recognize current v3
  handoff and delegation shapes, and reject assistant text as STT input.

## [0.1.4] - 2026-08-08

### Fixed

- Admit only one subscription-backed realtime speech session at a time and
  reject overlapping STT, TTS, or duplex requests immediately with HTTP 409,
  instead of allowing a losing WebRTC session to time out after 90 seconds.
- Retry transient WebRTC input-drain and missing-terminal-transcript stalls in
  fresh, fully cleaned sessions under one bounded transcription budget.
- Release speech-session admission after success, failure, timeout, or
  cancellation, and log privacy-safe audio level metrics when transcription
  reaches its deadline.

## [0.1.3] - 2026-08-08

### Fixed

- Run App Server in a mode-0700 temporary Codex home linked only to the managed
  file-backed ChatGPT login, keeping normal Codex history, configuration, apps,
  plugins, and MCP sidecars outside voice sessions.
- Persist threads only inside that private home and delete them when their
  bridge-managed lifetime ends, preventing finished Conversation, STT, TTS,
  and realtime threads from accumulating during App Server's idle-unload
  window.
- Audit effective configuration layers for injected MCP servers and accept
  bounded App Server JSON-RPC messages larger than asyncio's small default.

## [0.1.2] - 2026-08-08

### Fixed

- Restore the documented Python 3.11+ bridge compatibility and enforce it in
  CI with standalone bridge tests on the minimum supported Python version.

## [0.1.1] - 2026-08-08

### Fixed

- Bound conversation completion waits, with ambiguous turns interrupted and retired.
- Reject overlapping turns instead of allowing an unbounded background queue.
- Apply one deadline and bounded event backlog to realtime handshakes.
- Fail transcription promptly when app-server exits and bound accepted audio to
  60 seconds under an end-to-end request deadline.

## [0.1.0] - 2026-08-08

### Added

- Home Assistant Conversation, speech-to-text, and text-to-speech entities.
- Home Assistant LLM tool forwarding with bounded multi-turn conversation reuse.
- Local bearer-authenticated bridge using Codex-managed ChatGPT OAuth.
- Subscription-backed WebRTC v3 speech adapters and an experimental realtime
  duplex-audio WebSocket.
- HACS packaging, diagnostics, security hardening, tests, and live smoke tools.
