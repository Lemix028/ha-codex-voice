# Computer: Audit und Meilenstein A

Stand: 10.10.2026. Fork: `Lemix028/ha-codex-voice`, Ausgangsrevision
`065413f517ee9150b5bdce47cd685212db169bf9` (Upstream v0.6.0).
Branch: `codex/computer-phase-ab`. MIT-Lizenz/Attribution bleiben erhalten.

| Bereich | Ist-Stand | Folgerung |
| --- | --- | --- |
| Assist mit Wyoming/Piper | Vorhanden, HA >=2026.8.0 | Computer nutzt diesen Pfad; Deutsch in der Pipeline einstellen |
| Gesprächs-ID | Teilweise: Bridge cached per HA-ID, 128 Threads, feste 60 Minuten | Gerätebindung und einheitlicher Ablauf fehlen |
| HA-Tools | Vorhanden: ausgewählte LLM API, Ausführung/Validierung in HA | Berechtigungsgrenze beibehalten |
| Konkurrenz | Vorhanden: Bridge turn_lock, busy, owner/turn-ID und Retire bei Abbruch | HA-Zuordnung vor ChatLog ergänzen; Replay prüfen |
| Lokale Intents | HA `prefer_local_intents`, vor Agenten-Aufruf | Funktionieren auch ohne Bridge, wenn Pipeline lokal konfiguriert ist |
| Neustart | Bridge löscht Threads, temporäres Codex-Home | Thread-IDs allein sind keine Wiederherstellung |
| Whisper | Natives Wyoming empfohlen; Codex-STT experimentell | Keine Änderung des Echo-Audios durch Agentenwechsel |
| Diagnostik | Teilweise Health/Redaktion; keine Conversation-Timing-Ringdaten | Content-freie Messung ergänzt |
| Memory | Fehlt | Aus; spätere Freigabe/Review erforderlich |

## Nachgewiesene ID-Kette (Quellcode, keine Hardwaremessung)

Geprüft: HA **2026.8.0 und 2026.10.0**, hassmic-Repo-Commit
`92fa487b59dcabe86527bef59f9c4b06ae1ac3b2` (nicht als installierter Firmwarestand behauptet).

1. hassmic `src/hassmic/proto_esphome.c:start` sendet VoiceAssistantRequest
   `start`, Flags, Audioeinstellungen, optional Wakeword. **Kein ID-Feld 2**.
   `on_event` wertet `continue_conversation` aus, speichert die zurückgelieferte
   conversation_id nicht. Das allein bedeutet in modernem HA keinen Kontextverlust.
2. HA `esphome/assist_satellite.py:handle_pipeline_start` nimmt eine ID entgegen,
   gibt sie aber nicht an `async_accept_pipeline_from_satellite` weiter.
3. HA `assist_satellite/entity.py` hält seine eigene `_conversation_id`.
   `async_get_chat_session` setzt abgelaufene IDs zurück. Die Pipeline bekommt
   die HA-Registry-`device_id` und die Satellite-Entity-ID.
4. HA `helpers/chat_session.py` bereinigt nach **5 Minuten** Inaktivität.
   `conversation/entity.py` öffnet ChatSession und ChatLog; `assist_pipeline`
   verarbeitet lokale Intents vor dem Fallback-Agenten.
5. Bisher sendet der Agent nur `chat_log.conversation_id` an die Bridge.
   Diese reused nur bei gleicher ID/Tool-Schema. Neustart, Eviction oder geänderte
   Tools erzeugen einen neuen Thread; HA-Historie wird einmal als untrusted
   Kontext eingefügt. HA besitzt nicht dauerhaft dieselbe Historie wie Codex.

Primärquellen:
[HA Satellite](https://github.com/home-assistant/core/blob/2026.8.0/homeassistant/components/assist_satellite/entity.py),
[HA ChatSession](https://github.com/home-assistant/core/blob/2026.8.0/homeassistant/helpers/chat_session.py),
[HA Pipeline](https://github.com/home-assistant/core/blob/2026.8.0/homeassistant/components/assist_pipeline/pipeline.py),
[HA 2026.10 Satellite](https://github.com/home-assistant/core/blob/2026.10.0/homeassistant/components/assist_satellite/entity.py),
[HA 2026.10 ChatSession](https://github.com/home-assistant/core/blob/2026.10.0/homeassistant/helpers/chat_session.py),
[hassmic](https://github.com/adenta/echo-dot-codex/blob/92fa487b59dcabe86527bef59f9c4b06ae1ac3b2/src/hassmic/proto_esphome.c).
Für ältere HA-Versionen und den Wyoming-Modus ist diese ID-Kette separat zu prüfen.

## Gelieferter Code / Messgrenzen

`metrics.py`: 64 RAM-Traces, zufällige Korrelations-ID, relative monotone Zeiten,
HA-Tool-Dauern und feste Fehlerkategorien. `conversation.py`, `api.py` und
`diagnostics.py`: Export über HA „Diagnose herunterladen“. Kein Audio/Text,
keine Device-/Conversation-IDs in den Trace-Daten. Codex-Zeit hier umfasst
Bridge-Transport und Serverbearbeitung; erstes Textdelta ist kein TTS-Chunk.
Nicht sichtbare Wake/Capture/Wyoming/Piper/Playback-Grenzen sind `null`.

`scripts/collect_assist_trace.py` liest ausschließlich bestehende HA-
`assist_pipeline/pipeline_debug/list|get`-Daten über authentifizierten WebSocket.
Er exportiert nur Zeiten/Local-Route und verwirft alle Event-Payloads. Er löst
keine Geräteaktion aus. `HASS_URL` und `HASS_TOKEN_FILE` im Prozess setzen;
Secret-Datei außerhalb des Repos, nur für Dienstbenutzer lesbar (Linux 0600).

```bash
python scripts/collect_assist_trace.py --pipeline-id PIPELINE --output trace.json
```

Die HA-eigene Trace-Pufferung kann Inhalte enthalten. Der Exporter aktiviert
keine Audioaufzeichnung. Für den Live-Test zwei Computer-Fragen stellen und
beide Runs exportieren; keine privaten Rohtraces/WAVs in Git ablegen.

## Installation / Rollback A

Siehe `installation.md`. Domain `codex_voice` ist verschieden von der bestehenden
`codex_conversation` (OpenAI Codex Conversation). Beide können parallel laufen.
Ein weiterer Aurelio-Upstream unter derselben `codex_voice`-Domain kann nicht
gleichzeitig installiert werden. A ändert kein Konfigurationsschema. Zum
Rollback bisherigen Conversation-Agenten in der unveränderten Whisper/Piper-
Pipeline wählen, Fork-Bridge stoppen und den gesicherten Komponentenordner
wiederherstellen. Ausgeführte Checks stehen in `phase-ab-report.md`.

**Exit Gate A offen:** Vom Nutzer bestätigt: HA 2026.10.0, Python 3.14,
4 vCPUs / 8 GB HAOS-RAM. Die genaue Wyoming-Faster-Whisper-Version und der
Echo-Live-Trace liegen nicht vor. Quellcode-/Mocknachweise ersetzen diesen
Abnahmetest und die Messung der VM-Auslastung nicht.
