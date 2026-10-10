# Konkrete Folgeveränderungen nach A/B

Noch **nicht aktiviert/implementiert**. Reihenfolge nach Abschnitt 6 des Katalogs:
Tools/Safety (Phase C), Memory (Phase D), Audio/STT (Phase E).

## Sichere HA-Tools und verifizierter lokaler Kontext

`custom_components/codex_voice/tool_policy.py` erhält `ActionClass` (read,
reversible, sensitive), `ToolAttempt` (Turn-/Call-ID, dispatched/confirmed/unknown)
und einen Wrapper **um die ausgewählte HA-LLM-API**. Keine generische Service-
Registrierung. Validierung/Entitätsfreigabe bleiben in HA; zusätzlich begrenzen
Licht-Intents die Domain strikt auf `light`. Ein NUC-Power-Switch wird dadurch
nicht zum Lichtziel, auch wenn Namen/Raum ähnlich sind. Namen/Aliase kommen aus
HA-Registry/Exposure, nie aus Namensähnlichkeit als Berechtigung.

`device_context.py`: `VerifiedAction(turn_id, entity_ids, domain, operation,
observed_state, observed_at, source)` nur nach erfolgreicher, zustandsbestätigter
Ausführung. Zeitlich begrenzt, max. 8 Zielgeräte, RAM-only, als untrusted strukturierte
Kontextdaten. `SessionManager.record_verified_action(owner, action)` und
`take_recent_actions(owner)` mit derselben Geräte-/User-Bindung. Keine Umdeutung
in Memory. Bei Teilfehlern nur tatsächlich bestätigte Ziele aufnehmen.

**B05-Schnittstellengrenze:** HA 2026.8/2026.10 lokale Intents umgehen den Agenten und
schreiben hier nur Assistant-Speech in ChatLog. Speech belegt keinen Target/State.
Keine privaten HA-Datenstrukturen patchen. Zuerst einen unterstützten HA-Hook
für typisierte lokale IntentResult-Daten identifizieren/mit Upstream abstimmen.
Alternativ ausdrücklich konfigurierte HA-Sentence-/Intent-Adapter für genau
freigegebene Lichter, die ihren strukturierten Erfolg liefern; die gewöhnliche
lokale Route muss bei Bridge-Ausfall weiterhin funktionieren. Bis dahin fragt
der Fork bei erkannten lokalen Pronomen-Steuerbefehlen nach dem Gerät. Nach
HA-ChatLog-Verlust ist nicht jeder lokale Vorgänger überhaupt beobachtbar;
diese Einschränkung ist nicht als erfülltes B05 zu zählen.

Read-Tools: typisierte `SensorReading(value, unit, state_timestamp, stale)` nur
exponierte Sensoren. Watt/kWh, Monatskosten mit angegebenem Zeitraum und Tarif;
ohne Messwert/Tarif Rückfrage, keine erfundenen Werte. Timeouts markieren
`unknown`; keine Schreib-Retries. Danach ausschließlich autorisierte Zustands-
Abfrage möglich. Sensitive Aktionen separat bestätigen, unbekannte Targets
ablehnen. Keine Shell/Dateisystem/Host-/beliebigen Netzwerktools.

Regressionssuite: T01–T03, T08–T11, lokale Befehle ohne Bridge/Codex, **NUC bleibt
an** bei „alle Lichter im Arbeitszimmer aus“, doppelte Call-ID, neue ID nach
unknown nicht als automatische Wiederholung, Domain-/Raum-/Exposure-Grenzen,
Teilfehler und widerrufene Freigabe. HA Intents/Aliase vor zusätzlichem Dispatcher
nutzen; keine zusätzliche LLM-Klassifizierung.

## Opt-in-Memory

`memory_store.py` als separater SQLite-Dienst mit `PRAGMA user_version`, WAL,
Transaktionen und einem einzigen serialisierten Writer außerhalb des HA-Eventloops.
Tabellen `sessions_metadata`, `memories`, `deletion_events`, `summaries` getrennt.
`memories`: Owner, opaque ID, Text, Ursprung/Consent, created/expires, revision.
`deletion_events` speichert Ereignis/ID/Zeit, niemals gelöschten Text. Zusammen-
fassungen bleiben separat und standardmäßig aus. Keine WAV-/Audio-Spalten.

APIs `remember(scope, text, consent, request_id)`, `list(scope)`, `forget(scope,
id)`, `export(scope)`, `delete_all(scope)`; Owner wird aus vertrauenswürdigem HA-
Kontext bestimmt, nicht vom LLM. Request-ID als Unique-Constraint verhindert
doppelte Writes. Sprachbefehle „Merk dir … / Was hast du dir gemerkt? / Vergiss …“
nur nach Opt-in. Keine stillen Modellvorschläge als gespeicherte Wahrheit.
Korrekturen ersetzen mit nachvollziehbarer Revision; Widersprüche explizit markieren.

`memory_context.py` wählt relevante Einträge (max. 8 / ca. 800 Tokens), Herkunft
und Datum als untrusted Kontext. Systemrechte/Tool-Freigaben kommen niemals aus
Memory. Speicher auf dediziertem Volume, nur Dienstbenutzer; HA-UI für aus/an,
Retention, Zusammenfassungen, Export und vollständige Löschung. Das bestehende
Sitzungs-Reset löscht weiterhin ausschließlich Kurzzeitkontext.

Vor Aktivierung: Datenhaltung/Providerpfad prüfen, schemafähiges Backup,
`backup_before_migration`, Test von Schema 1→2 mit Abbruch/Restore, Consent aus
verhindert sämtliche Memory-Writes, T06/T12/T13/T16 inklusive Löschung in Backups
und Logs. Keine implizite Migration der jetzigen HA-Metadaten in Nutzermemory.

## Audio/STT

`scripts/stt_benchmark.py` soll Referenz-CSV und **ausdrücklich bereitgestellte**
Testaufnahmen lesen; kein Aufzeichnen im Hintergrund. Ausgabe enthält WER,
Intent-/Target-/Polarity-Exaktheit, gefährliche Fehler, getrennte Capture/VAD/
Inferenz sowie p50/p95. Fehlende Zeiten null, failed samples mitzählen. Erfasste
Audiofiles außerhalb Git, Retention/Löschung festlegen. Die bereitliegende CSV
enthält 30 synthetische Sätze, keine echte Sprachaufnahme.

Gleiche Bedingungen je Modell: 1 m frontal, 2–3 m frontal, 2–3 m seitlich,
Ruhe / optional Musik; `base-int8` gegen `small-int8`, CPU/RSS/Guest-Budget messen.
Optional Qwen3-ASR nur bei vertretbarer Laufzeit und ständig verfügbarem Dienst.
Kein ausgeschalteter Entwicklungs-PC als Alltags-Abhängigkeit.

`stt_vocabulary.py`: kurze Liste aus conversation-exponierten Entitätsnamen,
Aliasen und relevanten Räumen; keine langen Beispielbefehle. Tatsächlich
installierte `--help`/App-Konfiguration und Version prüfen, Paket/Revision pinnen.
Upstream beschreibt `--initial-prompt`, `--hass-api`, `--hass-prompt-max-tokens`
und Secret-Datei `WYO_WHISPER_HASS_TOKEN_FILE`, plus Exposed-Entity-Filter.
[Primärquelle](https://github.com/OHF-Voice/wyoming-faster-whisper#home-assistant).
HA-Token besitzen keine beliebig feingranulare Scope-Garantie: eigener Benutzer
mit geringstmöglichen realen Rechten, oder zuerst statisches Vokabular ohne HA-
Token. Keine nicht freigegebenen Namen in Biasing aufnehmen.

`--debug` kann das genaue Vokabular loggen; nicht aktivieren. Namen-Biasing
garantiert keine sichere Polarität; nie `an` in `aus` umdeuten. Bei unsicherem
Target Rückfrage. AFE/micRaw/micAsr erst nach Geräte-Settings-Backup und getrenntem
Diagnoseauftrag ohne Flash vergleichen. VAD ausschließlich nach Messung ändern,
keine abgeschnittenen Silben für eine bessere Latenzzahl akzeptieren.
