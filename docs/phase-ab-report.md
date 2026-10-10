# Computer: Meilensteine, Tests und Abnahme

Stand: 10.10.2026. Basis `065413f517ee9150b5bdce47cd685212db169bf9`,
lokaler Branch `codex/computer-phase-ab`. Die Implementierung und die
Softwaretests sind ausgeführt. Eine produktive Installation, eine reale
Echo-Geräteaktion und die Hardware-Abnahme wurden hier nicht ausgeführt.

## Meilenstein A: Audit und Diagnose

Untersucht wurden Conversation/ChatLog, die native HA-LLM-API, Bridge-Threads,
Tool-Ausführung, Wyoming/Piper und hassmic. HA 2026.10 nutzt einen anderen
Tool-Schema-Codec als die 2026.8-Baseline. Die ID-Kette und die Grenzen zwischen
HA-ChatLog und Codex-Thread stehen in `computer-audit.md`.

Geliefert: `metrics.py`, Instrumentierung in `conversation.py`/`api.py`,
Diagnoseexport in `diagnostics.py`, der nur lesende
`scripts/collect_assist_trace.py` sowie Architektur-/Installationsdokumentation.
Der RAM-Ring speichert höchstens 64 Traces mit Zeiten und festen Fehlerkategorien,
keine Sprache, Geräte-IDs, Argumentwerte oder Zugangsdaten. Nicht beobachtbare
Audio-/TTS-Grenzen bleiben ausdrücklich `null`.

Die unveränderte 0.6.0-Basis wurde in einem getrennten Snapshot getestet:
121 Komponenten-Tests und 785 Bridge-Tests bestanden. Auf dem Windows-Checkout
scheiterte zunächst eine Byte-Integritätsprüfung wegen automatisch umgewandelter
CRLF-Zeilenenden des Piper-Lockassets. Nach Wiederherstellung der originalen
Git-LF-Bytes bestanden alle 785 Tests. `.gitattributes` schützt das Asset künftig;
die Integritätsprüfung und der erwartete Hash bleiben erhalten.

Installation und Rollback: `installation.md`. Diagnoseexport benötigt eine
explizite lokale HA-Token-Datei; diese wurde hier weder gelesen noch angelegt.
Die bestehende `codex_conversation`-Integration kann parallel installiert bleiben.

**A-Abnahme offen:** ein realer Computer-Assist-Trace und eine messbare
Sprach-/Ressourcenbaseline. Die vom Nutzer bestätigten Daten sind HA 2026.10.0,
Python 3.14, 4 vCPUs und 8 GB HAOS-RAM. „Aktuellstes Wyoming“ ist keine pinbare
Versionsnummer. Der installierte hassmic-Firmwarestand ist ebenfalls unbekannt.

## Separater Meilenstein: JSON-Hotfix 0.6.1

Die alte Schema-Konvertierung legt `probatio.UNSUPPORTED` unter
`$.tools[0].parameters` ab. Der Wert hat den Typ
`probatio.codecs._shared._Unsupported` und wird vor dem Bridge-Verbindungsaufbau
von HA JSON_DUMP abgewiesen. Reproduktion mit echten HA-2026.10-Bibliotheken,
keine privaten Produktionsdaten benötigt.

Geliefert: `llm_tools.py`, `json_serialization.py`, `api.py`, typed-ToolResult-
Adapter in `conversation.py`, der getrennte Realtime-Result-Adapter,
Manifest-Abhängigkeit, Regressionstests und ergänzende HA-2026.10-CI.
Normale HA-Schema-/Target-Validierung und Toolberechtigungen bleiben erhalten.
Unbekannte Werte werden mit redigiertem Pfad/Typ diagnostiziert und abgewiesen.
Der Hotfix benötigt keine neue Bridge-Sitzungsfunktion. Vollständige Erläuterung,
Installation und Rollback stehen in `json-serialization-fix.md`.

Der Fix ist als Commit **`6314d1e`** und lokaler Branch
**`codex/fix-ha-2026-10-json`** getrennt erreichbar. Der separat exportierte
0.6.1-Stand besteht **137 Komponenten-Tests unter
HA 2026.10.0** und **135 unter HA 2026.8.0** (dort 2 HA-2026.10-Typfälle
übersprungen), jeweils Python 3.14.4. Damit ist die minimale Fassung unabhängig
von den folgenden Sitzungsänderungen geprüft.

## Meilenstein B: Sitzungen und Kontext

Geliefert: `conversation_sessions.py`, HA-Entity-Anbindung, neue deutsche und
englische Profiloptionen, Bridge-TTL/Reset/Replay-Schutz in `bridge/service.py`
und begrenzte Kontextübernahme in `bridge/conversation_context.py`.

- Zuordnung innerhalb eines Profils: vertrauenswürdige Geräte-ID, alternativ
  Satellite-ID, zusätzlich bekannter HA-Benutzer. Bei fehlenden IDs isolierter
  ID-Modus oder ausdrücklich konfiguriertes Profil für genau ein Gerät.
- Inaktivitätsdauer 2–60 Minuten, standardmäßig 15; maximal 128 Sitzungen,
  begrenzte Warteschlange, Serialisierung paralleler Aufrufe je Zuordnung.
- RAM-Kontext maximal 24 vollständige Nachrichten / 16.000 JSON-Zeichen.
  Kein Abschneiden mitten in JSON. System-/Developer-Rechte werden aus
  importiertem Kontext entfernt; Historie bleibt untrusted.
- Optionaler HA-Store Schema 1 enthält Salt, Owner-Hashes, zufällige Bridge-IDs
  und Zeitstempel. Keine Texte, Audiodaten oder Tool-Ergebnisse.
- Neustart: bei laufender Bridge kann die gespeicherte Zuordnung wieder an
  ihren Thread anknüpfen. Nach Bridge-Neustart ist nur vorhandener begrenzter
  RAM-Kontext rekonstruierbar. Nach beiderseitigem Neustart ist früherer Inhalt
  verloren; der Assistent meldet unvollständigen Kontext.
- „Neues Gespräch“ rotiert nur diese Zuordnung; TTL/Reset räumen auf.
  Abbruch/Fehler rotieren die Bindung. Keine automatischen Geräte-Retries,
  doppelte Request-/Tool-IDs werden abgewiesen. Tool-Timeout: 25 Sekunden,
  unbekanntes Ergebnis wird ausdrücklich als unsicher gemeldet.

Softwaretests decken Fortsetzung bei wechselnder HA-ID, Geräte-/User-Isolation,
TTL, scoped Reset, Metadaten-Neustart, verlorenen Kontext, parallele Aufrufe,
Überlast, Abbruch, Replay und begrenzte Historie ab. Bestehende Profile behalten
den alten ID-Modus, bis Fortsetzung explizit aktiviert wird.

**B05 bleibt teilweise offen:** native lokale Intents umgehen den Agenten;
Assistant-Speech ist kein verifiziertes Gerät-/Aktions-/State-Ergebnis.
Bei beobachtbaren lokalen Pronomen-Steuerbefehlen fragt der Fork sicher nach.
Nach HA-ChatLog-Verlust ist auch der lokale Vorgänger nicht immer beobachtbar.
Ein unterstützter typisierter Hook und konkrete Adapter sind in
`future-changes.md` entworfen. Dies zählt nicht als vollständige B05-Erfüllung.

Installation, Konfiguration, Datenformat und Rollback: `installation.md`;
Datenschutz/Providerpfad: `memory-and-privacy.md`. Neue ConfigEntry-Optionen
benötigen keine bestehende Schema-Migration. Langzeitgedächtnis bleibt aus.

## Ausgeführte Prüfung des vollständigen 0.7.0-Standes

Linux/WSL, Python 3.14.4, getrennte Umgebungen mit echtem HA Core:

| Prüfung | Ergebnis |
| --- | --- |
| HA 2026.10.0: `python -m pytest tests/component` | **154 bestanden**, 77,94 s |
| HA 2026.8.0: `python -m pytest tests/component` | **152 bestanden, 2 übersprungen**, 77,01 s |
| Bridge: `python -m pytest -p no:homeassistant tests/bridge` | **792 bestanden**, 31,22 s |
| Mypy für API, LLM-/Realtime-Adapter, Conversation, Sessions, JSON, Metrics, Exporter | **8 Dateien ohne Fehler** |
| `ruff check .`, `ruff format --check .` | bestanden, **112 Dateien formatiert** |
| Python 3.11 `compileall` für Bridge, Device-Skripte und Scripts | bestanden |
| `git diff --check` | bestanden |

Die zwei Baseline-Skips betreffen ausschließlich den erst in HA 2026.10
vorhandenen Probatio-/ToolResult-Pfad. Ein lokaler WebSocket-Integrationstest
verwendet echte APIInstance/ChatLog und schaltet nur `light.synthetic` im
Test-Hass. `switch.nuc` bleibt an; ein nicht erlaubter Target wird abgewiesen.
Dies ist keine reale Smart-Home-Aktion oder Sprachqualitätsmessung.

Reproduzierbare Befehle:

```bash
uv sync --extra test --extra lint
uv run ruff check .
uv run ruff format --check .
uv run pytest tests/component
uv run pytest -p no:homeassistant tests/bridge
uv run mypy --follow-imports=silent custom_components/codex_voice/api.py custom_components/codex_voice/llm_tools.py custom_components/codex_voice/realtime_tools.py custom_components/codex_voice/conversation.py custom_components/codex_voice/conversation_sessions.py custom_components/codex_voice/json_serialization.py custom_components/codex_voice/metrics.py scripts/collect_assist_trace.py
uv venv --python 3.14 .venv-ha-2026-10
uv pip install --python .venv-ha-2026-10/bin/python -r requirements-ha-2026.10.txt
.venv-ha-2026-10/bin/python -m pytest tests/component
```

CI wurde ergänzt, aber nicht auf GitHub ausgeführt. HACS/hassfest und eine
ausgeführte Python-3.11-Bridge-Testsuite sind hier nicht als bestanden belegt;
die vorhandenen CI-Jobs prüfen diese zusätzlich. Windows-Pytest zeigte einen
Hänger; die oben genannten vollständigen Testläufe liefen unter Linux/WSL.

## Vorher/Nachher und noch ausstehende Hardware-Abnahme

| Merkmal | 0.6.0-Basis | Gelieferter Stand |
| --- | --- | --- |
| HA 2026.10 Tool-Start | Probatio-Sentinel, JSON-Fehler reproduziert | normaler Tool-Start/Result getestet |
| Bindung über HA-ID-Wechsel | nur ankommende HA-ID | Geräte-/Userbindung getestet |
| Sitzungs-TTL | Bridge 60 Minuten | konfigurierbar 2–60, Cleanup/Reset getestet |
| Neustartkontext | Threadverlust ohne eindeutige Aussage | Metadaten/Restkontext begrenzt, Verlust explizit |
| Gefährliche Sprachfehlsteuerungen / 90%-Ziel | nicht gemessen | nicht gemessen |
| Pipeline-Latenz p50/p95 | nicht gemessen | nicht gemessen |
| HAOS-/Bridge-CPU, RSS, OOM | nicht gemessen | nicht gemessen |
| Echte Power-/Energiesensor-Auskunft | nicht geprüft | nicht geprüft |

Zur Hardware-Abnahme die Test-Pipeline aus `installation.md` verwenden:
zwei getrennte Wakewords innerhalb TTL, dieselbe Nachfrage nach TTL,
„neues Gespräch“, HA-/Bridge-Neustart getrennt und gemeinsam, zweites Gerät
bzw. anderer HA-Benutzer und Abbruch/Überlast. Dabei T04/T05/T07/T14 aus dem
Katalog protokollieren; keine privaten Rohtraces ins Repository ablegen.

Für die Audio-Baseline liegen 30 synthetische Referenzsätze in
`tests/fixtures/de_commands.csv`. Gleiche Bedingungen bei 1 m / 2–3 m frontal
und seitlich, Ruhe/optional Musik; Intent, Target und Polarität gemeinsam
bewerten. Es wurden keine Aufnahmen gemacht. A-/B-Exit-Gates bleiben bis
zu diesen realen Nachweisen und der benannten B05-Grenze offen.

Die nächste Implementierung ist konkret in `future-changes.md` beschrieben:
verifizierter lokaler Kontext und sichere Tools, überprüftes Opt-in-Memory,
anschließend STT-Benchmark/Vokabular. Diese Features wurden nicht aktiviert.

## Lokale Installationsartefakte

Unter `dist/` werden aus den Git-Commits vier Archive bereitgestellt:

- `codex_voice.zip`: HACS-Release-Asset 0.7.0 mit den Integrationsdateien direkt
  im ZIP-Hauptverzeichnis. Für einen GitHub-Release mit Tag `v0.7.0` auf `main`.
- `computer-voice-json-hotfix-0.6.1.zip`: minimale Komponente für den vorhandenen
  0.6.0-Stand, keine Sitzungsumstellung und kein nötiges Bridge-Upgrade.
- `computer-voice-component-0.7.0.zip`: kompletter A/B-Komponentenstand.
- `computer-voice-bridge-0.7.0.zip`: dazugehörige Bridge, systemd-Vorlage,
  Trace-Exporter und Dokumentation. Login-Dateien/venvs sind nicht enthalten.

Die Archive enthalten nur Dateien aus den jeweiligen Git-Commits.
`dist/SHA256SUMS.txt` und `dist/validation.json` halten Prüfsummen,
Commit-Zuordnung und ausgeführte Testergebnisse fest. Der vollständige Stand
ist auf `main` des Forks gepusht; die Archive werden lokal bereitgestellt.
Ein GitHub-Release wurde noch nicht veröffentlicht.
