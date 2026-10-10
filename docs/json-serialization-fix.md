# JSON-Serialisierung: HA 2026.10 / Codex Voice 0.6.0

## Reproduzierte Ursache

Der erste fehlerhafte Wert ist im normalen Assist-Start-Payload
**`$.tools[0].parameters`**: ein `probatio.UNSUPPORTED`-Marker. In den getesteten
HA-2026.10-Bibliotheken hat er den Typ
**`probatio.codecs._shared._Unsupported`**. Dies ist kein Gesprächsinhalt.

HA 2026.10 stellt `llm.Tool.parameters` auf **`probatio.Schema`** um und
`llm.selector_serializer` liefert bei nicht selbst verarbeiteten Knoten
`probatio.UNSUPPORTED`. Der bisherige `voluptuous_openapi.convert` prüft nur
seinen eigenen, anderen `UNSUPPORTED`-Marker. Er interpretiert den neuen Marker
als fertiges Schema und legt ihn unverändert unter `parameters` ab. HA `JSON_DUMP`
weist ihn beim Start-Payload zurück, bevor `ws_connect` die Bridge erreicht.

Primärquellen: [HA LLM API 2026.10](https://github.com/home-assistant/core/blob/2026.10.0/homeassistant/helpers/llm.py),
[HA JSON-Encoder](https://github.com/home-assistant/core/blob/2026.10.0/homeassistant/helpers/json.py).
Die Bibliotheken wurden in einer eigenen Test-venv installiert; kein Zugriff
auf private Produktiv-Payloads oder OAuth-Daten war für die Reproduktion nötig.

## Fix und Sicherheitsgrenzen

- `llm_tools.py`: native Probatio-Schemas mit **`probatio.to_openapi`** konvertieren;
  alte Voluptuous-Schemas weiterhin mit ihrem eigenen Codec. Die Adapter übersetzen
  die beiden Unsupported-Marker ausdrücklich. Kein Marker wird als String/Schema
  akzeptiert. `voluptuous-openapi==0.4.1` ist eine eigene Manifest-Anforderung,
  da eine frische HA-2026.10-Installation die alte Abhängigkeit nicht mehr mitliefert.
- `json_serialization.py` / `api.py`: rekursive, typgerechte Normalisierung vor
  HA `JSON_DUMP`. ISO-Datums-/Zeitwerte, Enum-Werte, UUID, Pfade, Dataclasses,
  HA `as_dict`, Mapping-/Tuple-/Set-Strukturen und gültige JSON-Fragmente behalten
  strukturierte Werte. Keine pauschale `default=str`-/`repr`-Umwandlung.
- Unbekannte Objekte, Bytes/Audio, nicht endliche Zahlen, überschrittene Integer-
  Bereiche, ungültiges Unicode, Zyklen und kollidierende JSON-Keys werden abgewiesen.
  `Decimal`/`timedelta` sind nicht ohne eine fachliche Einheitenentscheidung
  freigegeben. Es gibt keine stillen Ersatzwerte und keine Auslassung defekter Felder.
- `conversation.py`: HA-2026.10-`ToolResultContent.result.data` lesen, den neuen
  `error`-Flag erhalten; alte `tool_result`-Daten bleiben kompatibel. Ausführung
  erfolgt weiterhin über HA ChatLog und die **ausgewählte normale HA-LLM-API**.
  Keine direkte Service-Ausführung, keine neuen Entitätsrechte.
- Der experimentelle Realtime-Broker verwendet denselben Schema-/Result-Adapter,
  behält aber seinen eigenen Transport, seine Größenprüfungen und den bisherigen
  Opt-in. Er wird für die normale Assist-Steuerung nicht benötigt oder aktiviert.

## Diagnose ohne Werte

Bei einer Ablehnung wird ausschließlich Pfad, Datentyp und feste Fehlerkategorie
protokolliert. Beispiel der synthetisch reproduzierten Ursache:

```text
Conversation JSON serialization failed:
path=$.tools[0].parameters
type=probatio.codecs._shared._Unsupported
reason=unsupported_type
```

Frei benannte Dictionary-Schlüssel werden als `[<key#0>]` usw. dargestellt,
damit auch private Entity-Namen oder Argument-Schlüssel nicht ins Log gelangen.
Protokoll-/Schema-Felder wie `messages`, `arguments` und `parameters` bleiben
erkennbar. Kein Dump des Payloads, keine Tool-Argumentwerte, keine Ausnahme-
Repräsentation privater Objekte, keine Tokens. Die gesprochene Fehlermeldung bleibt
generisch; tatsächlich nicht unterstützte Werte bleiben fail-closed.

## Tests und reproduzierbare Kompatibilität

`tests/component/test_json_serialization.py` reproduziert den alten Markerfehler
mit den echten HA-Bibliotheken und prüft die Konvertierung. Weitere Fälle:
mit/ohne Tools, verschachtelte zeitliche/strukturierte Werte, Diagnose-Redaktion,
Opaque-Objekte ohne repr, ungültige Zahlen/Unicode, Zyklen, Schlüssel-Kollisionen,
neuer ToolResult-Error-Flag und unveränderte Zielvalidierung. Zusätzlich wird
die tatsächlich registrierte eingebaute Assist-API mit ihren verfügbaren
HA-Intent-Tools (einschließlich HassTurnOn/HassTurnOff) serialisiert.

Ein echter `BridgeClient` spricht einen lokalen Test-WebSocket an. Ein echter
HA `APIInstance`/ChatLog validiert und schaltet ausschließlich `light.synthetic`
im Test-Hass; `switch.nuc` bleibt an. Ein explizit nicht erlaubter Target wird
abgewiesen. Keine dieser Aktionen betrifft reale Geräte.

```bash
# Bestehende, im Fork gepinnte HA-2026.8-Baseline
uv sync --extra test --extra lint
uv run pytest tests/component/test_json_serialization.py tests/component
uv run pytest -p no:homeassistant tests/bridge

# Separate HA-2026.10.0-Kompatibilitätsumgebung
uv venv --python 3.14 .venv-ha-2026-10
uv pip install --python .venv-ha-2026-10/bin/python -r requirements-ha-2026.10.txt
.venv-ha-2026-10/bin/python -m pytest tests/component
```

Der **isolierte 0.6.1-Hotfix**, ohne A/B-Sitzungsänderungen, wurde mit
Python 3.14.4 unter Linux/WSL vollständig getestet:

| Umgebung | Komponenten-Tests |
| --- | --- |
| HA 2026.10.0 | **137 bestanden**, 69,97 s |
| HA 2026.8.0 | **135 bestanden, 2 übersprungen**, 69,04 s |

Die Baseline-Skips betreffen die erst in HA 2026.10 vorhandenen Probatio-/
ToolResult-Typen. Lint/Format des isolierten Standes sind bestanden
(97 Python-Dateien).
CI enthält beide HA-Versionen; eine Ausführung auf GitHub wird nicht behauptet.
Der vollständige A/B-Stand enthält zusätzlich `phase-ab-report.md`.
Pipeline-/Echo-Hardwaretests sind davon getrennt und weiterhin offen.

## Installieren / Rollback

Für die bestehende 0.6.0-Installation ist der getrennte JSON-Hotfix-Commit bzw.
das **0.6.1-Hotfix-Komponentenarchiv** vorgesehen. Komponentenordner sichern,
`custom_components/codex_voice` ersetzen, HA neu starten. HA installiert die
deklarierte Legacy-Abhängigkeit automatisch. Das normale HA-Toolprofil weiterhin
auswählen; keinen Realtime-Schalter einschalten. Für den Hotfix ist keine neue
Bridge-Sitzungsfunktion nötig. Noch kein veröffentlichter HACS-Release behauptet.

Beim vollständigen A/B-Stand 0.7.0 gelten zusätzlich `installation.md` und die
gemeinsame Bridge-/Komponenten-Aktualisierung für aktivierte Sitzungsfortsetzung.

Rollback: vorherigen Komponentenordner wiederherstellen und HA neu starten,
oder die bestehende Pipeline auf den funktionsfähigen ursprünglichen
`codex_conversation`-Agenten stellen. Schema/Datenspeicher des JSON-Hotfixes werden
nicht migriert. Auf HA 2026.10 kann 0.6.0 erneut denselben Fehler zeigen;
Zurückrollen repariert diese Upstream-Inkompatibilität nicht.
