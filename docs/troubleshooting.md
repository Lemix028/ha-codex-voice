# Computer diagnostizieren

## Kontext fehlt

HA „Diagnose herunterladen“ beim Fork: `conversation_traces` zeigt pro Anfrage
`identity_source`, ob eine Incoming-ID vorhanden/geändert war, `context_status`
und Zeiten. IDs selbst sind nicht enthalten. `device` ist bevorzugt,
`satellite` die zweite Wahl, `incoming_id` kann nach Wakewords wechseln;
`isolated` führt bewusst keinen geräteübergreifenden Dialog.

Fortsetzung im Profil aktivieren, TTL prüfen, Bridge und Komponente gemeinsam
aktualisieren. Alte Bridges ohne `context_status` werden im neuen Sitzungsmodus
mit einem sicheren Fehler abgewiesen. ID-Modus bleibt als Fallback nutzbar.
Nach ausschließlich Bridge-Neustart können erfolgreiche RAM-Turns als untrusted
Kontext wieder eingehen. Nach HA-Neustart bleiben nur Bindungen: noch laufender
Bridge-Thread wird weitergenutzt, sonst wird Verlust/Teilkontext gesprochen.
Keine Nachricht/Tool-Ausführung wird dafür automatisch wiederholt.

## Fehler, Quote, Abbruch

`authentication_required`: reguläres `codex login` unter dem Bridge-Benutzer.
`quota_exhausted`: Abo-Kontingent respektieren; lokale HA-Intents weiterhin nutzen.
`busy`: maximal zwei Aufrufe pro Sitzung zugelassen, unterschiedliche Geräte
bleiben getrennt. `cancelled_outcome_unknown` / `tool_outcome_unknown`: Zustand
gezielt lesen; eine bereits ausgeführte Aktion ist nicht zurückgenommen.
Keine automatische Wiederholung. API hat 25 Sekunden Tool-Deadline, Bridge-
Turn und WebSocket haben ihre bisherigen absoluten Grenzen.

`remote_reset_unavailable`: neue Sitzung ist lokal bereits getrennt, alte Bridge-
Threads laufen aus bzw. werden beim Dienststop gelöscht. Storage-Warnung:
Dateirechte/HA-Speicherplatz prüfen. Unbekannte Schema-Dateien erst sichern;
sie werden nicht automatisch überschrieben.

## Messung / lokale Route

Fork-Traces beginnen an der Conversation-Grenze. HA-native lokale Befehle
umgehen diesen Agenten und erscheinen deshalb nicht in diesem Ring.
`collect_assist_trace.py` liest den normalen HA-Pipeline-Trace für `processed_locally`
und STT-/Intent-/TTS-Stufen. `run-end` bedeutet nicht „Lautsprecher fertig“.
Erstes Textdelta ist kein Piper-PCM-Chunk. Nicht sichtbare Werte bleiben null.
Tools behalten HA als Validierungs-/Ausführungsinstanz.

## Dumpfe/fehlerhafte STT

Eine dumpfe vorhandene `01_stt-stt.faster_whisper.wav` weist auf Aufnahme/AFE hin;
ein neuer Conversation-Agent verändert diese Datei nicht. Mikrofonabstand und
Richtung kontrollieren; keine `an`-/`aus`-Korrektur durch Vermutung. Die synthetische
30-Satz-Liste in `tests/fixtures/de_commands.csv` dient später einer kontrollierten
A/B-Reihe. Keine Verbesserungsbehauptung ohne dieselben Aufnahmen und Hörvergleich.

Die installierte Whisper-App muss ihre tatsächlichen Optionen zeigen. Upstream
hat `--initial-prompt`, conversation-exponierte Namen/Aliase und
`WYO_WHISPER_HASS_TOKEN_FILE`; nicht jede HAOS-App bietet diese Optionen an.
Kein Token in Prozessargumente. AFE/micRaw/micAsr-Vergleich ist eine spätere,
separat kontrollierte Diagnose; hier kein ADB oder Firmwareeingriff.
