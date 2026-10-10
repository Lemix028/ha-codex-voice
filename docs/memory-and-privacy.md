# Kurzzeitkontext und Datenschutz

Phase A/B aktiviert **kein Langzeitgedächtnis**, keine automatische Zusammenfassung,
kein WAV-Archiv und keinen Debug-Mitschnitt. Nutzertexte, Antworten und HA-Tool-
Ergebnisse werden zur aktuellen Anfrage an Codex gesendet. Der Fork behält bis
zu 24 Nachrichten / 16.000 JSON-Zeichen pro Sitzung **im RAM** für Kontinuität
nach Bridge-Neustarts. Alte Nachrichten werden vollständig entfernt, nicht in
ungültige JSON-Fragmente zerschnitten. Bei verkürztem Kontext und neuem Thread
wird die Einschränkung gesprochen. Das ist keine zugesagte Nutzermemory.

Die bestehende Codex-Runtime nutzt weiterhin ihr privates temporäres Home und
thread/delete. Modell-Threads können dort für die aktive Laufzeit auf Disk
existieren; `history.persistence=none` ist keine Garantie, dass der App-Server
keine Thread-Dateien anlegt. Ein Systemabsturz kann temporäre Dateien hinterlassen.
Der dedizierte Dienstbenutzer/PrivateTmp schützt den Zugriff; nach Dienststop nur
eindeutig verwaiste Runtime-Verzeichnisse dieses Dienstes bereinigen. Provider-
Datenverarbeitung unterliegt weiter dem verwendeten Codex-/ChatGPT-Dienst.

| Speicher | Inhalt | Dauer / Kontrolle |
| --- | --- | --- |
| HA-native ChatLog | Von HA verwalteter Kurzzeitverlauf | HA-Timeout, im geprüften Core 5 Minuten |
| Fork-RAM | Begrenzte erfolgreiche Turns, native IDs nur temporär | Konfigurierbare 2–60 Minuten; Reset/Unload/Neustart |
| HA Store, optional | Gehashte Bindung, Salt, zufällige ID, Zeitstempel | TTL-Bereinigung, Reset rotiert, Deaktivieren entfernt Datei |
| Diagnose-RAM | Letzte 64 numerische Traces, feste Statuskategorien | Ring / Neustart; Export nur inhaltlos |
| Codex-Thread | Aktiver Dialog im isolierten Runtime-Home | TTL/Eviction/Reset/Bridge-Shutdown, experimentelles Backend |
| Langzeitmemory | Kein Speicher implementiert/aktiviert | Separater späterer Review/Opt-in erforderlich |

Owner-Hashes sind pseudonyme Metadaten, keine Garantie vollständiger Anonymität.
Das Salt wird nur in der Store-Datei gespeichert. Diagnosen enthalten weder
Salt/Owner-/Thread-IDs noch Transkripte/Tool-Argumente. Der konfigurierte Fallback
wird im HA-Diagnoseexport geschwärzt. Bekannte HA-Benutzer werden getrennt;
ein Echo liefert **keine Sprecheridentifikation**. Mehrere Personen desselben
anonymen Echo-Geräts teilen dessen Kurzzeitdialog.

Der Exporter liest HA-Debugdaten nur im RAM und verwirft die Nutzdaten vor dem
Schreiben. HA/Whisper/Piper können unabhängig eigene Logs/Trace-Puffer besitzen:
keine DEBUG-Logs für ChatLog/LLM oder STT aktivieren, sofern Inhaltsaufzeichnung
nicht bewusst gewünscht ist. Im Fork wird kein Aufnahme-Debugmodus angeboten.
Keine private Aufnahme für diese automatisierten Tests erforderlich.

Backups können Sitzungsmetadaten und reguläre HA-Secrets enthalten. Außerhalb
des Repos schützen; bei vollständiger Löschung auch Backup-Retention berücksichtigen.
Ein späteres Memory-Schema benötigt eine eigene Backup-/Migrations-/Löschprüfung.
Konkrete Folgeschritte stehen in `future-changes.md`.
