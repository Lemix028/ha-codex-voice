# Kurzzeitkontext und Datenschutz

## Ergänzung für v0.8.0

Neue Profile verwenden Kontext nur bei erkennbarer Anschlussfrage, einer
passenden Antwort auf eine höchstens zwei Minuten alte Rückfrage oder
ausdrücklicher Fortsetzung. Eigenständige Themen übertragen keine alte
Historie. Der letzte Kontext bleibt begrenzt im RAM; Reset, Fehler, TTL und
Neustart entfernen ihn. Auch strukturierte lokale HA-Ergebnisse werden nur
für diesen kurzfristigen Kontext behalten, niemals im Diagnose-Ring.

Adaptive Profile speichern keine Sitzungsmetadaten und verwenden für jeden
Turn einen frischen Codex-Thread mit unmittelbarem Löschversuch. Die bisherigen Profile behalten
ihren v0.7.0-Modus und dessen unten beschriebene Wiederherstellung. Das separate
Wissensrepository und die Webrichtlinie sind vorbereitet, standardmäßig
deaktiviert und nicht als Modellwerkzeuge registriert. Echo-Satelliten erhalten
keinen Zugriff auf private Wissenseinträge. Details: [v0.8.0.md](v0.8.0.md).

## Bisheriger Modus und gemeinsame Grenzen

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
| Langzeitmemory | Vorbereitetes Opt-in-Repository, nicht aktiviert oder als Tool registriert | Keine Datenbank ohne explizite Aktivierung und berechtigten Zugriff |

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
