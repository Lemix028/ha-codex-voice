# Computer installieren und zurückrollen

Diese Anleitung betrifft ausschließlich den normalen **Computer → hassmic →
HA Assist → Wyoming Whisper → Conversation → Piper**-Pfad. Die produktive
Pipeline wird erst nach dem Hardwaretest umgestellt. Kein Echo-Flash/ADB,
ThirdReality-Overlay oder offenes Mikrofon ist erforderlich.

Für v0.8.0 gelten zusätzlich [Gesprächsmodus, Licht-Kurzbefehle und
Pipeline-Vorrang](v0.8.0.md). Neue Profile arbeiten eigenständig; bestehende
Profile wechseln ihren Modus erst nach ausdrücklicher Auswahl.

## Voraussetzungen und Sicherung

- HA Core mindestens 2026.8.0, Komponenten-Tests gegen **2026.8.0 und 2026.10.0**
  mit Python 3.14.4. Die Bridge unterstützt Python 3.11+.
- Codex CLI mindestens 0.147.0, reguläres `codex login` auf dem Bridge-Host.
  Die App-Server-Schnittstelle bleibt experimentell.
- Deine HAOS-VM: **4 vCPUs / 8 GB RAM** laut Angabe. Host: i5-6260U, 16 GB.
  CPU-/RAM-Nutzung und OOM-Grenzen müssen im Betrieb gemessen werden.
- Vor Installation HA-Backup erstellen; bisherigen Pipeline-Agenten, Whisper-
  und Piper-Auswahl notieren. Backup außerhalb des Repos aufbewahren.
- Bestehende Integration `codex_conversation` bleibt installiert. Fork-Domain
  `codex_voice` kollidiert damit nicht. Ein anderer `codex_voice`-Upstream
  müsste vorher gesichert/ersetzt werden, da gleiche Domains nicht parallel gehen.

## Getrennte Bridge im Proxmox-LXC

Unprivilegierten Linux-LXC ohne Host-/Docker-Socket-Mounts verwenden. Eigener
Dienstbenutzer, Code unter `/opt/computer-voice`, Login-State unter
`/var/lib/computer-voice`. Zu Beginn Ressourcen konservativ begrenzen und
überwachen; die beigefügte systemd-Vorlage schlägt 1 CPU / 2 GB vor, ohne eine
Leistungsgarantie. Dies ist eine eigene VM/LXC-Zuteilung, kein Teil der 8 GB HAOS.

Die Bridge-Quellen aus dem Fork-Branch `main` oder dem Bridge-Archiv nach
`/opt/computer-voice` übertragen. Code/venv für den Dienst nur lesbar halten.

```bash
cd /opt/computer-voice
python3 -m venv .venv
.venv/bin/pip install -r bridge/requirements.txt
sudo useradd --system --create-home --home-dir /var/lib/computer-voice --shell /usr/sbin/nologin computer-voice
sudo -u computer-voice -H codex login
```

Die Anmeldung muss unter diesem Benutzer erfolgen. Kein Kopieren/Auslesen/
Umwandeln von `auth.json`, keine API-Key-Konvertierung. Der vorhandene
Bridge-Runtime prüft den Dateibesitz/die Rechte und isoliert Codex-Konfiguration,
Shell, Apps, Plugins, MCP und Netzwerkberechtigungen des Sprachagenten.
Codex benötigt selbst weiterhin seinen normalen Provider-Zugang.

`deploy/systemd/computer-voice.env.example` als Vorlage für
`/etc/computer-voice/bridge.env` verwenden, Eigentümer root, Modus 0600.
Einen eigenen langen Zufallstoken dort eintragen (z. B. mit einem Passwortmanager).
`HA_CODEX_BINARY` auf den tatsächlich installierten nativen CLI-Pfad setzen,
falls der Launcher im Dienst nicht funktioniert. Keine Tokens in Befehlszeilen,
URLs, Git oder Logs. Login-Rechte bei Bedarf prüfen, ohne die Datei auszugeben.

Das Beispiel bindet Loopback. Für HA aus einem anderen LXC entweder einen
TLS-Proxy nutzen oder bewusst die vertrauenswürdige LXC-Adresse binden;
Firewall: Port 8787 ausschließlich vom HAOS-Host erreichbar. Wyoming 10200/10300
ebenfalls nur für HA freigeben. Nicht ins Internet veröffentlichen.

```bash
sudo cp deploy/systemd/computer-voice.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now computer-voice
sudo systemctl status computer-voice
```

Die Vorlagen werden **nicht automatisch installiert**. Login und Dienststart
sind Deployment-Schritte, keine bereits hier ausgeführten Produktionsaktionen.

## HA-Komponente und Test-Pipeline

Für die bestehende 0.6.0-Installation gibt es separat den **0.6.1-JSON-Hotfix**:
Komponentenordner sichern, das Hotfix-Archiv unter `/config` auspacken und HA
neu starten. Dieser Stand erhält den bisherigen Sitzungsmodus und benötigt
kein Bridge-Upgrade; siehe `json-serialization-fix.md`. Für A/B 0.7.0 gelten die
nachfolgenden Schritte und die gemeinsame Aktualisierung der Bridge.

Der vollständige Stand 0.7.0 liegt auf `main` in `Lemix028/ha-codex-voice`.
Für GitHub Releases/HACS das Asset **`codex_voice.zip`** verwenden: Seine
Integrationsdateien liegen direkt im ZIP-Hauptverzeichnis, entsprechend
`hacs.json`. Beim manuellen Installieren dieses ZIP deshalb nach
`/config/custom_components/codex_voice` auspacken.

Das separat benannte `computer-voice-component-0.7.0.zip` enthält dagegen den
Pfad `custom_components/codex_voice` und wird unter `/config` ausgepackt.
Alternativ den Ordner
`custom_components/codex_voice` übertragen. Danach HA neu starten und die
Integration hinzufügen: LXC-Bridge-URL und **separater Bridge-Token**, kein HA-
oder Codex-Login-Token. Bridge und Komponente gemeinsam aktualisieren.

1. Bestehende Assist-Pipeline duplizieren, etwa **Computer Test**.
2. Deutsch `de-DE`, Wyoming Faster Whisper `small-int8` / `de`, vorhandene
   deutsche Piper-Stimme wählen. Nur den Conversation-Agenten auf den Fork setzen.
3. **Befehle bevorzugt lokal verarbeiten** einschalten. Freigegebene Entitäten
   bewusst prüfen; Host-/Power-Schalter nicht für generische Lichtbefehle exponieren.
4. Conversation-Profil „Computer“ konfigurieren. Neue Profile: Fortsetzung an,
   Leerlauf 15 Minuten, Metadaten an. Bestehende Profile behalten den bisherigen
   ID-Modus, bis Fortsetzung explizit aktiviert wird. 2–60 Minuten sind erlaubt.
5. Fallback-Zuordnung normalerweise **leer** lassen. Sie gilt nur bei fehlender
   Geräte- und Satellite-ID, in einem **eigenen Profil für genau einen** bekannten
   Satelliten. Unidentifizierte Aufrufer dieses Profils teilen diese explizite
   Zuordnung. Kein allgemeines Fallback-Profil für mehrere Geräte verwenden.
6. Realtime-Tools ausgeschaltet lassen; dieser Schalter gehört zum separaten
   experimentellen Pfad. Whisper/Piper und Wakeword **Computer** beibehalten.
7. Text-Test, danach bewusst gewählte, harmlose Echo-Tests gemäß Report durchführen.
   Den Echo erst für diese Abnahme auf die Test-Pipeline stellen.

## Konfiguration / Datenformat / Migration

| Option | Default neuer Profile | Wirkung |
| --- | --- | --- |
| `session_continuity` | `true` | Gerät/HA-Benutzer statt wechselnder HA-ID bindet Bridge-Thread |
| `session_idle_minutes` | `15` | 2–60, ab letztem abgeschlossenen Agenten-Turn |
| `session_fallback` | leer | Explizites einzelnes Gerät bei fehlender ID |
| `session_metadata` | `true` | Nur Zuordnungsmetadaten in HA Store, Schema 1 |

Kein bestehendes ConfigEntry-Schema wird migriert. Neue optionale Felder verwenden
kompatible Defaults; keine Altprofile werden still umgestellt. HA speichert je
Profil `.storage/codex_voice.sessions.ENTRY.SUBENTRY`: Schema/Salt, Owner-Hashes,
zufällige Bridge-Gesprächs-IDs und letzte Aktivität. Keine Texte/Tool-Ergebnisse.
Metadaten werden vor und nach Agenten-Turns gespeichert. Fehler lassen den
Dialog weiterlaufen und werden inhaltlos gemeldet; Wiederherstellung kann dann
unvollständig sein. Unbekannte/defekte Schemata werden nicht überschrieben.

Nach 30 Sekunden spätestens nach TTL werden inaktive RAM-Sitzungen bereinigt;
die Bridge räumt in demselben Intervall abgelaufene Threads auf. Ein aktiver
Turn wird dabei nicht gelöscht. Ein rein lokal behandelter Befehl ruft den
Agenten nicht auf und verlängert dessen LLM-Sitzung daher nicht.

„**Computer, neues Gespräch**“ oder „Gespräch beenden“ rotiert nur diese Sitzung
und versucht den alten Bridge-Thread zu löschen. Bei nicht erreichbarer Bridge
ist das neue Mapping trotzdem sofort wirksam; der alte Remote-Cache läuft aus.
Metadaten/Fortsetzung explizit deaktivieren entfernt die zugehörige Store-Datei.
Bereits ausgeführte Geräteaktionen werden nicht rückgängig gemacht.

## Rollback A/B

1. Echo auf die bisherige Pipeline stellen oder dort den bisherigen
   **OpenAI Codex Conversation**-Agenten auswählen. Whisper/Piper weiter verwenden.
2. Falls nur Sitzungen deaktiviert werden sollen: im Fork-Profil Fortsetzung
   ausschalten. Damit wird wieder der alte ID-Modus genutzt und der Store entfernt.
3. Fork-Integration entfernen/gesicherten Komponentenordner wiederherstellen;
   HA neu starten. Die andere Domain wurde nicht verändert.
4. `sudo systemctl disable --now computer-voice`, wenn kein Fork-Client mehr läuft.
   Vorherigen Bridge-Code bei Bedarf aus dem festgehaltenen Upstream-Commit
   wiederherstellen. Neue Protokollfelder sind für alte ID-Clients optional.
5. Eigene verbleibende `.storage/codex_voice.sessions.*`-Metadaten bei gestopptem
   Fork gezielt löschen oder aus dem Backup wiederherstellen. Keine anderen
   `.storage`-Dateien verändern. Native HA-ChatLogs werden ohnehin kurzfristig
   bereinigt; aktives Fork-Reset importiert sie nicht erneut in den neuen Thread.

Rollback ist auf Unit-/Protokollebene geprüft (Defaults, Reset, Metadaten-Schema).
Der echte Pipelinewechsel und Backup-Restore auf HAOS stehen noch zur Abnahme aus.
