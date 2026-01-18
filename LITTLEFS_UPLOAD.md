# LittleFS Upload Instructies

## Overzicht
Dit project is geconfigureerd om bestanden uit de `data/` map naar het LittleFS filesystem op de ESP32 te uploaden.

## Configuratie
- **Partition:** littlefs (1 MB) in `partitions_4MB.csv`
- **Data folder:** `data/` (bevat: index.html, app.js, style.css)
- **Upload script:** `upload_littlefs.py`

## Gebruik

### Optie 1: Via PlatformIO commando's (Aanbevolen)

#### Alleen filesystem image bouwen:
```bash
pio run --target buildfs --environment ttgo-t8
```

#### Alleen filesystem uploaden:
```bash
pio run --target uploadfs --environment ttgo-t8
```

#### Build en upload in één commando:
```bash
pio run --target buildfs --environment ttgo-t8 && pio run --target uploadfs --environment ttgo-t8
```

### Optie 2: Via custom script target

Het custom script `upload_littlefs.py` biedt een gecombineerde target:

```bash
pio run --target uploadlfs --environment ttgo-t8
```

Dit commando voert beide stappen automatisch uit:
1. Bouwt het LittleFS image uit de `data/` folder
2. Upload het image naar de ESP32

### Optie 3: Via VS Code PlatformIO sidebar

1. Open de PlatformIO sidebar (alien icoon)
2. Navigeer naar: **ttgo-t8** > **Platform**
3. Klik op: **Build Filesystem Image**
4. Klik daarna op: **Upload Filesystem Image**

## Belangrijke opmerkingen

- Zorg ervoor dat je ESP32 verbonden is via USB voordat je upload
- De upload overschrijft alle bestaande bestanden op het LittleFS partition
- Wijzigingen in de `data/` folder vereisen een nieuwe build en upload
- De ESP32 moet niet draaien tijdens filesystem upload (stop de monitor indien actief)

## Bestanden in data/ folder

Momenteel worden de volgende bestanden geüpload naar LittleFS:
- `index.html` - Web interface
- `app.js` - JavaScript applicatie
- `style.css` - Stylesheet

## Troubleshooting

Als de upload faalt:
1. Controleer of de ESP32 correct verbonden is
2. Stop de serial monitor (pio device monitor)
3. Probeer de upload speed te verlagen in platformio.ini
4. Controleer of de partition table correct is geflashed
