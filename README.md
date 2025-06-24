# GeoIP Merge Tool

Скрипт `merge_geoip.py` объединяет два CSV-файла с данными IP-сетей и странами из geoip.noc.gov.ru. 
Он выдаёт итоговый CSV с колонками, как на листе "Итог" из IP.xlsx:

- `_last_changed`
- `network`
- `start_ip`
- `end_ip`
- `code`
- `name`

## Установка

```bash
pip install -r requirements.txt
```

## Использование

```bash
python merge_geoip.py <locations.csv> <blocks.csv> <output.csv> [дд.мм.гггг чч:мм:сс]
```

- `locations.csv` — файл со странами (RU-GeoIP-Country-Locations-en.csv)
- `blocks.csv` — файл с IP-сетями (RU-GeoIP-Country-Blocks-IPv4.csv)
- `output.csv` — итоговый файл
- дата (необязательно) — формат: `дд.мм.гггг чч:мм:сс` (если не указана — спрашивается)

## Пример

```bash
python merge_geoip.py RU-GeoIP-Country-Locations-en.csv \
                      RU-GeoIP-Country-Blocks-IPv4.csv \
                      Итог.csv
```

## Зависимости

См. `requirements.txt`
