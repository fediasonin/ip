# merge_geoip.py
"""
Скрипт склеивает два CSV‑файла из geoip.noc.gov.ru (locations + blocks)
и выпускает аккуратный итоговый CSV c колонками:
    _last_changed, network, start_ip, end_ip, code, name
как на листе «Итог» вашего IP.xlsx.

Запуск:
    python merge_geoip.py <locations.csv> <blocks.csv> <output.csv> [дата]

Где **дата** (необязательный 4‑й аргумент) — время выгрузки в формате
`дд.мм.гггг чч:мм:сс`. Если не указан, скрипт спросит его интерактивно,
а при пустом вводе проставит текущее время.

Зависимости: `pandas ≥ 1.5`  → `pip install pandas`
"""

from __future__ import annotations

import sys
import datetime as _dt
import ipaddress as _ip
import warnings
from pathlib import Path

import pandas as _pd


def _load_locations(path: Path) -> dict[int, tuple[str, str]]:
    """Возвращает словарь geoname_id → (ISO‑код, название страны)."""
    df = _pd.read_csv(path, dtype="string")
    return {
        int(row.geoname_id): (row.country_iso_code, row.country_name)
        for row in df.itertuples(index=False)
    }


def _calc_ip_range(cidr: str) -> tuple[str, str]:
    """Возвращает (start_ip, end_ip) для сети в CIDR‑нотации."""
    net = _ip.ip_network(cidr, strict=False)
    return str(net.network_address), str(net.broadcast_address)


# ────────────────────────────────────────────────────────────────────────────────
#  Core routine
# ────────────────────────────────────────────────────────────────────────────────

def merge(
    locations_csv: str | Path,
    blocks_csv: str | Path,
    timestamp: str,
) -> _pd.DataFrame:

    locations_map = _load_locations(Path(locations_csv))

    # Подавляем DtypeWarning (pandas генерирует его ДО обработки наших dtypes)
    warnings.filterwarnings("ignore", category=_pd.errors.DtypeWarning)

    # Читаем всё как строки, чтобы не споткнуться о пробелы и прочий мусор
    blocks = _pd.read_csv(
        blocks_csv,
        dtype="string",
        skipinitialspace=True,
        na_values=["", " ", "  "],
        low_memory=False,
    )

    int_cols = [
        "geoname_id",
        "registered_country_geoname_id",
        "represented_country_geoname_id",
        "is_anonymous_proxy",
        "is_satellite_provider",
        "is_anycast",
    ]

    for col in int_cols:
        if col in blocks.columns:
            blocks[col] = _pd.to_numeric(blocks[col], errors="coerce").astype("Int64")

    def _lookup_country(row):
        for key in (
            row.geoname_id,
            row.registered_country_geoname_id,
            row.represented_country_geoname_id,
        ):
            if not _pd.isna(key) and int(key) in locations_map:
                return locations_map[int(key)]
        return (None, None)

    blocks[["code", "name"]] = blocks.apply(
        lambda r: _pd.Series(_lookup_country(r)), axis=1
    )

    blocks[["start_ip", "end_ip"]] = blocks["network"].apply(
        lambda cidr: _pd.Series(_calc_ip_range(cidr))
    )

    blocks["_last_changed"] = timestamp

    result = blocks[
        ["_last_changed", "network", "start_ip", "end_ip", "code", "name"]
    ].fillna("").copy()

    return result


def _ask_timestamp() -> str:
    """Интерактивно спрашивает время выгрузки."""
    user_input = input(
        "Укажи время выгрузки в формате дд.мм.гггг чч:мм:сс "
        "[Enter — текущее]: "
    ).strip()
    return user_input


def _validate_timestamp(ts: str) -> str:
    """Проверяет формат и возвращает строку‑штамп."""
    if not ts:
        return _dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    try:
        _dt.datetime.strptime(ts, "%d.%m.%Y %H:%M:%S")
        return ts
    except ValueError as exc:
        print(f"✗ Неверный формат даты/времени: {exc}", file=sys.stderr)
        sys.exit(2)


def _cli():
    if len(sys.argv) not in (4, 5):
        print(
            "Usage: python merge_geoip.py <locations.csv> <blocks.csv> "
            "<output.csv> [дд.мм.гггг чч:мм:сс]",
            file=sys.stderr,
        )
        sys.exit(1)

    locations_csv, blocks_csv, output_csv = map(Path, sys.argv[1:4])

    timestamp_raw = sys.argv[4] if len(sys.argv) == 5 else _ask_timestamp()
    timestamp = _validate_timestamp(timestamp_raw)

    df = merge(locations_csv, blocks_csv, timestamp)
    df.to_csv(output_csv, index=False)
    print(f"✓ Готово. Итоговый файл сохранён в {output_csv}")


if __name__ == "__main__":
    _cli()
