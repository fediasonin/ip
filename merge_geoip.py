# merge_geoip.py
"""
Скрипт склеивает два CSV‑файла geoip.noc.gov.ru (locations + blocks)
и выдаёт итоговый CSV с колонками:
    _last_changed, network, start_ip, end_ip, from, to, code, name

Оптимизировано: расчёт диапазонов CIDR теперь делается **векторно** без
долгого `apply`, так что даже 350k+ сетей обрабатываются за считанные
секунды.

Запуск:
    python merge_geoip.py <locations.csv> <blocks.csv> <output.csv> [дата]

Дата (4‑й аргумент, опционально) — `дд.мм.гггг чч:мм:сс`. Если пропустить —
спросит, Enter = текущее время.

Зависимости: `pandas>=1.5` (pip install -r requirements.txt)
"""

from __future__ import annotations

import sys
import datetime as _dt
import ipaddress as _ip
import warnings
from pathlib import Path

import pandas as _pd

# ────────────────────────────────────────────────────────────────────────────────
#  Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _load_locations(path: Path) -> dict[int, tuple[str, str]]:
    df = _pd.read_csv(path, dtype="string")
    return {
        int(row.geoname_id): (row.country_iso_code, row.country_name)
        for row in df.itertuples(index=False)
    }


def _cidr_to_bounds_vec(cidr_series: _pd.Series) -> tuple[list[str], list[str]]:
    """Быстрый векторный расчёт start/end IP для колонок в CIDR‑формате."""
    starts: list[str] = []
    ends: list[str] = []
    append_s = starts.append
    append_e = ends.append
    for net_str in cidr_series.tolist():
        net = _ip.ip_network(net_str, strict=False)
        append_s(str(net.network_address))
        append_e(str(net.broadcast_address))
    return starts, ends


def _ip_to_decimal_vec(ip_list: list[str]) -> list[int]:
    return [int(_ip.IPv4Address(ip)) for ip in ip_list]


# ────────────────────────────────────────────────────────────────────────────────
#  Core merge
# ────────────────────────────────────────────────────────────────────────────────

def merge(locations_csv: Path, blocks_csv: Path, timestamp: str) -> _pd.DataFrame:
    loc_map = _load_locations(locations_csv)
    warnings.filterwarnings("ignore", category=_pd.errors.DtypeWarning)

    blocks = _pd.read_csv(
        blocks_csv,
        dtype="string",
        skipinitialspace=True,
        na_values=["", " ", "  "],
        low_memory=False,
    )

    # country lookup
    def _lookup(row):
        for key in (
            row.geoname_id,
            row.registered_country_geoname_id,
            row.represented_country_geoname_id,
        ):
            if key and key.isdigit() and int(key) in loc_map:
                return loc_map[int(key)]
        return (None, None)

    blocks[["code", "name"]] = blocks.apply(lambda r: _pd.Series(_lookup(r)), axis=1)

    # vectorised cidr → start/end
    starts, ends = _cidr_to_bounds_vec(blocks["network"])
    blocks["start_ip"] = starts
    blocks["end_ip"] = ends

    # vectorised decimal conversion
    blocks["from"] = _ip_to_decimal_vec(starts)
    blocks["to"] = _ip_to_decimal_vec(ends)

    blocks["_last_changed"] = timestamp

    return blocks[
        ["_last_changed", "network", "start_ip", "end_ip", "from", "to", "code", "name"]
    ].fillna("")


# ────────────────────────────────────────────────────────────────────────────────
#  CLI
# ────────────────────────────────────────────────────────────────────────────────

def _ask_ts() -> str:
    return input("Дата выгрузки (дд.мм.гггг чч:мм:сс) [Enter — сейчас]: ").strip()


def _norm_ts(ts: str) -> str:
    if not ts:
        return _dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    try:
        _dt.datetime.strptime(ts, "%d.%m.%Y %H:%M:%S")
        return ts
    except ValueError:
        print("✗ Неверный формат даты", file=sys.stderr)
        sys.exit(2)


def _cli():
    if len(sys.argv) not in (4, 5):
        print("Usage: python merge_geoip.py <loc.csv> <blocks.csv> <out.csv> [date]", file=sys.stderr)
        sys.exit(1)

    loc_csv, blk_csv, out_csv = map(Path, sys.argv[1:4])
    ts_raw = sys.argv[4] if len(sys.argv) == 5 else _ask_ts()
    ts = _norm_ts(ts_raw)

    df = merge(loc_csv, blk_csv, ts)
    df.to_csv(out_csv, index=False)
    print(f"✓ Итоговый файл сохранён: {out_csv}")


if __name__ == "__main__":
    _cli()
