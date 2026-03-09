from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from contextlib import closing
from datetime import datetime
from pathlib import Path

import pandas as pd


SCHEMA_FIELDS = [
    "stat_date",
    "parking_lot",
    "total_revenue",
    "temp_revenue",
    "monthly_revenue",
    "entry_count",
    "payment_failure_rate",
    "abnormal_open_count",
    "free_release_count",
    "occupancy_rate",
]

PARKING_LOT_ALIASES = {
    "高林社区商业中心": "厦门高林居住区高林一里 A1-1地块商业中心",
}


def build_daily_ops_rows(
    capacities: dict[str, int],
    payment_rows: list[dict],
    passage_rows: list[dict],
) -> list[dict]:
    payment_stats = _aggregate_payment_rows(payment_rows)
    passage_stats = _aggregate_passage_rows(passage_rows, capacities)

    all_keys = sorted(set(payment_stats) | set(passage_stats))
    rows = []
    for stat_date, parking_lot in all_keys:
        payment = payment_stats.get((stat_date, parking_lot), {})
        passage = passage_stats.get((stat_date, parking_lot), {})
        rows.append(
            {
                "stat_date": stat_date,
                "parking_lot": parking_lot,
                "total_revenue": round(float(payment.get("total_revenue", 0.0)), 2),
                "temp_revenue": round(float(payment.get("temp_revenue", 0.0)), 2),
                "monthly_revenue": 0.0,
                "entry_count": int(passage.get("entry_count", 0)),
                "payment_failure_rate": round(float(payment.get("payment_failure_rate", 0.0)), 4),
                "abnormal_open_count": 0,
                "free_release_count": int(passage.get("free_release_count", 0)),
                "occupancy_rate": round(float(passage.get("occupancy_rate", 0.0)), 4),
            }
        )
    return rows


def build_daily_ops_rows_from_excel(base_dir: Path) -> list[dict]:
    capacities = load_capacity_map(base_dir / "车场基础数据.xlsx")
    payment_rows = load_payment_rows(base_dir / "流水数据")
    passage_rows = load_passage_rows(base_dir / "通行数据")
    return build_daily_ops_rows(
        capacities=capacities,
        payment_rows=payment_rows,
        passage_rows=passage_rows,
    )


def build_sqlite_database(
    capacities: dict[str, int],
    payment_rows: list[dict],
    passage_rows: list[dict],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(output_path)) as conn:
        _create_sqlite_schema(conn)
        lot_ids = _insert_parking_lots(conn, capacities, payment_rows, passage_rows)
        _insert_payment_records(conn, lot_ids, payment_rows)
        _insert_passage_records(conn, lot_ids, passage_rows)
        _create_sqlite_indexes(conn)
        conn.commit()


def build_sqlite_database_from_excel(base_dir: Path, output_path: Path) -> None:
    capacities = load_capacity_map(base_dir / "车场基础数据.xlsx")
    payment_rows = load_payment_rows(base_dir / "流水数据")
    passage_rows = load_passage_rows(base_dir / "通行数据")
    build_sqlite_database(
        capacities=capacities,
        payment_rows=payment_rows,
        passage_rows=passage_rows,
        output_path=output_path,
    )


def write_daily_ops_csv(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SCHEMA_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def load_capacity_map(source_path: Path) -> dict[str, int]:
    df = pd.read_excel(source_path)
    capacities = {}
    for record in df.to_dict(orient="records"):
        lot = normalize_parking_lot_name(record.get("车场名称"))
        if not lot:
            continue
        capacities[lot] = int(record.get("总车位") or 0)
    return capacities


def load_payment_rows(source_dir: Path) -> list[dict]:
    records: list[dict] = []
    for source_path in sorted(source_dir.glob("*.xlsx")):
        df = pd.read_excel(source_path)
        df = df.fillna("")
        records.extend(df.to_dict(orient="records"))
    return records


def load_passage_rows(source_dir: Path) -> list[dict]:
    records: list[dict] = []
    for source_path in sorted(source_dir.glob("*.xlsx")):
        xls = pd.ExcelFile(source_path)
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(source_path, sheet_name=sheet_name)
            if df.dropna(how="all").empty:
                continue
            df = df.fillna("")
            records.extend(df.to_dict(orient="records"))
    return records


def normalize_parking_lot_name(value) -> str:
    normalized = str(value).strip()
    return PARKING_LOT_ALIASES.get(normalized, normalized)


def _create_sqlite_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS parking_payment_records;
        DROP TABLE IF EXISTS parking_passage_records;
        DROP TABLE IF EXISTS parking_lots;

        CREATE TABLE parking_lots (
            lot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            parking_lot_name TEXT NOT NULL UNIQUE,
            total_spaces INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE parking_payment_records (
            payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            lot_id INTEGER NOT NULL,
            initiated_at TEXT,
            paid_at TEXT,
            license_plate TEXT,
            entry_at TEXT,
            receivable_amount REAL NOT NULL DEFAULT 0,
            actual_amount REAL NOT NULL DEFAULT 0,
            payment_result TEXT,
            payment_method TEXT,
            refund_amount REAL NOT NULL DEFAULT 0,
            payment_source TEXT,
            invoice_flag TEXT,
            FOREIGN KEY(lot_id) REFERENCES parking_lots(lot_id)
        );

        CREATE TABLE parking_passage_records (
            passage_id INTEGER PRIMARY KEY AUTOINCREMENT,
            lot_id INTEGER NOT NULL,
            license_plate TEXT,
            vehicle_type TEXT,
            entry_at TEXT,
            entry_gate TEXT,
            exit_at TEXT,
            exit_gate TEXT,
            stay_minutes REAL NOT NULL DEFAULT 0,
            receivable_amount REAL NOT NULL DEFAULT 0,
            actual_amount REAL NOT NULL DEFAULT 0,
            notes TEXT,
            FOREIGN KEY(lot_id) REFERENCES parking_lots(lot_id)
        );
        """
    )


def _insert_parking_lots(
    conn: sqlite3.Connection,
    capacities: dict[str, int],
    payment_rows: list[dict],
    passage_rows: list[dict],
) -> dict[str, int]:
    lot_names = set(capacities)
    lot_names.update(
        normalize_parking_lot_name(row.get("停车场名称"))
        for row in payment_rows
        if normalize_parking_lot_name(row.get("停车场名称"))
    )
    lot_names.update(
        normalize_parking_lot_name(row.get("停车场名称"))
        for row in passage_rows
        if normalize_parking_lot_name(row.get("停车场名称"))
    )
    for lot_name in sorted(lot_names):
        conn.execute(
            "INSERT INTO parking_lots (parking_lot_name, total_spaces) VALUES (?, ?)",
            (lot_name, int(capacities.get(lot_name, 0) or 0)),
        )
    rows = conn.execute("SELECT lot_id, parking_lot_name FROM parking_lots").fetchall()
    return {name: lot_id for lot_id, name in rows}


def _insert_payment_records(conn: sqlite3.Connection, lot_ids: dict[str, int], payment_rows: list[dict]) -> None:
    payload = []
    for row in payment_rows:
        lot_name = normalize_parking_lot_name(row.get("停车场名称"))
        if lot_name not in lot_ids:
            continue
        payload.append(
            (
                lot_ids[lot_name],
                _coerce_datetime(row.get("发起支付时间")),
                _coerce_datetime(row.get("支付时间")),
                str(row.get("车牌号", "")).strip(),
                _coerce_datetime(row.get("入场时间")),
                _coerce_float(row.get("应收(元)")),
                _coerce_float(row.get("实收(元)")),
                str(row.get("收费结果", "")).strip(),
                str(row.get("支付方式", "")).strip(),
                _coerce_float(row.get("退款金额(元)")),
                str(row.get("缴费来源", "")).strip(),
                str(row.get("是否开票", "")).strip(),
            )
        )
    conn.executemany(
        """
        INSERT INTO parking_payment_records (
            lot_id, initiated_at, paid_at, license_plate, entry_at,
            receivable_amount, actual_amount, payment_result, payment_method,
            refund_amount, payment_source, invoice_flag
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )


def _insert_passage_records(conn: sqlite3.Connection, lot_ids: dict[str, int], passage_rows: list[dict]) -> None:
    payload = []
    for row in passage_rows:
        lot_name = normalize_parking_lot_name(row.get("停车场名称"))
        if lot_name not in lot_ids:
            continue
        payload.append(
            (
                lot_ids[lot_name],
                str(row.get("车牌号", "")).strip(),
                str(row.get("车辆类型", "")).strip(),
                _coerce_datetime(row.get("入场时间")),
                str(row.get("入场门道", "")).strip(),
                _coerce_datetime(row.get("出场时间")),
                str(row.get("出场门道", "")).strip(),
                _coerce_float(row.get("停留时间（分）")),
                _coerce_float(row.get("应收（元）")),
                _coerce_float(row.get("实收（元）")),
                str(row.get("备注", "")).strip(),
            )
        )
    conn.executemany(
        """
        INSERT INTO parking_passage_records (
            lot_id, license_plate, vehicle_type, entry_at, entry_gate, exit_at,
            exit_gate, stay_minutes, receivable_amount, actual_amount, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )


def _create_sqlite_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX idx_payment_lot_paid_at ON parking_payment_records(lot_id, paid_at);
        CREATE INDEX idx_passage_lot_entry_at ON parking_passage_records(lot_id, entry_at);
        """
    )


def _aggregate_payment_rows(rows: list[dict]) -> dict[tuple[str, str], dict]:
    grouped: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"payment_record_count": 0, "payment_failure_count": 0, "total_revenue": 0.0, "temp_revenue": 0.0}
    )
    for row in rows:
        lot = normalize_parking_lot_name(row.get("停车场名称"))
        stat_date = _coerce_date(row.get("支付时间") or row.get("发起支付时间"))
        if not lot or not stat_date:
            continue

        bucket = grouped[(stat_date, lot)]
        bucket["payment_record_count"] += 1
        actual_received = _coerce_float(row.get("实收(元)"))
        bucket["total_revenue"] += actual_received
        bucket["temp_revenue"] += actual_received

        result_text = str(row.get("收费结果", "")).strip()
        if _is_payment_failure(result_text):
            bucket["payment_failure_count"] += 1

    return {
        key: {
            "total_revenue": round(value["total_revenue"], 2),
            "temp_revenue": round(value["temp_revenue"], 2),
            "payment_failure_rate": (
                value["payment_failure_count"] / value["payment_record_count"]
                if value["payment_record_count"]
                else 0.0
            ),
        }
        for key, value in grouped.items()
    }


def _aggregate_passage_rows(rows: list[dict], capacities: dict[str, int]) -> dict[tuple[str, str], dict]:
    grouped: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"entry_count": 0, "occupied_minutes": 0.0, "free_release_count": 0}
    )
    for row in rows:
        lot = normalize_parking_lot_name(row.get("停车场名称"))
        stat_date = _coerce_date(row.get("入场时间"))
        if not lot or not stat_date:
            continue

        bucket = grouped[(stat_date, lot)]
        bucket["entry_count"] += 1
        stay_minutes = min(max(_coerce_float(row.get("停留时间（分）")), 0.0), 1440.0)
        bucket["occupied_minutes"] += stay_minutes

        vehicle_type = str(row.get("车辆类型", "")).strip()
        receivable = _coerce_float(row.get("应收（元）"))
        actual = _coerce_float(row.get("实收（元）"))
        if vehicle_type == "临时车" and receivable == 0 and actual == 0:
            bucket["free_release_count"] += 1

    result: dict[tuple[str, str], dict] = {}
    for key, value in grouped.items():
        lot = key[1]
        capacity = max(capacities.get(lot, 0), 1)
        occupancy_rate = min(value["occupied_minutes"] / (capacity * 1440.0), 1.0)
        result[key] = {
            "entry_count": value["entry_count"],
            "free_release_count": value["free_release_count"],
            "occupancy_rate": occupancy_rate,
        }
    return result


def _is_payment_failure(result_text: str) -> bool:
    if not result_text:
        return False
    if "失败" in result_text:
        return True
    return result_text in {"支付成功,通知车场失败"}


def _coerce_date(value) -> str | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return pd.to_datetime(text).date().isoformat()


def _coerce_float(value) -> float:
    if value in {None, ""}:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(str(value).replace(",", "").strip() or 0.0)


def _coerce_datetime(value) -> str | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    parsed = pd.to_datetime(text).to_pydatetime()
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    parser = argparse.ArgumentParser(description="将 5 个停车场 Excel 明细转换为停车经营 CSV 和 SQLite 数据库")
    parser.add_argument(
        "--input-dir",
        default="data/5个停车场数据",
        help="输入目录，默认 data/5个停车场数据",
    )
    parser.add_argument(
        "--output",
        default="data/sample_parking_ops.csv",
        help="输出 CSV 路径，默认 data/sample_parking_ops.csv",
    )
    parser.add_argument(
        "--sqlite-output",
        default="data/sample_parking_ops.db",
        help="输出 SQLite 路径，默认 data/sample_parking_ops.db",
    )
    args = parser.parse_args()

    rows = build_daily_ops_rows_from_excel(Path(args.input_dir))
    write_daily_ops_csv(rows, Path(args.output))
    build_sqlite_database_from_excel(Path(args.input_dir), Path(args.sqlite_output))
    print(f"已生成 {args.output}（{len(rows)} 行）和 {args.sqlite_output}。")


if __name__ == "__main__":
    main()
