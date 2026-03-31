import csv
from datetime import date
from pathlib import Path
import pandas as pd
from database import get_connection
from services_helper import (
    today_iso,
    parse_month_start,
    compute_charge_dt,
    compute_paycheck_dt
    )

def import_recurring_bills_from_csv(csv_path: str) -> int:
    path = Path(csv_path)
    if not path.exists():
        return 0

    conn = get_connection()
    cur = conn.cursor()

    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    row_count = 0
    now_dt = today_iso()

    for row in rows:
        bill_nm = str(row["bill_nm"]).strip()
        default_amt = float(row["default_amt"])
        charge_day = int(row["charge_day"])
        bill_source_nm = (row.get("bill_source_nm") or "joint_checking").strip()
        active_flg = int(row.get("active_flg", 1) or 1)
        notes = (row.get("notes") or "").strip() or None
        debt_flg = int(row.get("debt_flg", 0) or 0)
        bill_end_dt = (row.get("bill_end_dt") or "").strip() or None

        existing = cur.execute("""
            SELECT bill_id
            FROM recurring_bills_dim
            WHERE bill_nm = ?
        """, (bill_nm,)).fetchone()

        if existing:
            cur.execute("""
                UPDATE recurring_bills_dim
                SET default_amt = ?,
                    charge_day = ?,
                    bill_source_nm = ?,
                    active_flg = ?,
                    notes = ?,
                    debt_flg = ?,
                    bill_end_dt = ?,
                    update_dt = ?
                WHERE bill_nm = ?
            """, (
                default_amt,
                charge_day,
                bill_source_nm,
                active_flg,
                notes,
                debt_flg,
                bill_end_dt,
                now_dt,
                bill_nm
            ))
        else:
            cur.execute("""
                INSERT INTO recurring_bills_dim (
                    bill_nm,
                    default_amt,
                    charge_day,
                    bill_source_nm,
                    active_flg,
                    notes,
                    debt_flg,
                    bill_end_dt,
                    insert_dt,
                    update_dt
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bill_nm,
                default_amt,
                charge_day,
                bill_source_nm,
                active_flg,
                notes,
                debt_flg,
                bill_end_dt,
                now_dt,
                now_dt
            ))

        row_count += 1

    conn.commit()
    conn.close()
    return row_count


def generate_monthly_bills(month_start_dt: str) -> int:
    month_start = parse_month_start(month_start_dt)
    year = month_start.year
    month = month_start.month

    conn = get_connection()
    cur = conn.cursor()

    recurring_rows = cur.execute("""
        SELECT bill_id, bill_nm, default_amt, charge_day, bill_source_nm, bill_end_dt
        FROM recurring_bills_dim
        WHERE active_flg = 1
        ORDER BY bill_source_nm, charge_day, bill_nm
    """).fetchall()

    inserted_count = 0

    for row in recurring_rows:
        if row["bill_end_dt"] and row["bill_end_dt"] < month_start_dt:
            continue

        charge_dt = compute_charge_dt(year, month, row["charge_day"])
        paycheck_dt = compute_paycheck_dt(charge_dt)

        cur.execute("""
            INSERT OR IGNORE INTO monthly_bills_fact (
                bill_id,
                month_start_dt,
                bill_nm,
                bill_source_nm,
                budgeted_amt,
                actual_amt,
                charge_dt,
                paycheck_dt,
                funded_by_paycheck_flg,
                paid_flg,
                status
            )
            VALUES (?, ?, ?, ?, ?, NULL, ?, ?, 0, 0, 'planned')
        """, (
            row["bill_id"],
            month_start_dt,
            row["bill_nm"],
            row["bill_source_nm"],
            row["default_amt"],
            charge_dt.isoformat(),
            paycheck_dt.isoformat()
        ))

        if cur.rowcount > 0:
            inserted_count += 1

    conn.commit()
    conn.close()
    return inserted_count



def set_actual_amt_for_bill(month_start_dt: str, bill_nm: str, actual_amt: float) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE monthly_bills_fact
        SET actual_amt = ?,
            status = CASE WHEN paid_flg = 1 THEN 'paid' ELSE 'adjusted' END
        WHERE month_start_dt = ?
          AND bill_nm = ?
    """, (actual_amt, month_start_dt, bill_nm))

    conn.commit()
    conn.close()


def mark_bill_paid(month_start_dt: str, bill_nm: str) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE monthly_bills_fact
        SET paid_flg = 1,
            status = 'paid'
        WHERE month_start_dt = ?
          AND bill_nm = ?
    """, (month_start_dt, bill_nm))

    conn.commit()
    conn.close()


def get_monthly_bills_df(month_start_dt: str) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT
            monthly_bill_id,
            month_start_dt,
            bill_nm,
            bill_source_nm,
            budgeted_amt,
            actual_amt,
            charge_dt,
            paycheck_dt,
            funded_by_paycheck_flg,
            paid_flg,
            status
        FROM monthly_bills_fact
        WHERE month_start_dt = ?
        ORDER BY bill_source_nm, charge_dt, bill_nm
    """, conn, params=(month_start_dt,))
    conn.close()
    return df


def get_lookup_options() -> tuple[pd.DataFrame, pd.DataFrame]:
    conn = get_connection()

    paychecks_df = pd.read_sql_query("""
        SELECT paycheck_id, person_nm, paycheck_date
        FROM paychecks
        ORDER BY paycheck_date DESC, person_nm
    """, conn)

    savings_df = pd.read_sql_query("""
        SELECT savings_id, savings_name
        FROM savings_dim
        WHERE active_flg = 1
        ORDER BY savings_name
    """, conn)

    conn.close()
    return paychecks_df, savings_df


def get_upcoming_bills_until_next_paycheck_df(account_nm: str) -> pd.DataFrame:
    conn = get_connection()
    today = date.today().isoformat()

    next_paycheck_df = pd.read_sql_query("""
        SELECT MIN(paycheck_date) AS next_paycheck_dt
        FROM paychecks
        WHERE paycheck_date > ?
    """, conn, params=(today,))

    next_paycheck_dt = next_paycheck_df.iloc[0]["next_paycheck_dt"]

    if not next_paycheck_dt:
        conn.close()
        return pd.DataFrame()

    df = pd.read_sql_query("""
        SELECT
            bill_nm,
            bill_source_nm,
            charge_dt,
            paycheck_dt,
            budgeted_amt,
            actual_amt,
            COALESCE(actual_amt, budgeted_amt) AS effective_amt,
            paid_flg,
            status
        FROM monthly_bills_fact
        WHERE paid_flg = 0
          AND bill_source_nm = ?
          AND charge_dt >= ?
          AND charge_dt < ?
        ORDER BY bill_source_nm, charge_dt, bill_nm
    """, conn, params=(account_nm, today, next_paycheck_dt))

    conn.close()
    return df


