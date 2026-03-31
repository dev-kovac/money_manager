from datetime import date
from typing import Optional
import pandas as pd
from database import get_connection

def calculate_account_funding_status(account_nm: str) -> None:
    conn = get_connection()
    cur = conn.cursor()

    paycheck_dates = cur.execute("""
        SELECT DISTINCT paycheck_dt
        FROM monthly_bills_fact
        WHERE bill_source_nm = ?
        ORDER BY paycheck_dt
    """, (account_nm,)).fetchall()

    for row in paycheck_dates:
        paycheck_dt = row["paycheck_dt"]

        bills_total = cur.execute("""
            SELECT COALESCE(SUM(COALESCE(actual_amt, budgeted_amt)), 0) AS bills_total
            FROM monthly_bills_fact
            WHERE paycheck_dt = ?
              AND bill_source_nm = ?
        """, (paycheck_dt,account_nm,)).fetchone()["bills_total"]

        if account_nm == "joint_checking":
            contribution_amt_col = "joint_contribution_amt"
            person_nm = False
        if account_nm == "devin_checking":
            contribution_amt_col = "personal_bills_amt"
            person_nm = "Devin"
        if account_nm == "rieanna_checking":
            contribution_amt_col = "personal_bills_amt"
            person_nm = "Rieanna"

        if not person_nm:
            query = f"""
                SELECT COALESCE(SUM(COALESCE({contribution_amt_col}, 0)), 0) AS contribution_total
                FROM paychecks
                WHERE paycheck_date = ?
            """
            joint_total = cur.execute(query, (paycheck_dt,)).fetchone()["contribution_total"]
        else:
            query = f"""
                SELECT COALESCE(SUM(COALESCE({contribution_amt_col}, 0)), 0) AS contribution_total
                FROM paychecks
                WHERE paycheck_date = ?
                AND person_nm = ?
            """
            joint_total = cur.execute(query, (paycheck_dt,person_nm,)).fetchone()["contribution_total"]
             

        funded_flag = 1 if joint_total >= bills_total and bills_total > 0 else 0

        cur.execute("""
            UPDATE monthly_bills_fact
            SET funded_by_paycheck_flg = ?
            WHERE paycheck_dt = ?
              AND bill_source_nm = ?
        """, (funded_flag, paycheck_dt, account_nm,))

    conn.commit()
    conn.close()


def get_account_funding_summary_df(account_nm: str) -> pd.DataFrame:
    conn = get_connection()

    if account_nm == "joint_checking":
            contribution_amt_col = "p.joint_contribution_amt"
            person_nm = False
    if account_nm == "devin_checking":
            contribution_amt_col = "p.personal_bills_amt"
            person_nm = "Devin"
    if account_nm == "rieanna_checking":
            contribution_amt_col = "p.personal_bills_amt"
            person_nm = "Rieanna"

    if not person_nm:
        query = f"""
            SELECT
                mbf.paycheck_dt,
                COALESCE(SUM(COALESCE(mbf.actual_amt, mbf.budgeted_amt)), 0) AS bills_total,
                (
                    SELECT COALESCE(SUM(COALESCE({contribution_amt_col}, 0)), 0)
                    FROM paychecks p
                    WHERE p.paycheck_date = mbf.paycheck_dt
                ) AS contribution_total
            FROM monthly_bills_fact mbf
            WHERE mbf.bill_source_nm = ?
            GROUP BY mbf.paycheck_dt
            ORDER BY mbf.paycheck_dt
        """
        df = pd.read_sql_query(query, conn, params=(account_nm,))
        conn.close()
    else:
        query = f"""
            SELECT
                mbf.paycheck_dt,
                COALESCE(SUM(COALESCE(mbf.actual_amt, mbf.budgeted_amt)), 0) AS bills_total,
                (
                    SELECT COALESCE(SUM(COALESCE({contribution_amt_col}, 0)), 0)
                    FROM paychecks p
                    WHERE p.paycheck_date = mbf.paycheck_dt
                    AND p.person_nm = ?
                ) AS contribution_total
            FROM monthly_bills_fact mbf
            WHERE mbf.bill_source_nm = ?
            GROUP BY mbf.paycheck_dt
            ORDER BY mbf.paycheck_dt
        """
        df = pd.read_sql_query(query, conn, params=(person_nm,account_nm,))
        conn.close()

    if not df.empty:
        df["difference"] = df["contribution_total"] - df["bills_total"]
        df["funded"] = df["contribution_total"] >= df["bills_total"]

    return df



def save_account_balance(
    account_nm: str,
    balance_amt: float,
    balance_dt: str,
    notes: Optional[str] = None,
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO account_balances (
            account_nm,
            balance_amt,
            balance_dt,
            notes
        )
        VALUES (?, ?, ?, ?)
    """, (account_nm, balance_amt, balance_dt, notes))

    conn.commit()
    conn.close()


def get_latest_account_balance(account_nm: str) -> Optional[dict]:
    conn = get_connection()
    cur = conn.cursor()

    row = cur.execute("""
        SELECT
            account_nm,
            balance_amt,
            balance_dt,
            notes
        FROM account_balances
        WHERE account_nm = ?
        ORDER BY balance_dt DESC, account_balance_id DESC
        LIMIT 1
    """, (account_nm,)).fetchone()

    conn.close()

    if not row:
        return None

    return dict(row)


def get_account_cash_coverage(account_nm: str = "joint_checking") -> dict:
    conn = get_connection()
    cur = conn.cursor()

    today = date.today().isoformat()

    balance_row = cur.execute("""
        SELECT
            balance_amt,
            balance_dt
        FROM account_balances
        WHERE account_nm = ?
        ORDER BY balance_dt DESC, account_balance_id DESC
        LIMIT 1
    """, (account_nm,)).fetchone()

    if not balance_row:
        conn.close()
        return {
            "has_balance": False,
            "current_balance": 0.0,
            "next_paycheck_dt": None,
            "upcoming_bills_total": 0.0,
            "remaining_after_bills": 0.0,
            "covered_flg": False,
        }

    current_balance = balance_row["balance_amt"]

    next_paycheck_row = cur.execute("""
        SELECT MIN(paycheck_date) AS next_paycheck_dt
        FROM paychecks
        WHERE paycheck_date > ?
    """, (today,)).fetchone()

    next_paycheck_dt = next_paycheck_row["next_paycheck_dt"]

    if not next_paycheck_dt:
        conn.close()
        return {
            "has_balance": True,
            "current_balance": current_balance,
            "next_paycheck_dt": None,
            "upcoming_bills_total": 0.0,
            "remaining_after_bills": current_balance,
            "covered_flg": True,
        }

    upcoming_bills_total = cur.execute("""
        SELECT COALESCE(SUM(COALESCE(actual_amt, budgeted_amt)), 0) AS total
        FROM monthly_bills_fact
        WHERE paid_flg = 0
          AND bill_source_nm = ?
          AND charge_dt >= ?
          AND charge_dt < ?
    """, (account_nm, today, next_paycheck_dt)).fetchone()["total"]

    conn.close()

    remaining_after_bills = current_balance - upcoming_bills_total

    return {
        "has_balance": True,
        "current_balance": current_balance,
        "next_paycheck_dt": next_paycheck_dt,
        "upcoming_bills_total": upcoming_bills_total,
        "remaining_after_bills": remaining_after_bills,
        "covered_flg": remaining_after_bills >= 0,
    }

