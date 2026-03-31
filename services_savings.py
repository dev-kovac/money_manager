from typing import Optional
import pandas as pd
from database import get_connection
from services_helper import today_iso


def insert_savings_goal(
    savings_name: str,
    target_amt: float,
    target_date: Optional[str] = None,
    priority_nbr: Optional[int] = None,
    active_flg: int = 1,
    category_nm: Optional[str] = None,
    goal_type_nm: Optional[str] = None,
    notes: Optional[str] = None
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO savings_dim (
            savings_name,
            target_amt,
            target_date,
            priority_nbr,
            active_flg,
            category_nm,
            goal_type_nm,
            notes,
            completed_flg,
            completed_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
        ON CONFLICT(savings_name) DO UPDATE SET
            target_amt = excluded.target_amt,
            target_date = excluded.target_date,
            priority_nbr = excluded.priority_nbr,
            active_flg = excluded.active_flg,
            category_nm = excluded.category_nm,
            goal_type_nm = excluded.goal_type_nm, 
            notes = excluded.notes
    """, (
        savings_name,
        target_amt,
        target_date,
        priority_nbr,
        active_flg,
        category_nm,
        goal_type_nm,
        notes
    ))

    conn.commit()
    conn.close()


# def set_actual_amt_for_bill(month_start_dt: str, bill_nm: str, actual_amt: float) -> None:
#     conn = get_connection()
#     cur = conn.cursor()

#     cur.execute("""
#         UPDATE monthly_bills_fact
#         SET actual_amt = ?,
#             status = CASE WHEN paid_flg = 1 THEN 'paid' ELSE 'adjusted' END
#         WHERE month_start_dt = ?
#           AND bill_nm = ?
#     """, (actual_amt, month_start_dt, bill_nm))

#     conn.commit()
#     conn.close()




def mark_completed_savings_goals() -> int:
    conn = get_connection()
    cur = conn.cursor()

    goal_rows = cur.execute("""
        SELECT
            s.savings_id,
            s.target_amt,
            COALESCE(SUM(a.alloc_amt), 0) AS total_saved_amt
        FROM savings_dim s
        LEFT JOIN paycheck_savings_alloc a
            ON s.savings_id = a.savings_id
        WHERE s.completed_flg = 0
        GROUP BY s.savings_id, s.target_amt
    """).fetchall()

    completed_count = 0
    today = today_iso()

    for row in goal_rows:
        if (row["total_saved_amt"] or 0) >= row["target_amt"]:
            cur.execute("""
                UPDATE savings_dim
                SET completed_flg = 1,
                    completed_date = ?
                WHERE savings_id = ?
            """, (today, row["savings_id"]))
            completed_count += 1

    conn.commit()
    conn.close()
    return completed_count


def get_savings_progress_df() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT
            s.savings_id,
            s.savings_name,
            s.target_amt,
            s.target_date,
            s.priority_nbr,
            s.active_flg,
            s.category_nm,
            s.goal_type_nm,
            s.completed_flg,
            s.completed_date,
            COALESCE(SUM(a.alloc_amt), 0) AS total_saved_amt,
            s.target_amt - COALESCE(SUM(a.alloc_amt), 0) AS remaining_amt
        FROM savings_dim s
        LEFT JOIN paycheck_savings_alloc a
            ON s.savings_id = a.savings_id
        GROUP BY
            s.savings_id,
            s.savings_name,
            s.target_amt,
            s.target_date,
            s.priority_nbr,
            s.active_flg,
            s.category_nm,
            s.goal_type_nm,
            s.completed_flg,
            s.completed_date
        ORDER BY s.completed_flg, s.priority_nbr, s.savings_name
    """, conn)
    conn.close()
    return df



