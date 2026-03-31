from typing import Optional
import pandas as pd
from database import get_connection


def upsert_paycheck(
    person_nm: str,
    paycheck_date: str,
    net_income_amt: Optional[float] = None,
    joint_contribution_amt: Optional[float] = None,
    allowance_amt: Optional[float] = None,
    personal_bills_amt: Optional[float] = None,
    savings_contribution_amt: Optional[float] = None,
    other_obligations_amt: Optional[float] = None,
    notes: Optional[str] = None
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO paychecks (
            person_nm,
            paycheck_date,
            net_income_amt,
            joint_contribution_amt,
            allowance_amt,
            personal_bills_amt,
            savings_contribution_amt,
            other_obligations_amt,
            notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(person_nm, paycheck_date) DO UPDATE SET
            net_income_amt = excluded.net_income_amt,
            joint_contribution_amt = excluded.joint_contribution_amt,
            allowance_amt = excluded.allowance_amt,
            personal_bills_amt = excluded.personal_bills_amt,
            savings_contribution_amt = excluded.savings_contribution_amt,
            other_obligations_amt = excluded.other_obligations_amt,
            notes = excluded.notes
    """, (
        person_nm,
        paycheck_date,
        net_income_amt,
        joint_contribution_amt,
        allowance_amt,
        personal_bills_amt,
        savings_contribution_amt,
        other_obligations_amt,
        notes
    ))

    conn.commit()
    conn.close()



def get_paychecks_df() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT
            paycheck_id,
            person_nm,
            paycheck_date,
            net_income_amt,
            joint_contribution_amt,
            allowance_amt,
            personal_bills_amt,
            savings_contribution_amt,
            other_obligations_amt,
            notes,
            (
                COALESCE(net_income_amt, 0)
                - COALESCE(joint_contribution_amt, 0)
                - COALESCE(allowance_amt, 0)
                - COALESCE(personal_bills_amt, 0)
                - COALESCE(savings_contribution_amt, 0)
                - COALESCE(other_obligations_amt, 0)
            ) AS remaining_amt,
            case when paycheck_date >= date('now') then 'planned' else 'done' end as planned_or_done
        FROM paychecks
        ORDER BY paycheck_date, person_nm
    """, conn)
    conn.close()
    return df



# def allocate_paycheck_to_savings(
#     paycheck_id: int,
#     savings_id: int,
#     alloc_amt: float,
#     alloc_dt: Optional[str] = None,
#     notes: Optional[str] = None
# ) -> None:
#     conn = get_connection()
#     cur = conn.cursor()

#     if alloc_dt is None:
#         row = cur.execute("""
#             SELECT paycheck_date
#             FROM paychecks
#             WHERE paycheck_id = ?
#         """, (paycheck_id,)).fetchone()

#         if not row:
#             conn.close()
#             raise ValueError(f"paycheck_id {paycheck_id} not found")

#         alloc_dt = row["paycheck_date"]

#     cur.execute("""
#         INSERT INTO paycheck_savings_alloc (
#             paycheck_id,
#             savings_id,
#             alloc_amt,
#             alloc_dt,
#             notes
#         )
#         VALUES (?, ?, ?, ?, ?)
#     """, (
#         paycheck_id,
#         savings_id,
#         alloc_amt,
#         alloc_dt,
#         notes
#     ))

#     conn.commit()
#     conn.close()
