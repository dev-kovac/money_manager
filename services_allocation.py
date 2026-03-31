from database import get_connection
from datetime import date
from services_helper import (
    parse_date,
    first_day_of_month,
    last_day_of_month,
    add_months
)


def get_next_paycheck_date(paycheck_dt: str) -> date:
    """
    Assumes pay schedule is the 15th and last day of month.
    """
    dt = parse_date(paycheck_dt)
    month_last = last_day_of_month(dt)

    if dt.day == 15:
        return month_last
    elif dt == month_last:
        next_month = add_months(dt.replace(day=1), 1)
        return next_month.replace(day=15)
    else:
        raise ValueError(
            f"Paycheck date {paycheck_dt} is not the 15th or the last day of the month."
        )


def get_coverage_month_for_paycheck(paycheck_dt: str) -> tuple[date, date]:
    """
    Rule:
    - paycheck on 15th funds current month
    - paycheck on last day of month funds next month
    """
    dt = parse_date(paycheck_dt)
    month_last = last_day_of_month(dt)

    if dt.day == 15:
        month_start = first_day_of_month(dt)
    elif dt == month_last:
        next_month = add_months(dt.replace(day=1), 1)
        month_start = first_day_of_month(next_month)
    else:
        raise ValueError(
            f"Paycheck date {paycheck_dt} is not the 15th or the last day of the month."
        )

    month_end = last_day_of_month(month_start)
    return month_start, month_end


def get_joint_bills_total_for_month(month_start: date, month_end: date) -> float:
    """
    Sums all upcoming joint bills in the coverage month.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COALESCE(SUM(COALESCE(actual_amt, budgeted_amt)), 0)
        FROM monthly_bills_fact
        WHERE bill_source_nm = 'joint_checking'
          AND charge_dt >= ?
          AND charge_dt <= ?
        """,
        (month_start.isoformat(), month_end.isoformat())
    )
    row = cur.fetchone()

    conn.close()
    return float(row[0] or 0.0)


def calculate_joint_transfer(person_nm: str, paycheck_dt: str) -> dict:
    """
    Calculates how much this person should move to joint checking
    from this paycheck using even monthly smoothing.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT person_nm, paycheck_date, net_income_amt
        FROM paychecks
        WHERE person_nm = ?
          AND paycheck_date = ?
        """,
        (person_nm, paycheck_dt)
    )
    paycheck_row = cur.fetchone()

    if not paycheck_row:
        raise ValueError(f"No paycheck found for {person_nm} on {paycheck_dt}")

    _, paycheck_dt_db, net_income_amt = paycheck_row

    month_start, month_end = get_coverage_month_for_paycheck(paycheck_dt_db)
    next_paycheck_dt = get_next_paycheck_date(paycheck_dt_db)

    total_joint_bills = get_joint_bills_total_for_month(month_start, month_end)

    # household total split across 2 people
    per_person_monthly_share = total_joint_bills / 2

    # each person's monthly share split across 2 paychecks
    recommended_joint_transfer = per_person_monthly_share / 2

    conn.close()
    return {
        "person_nm": person_nm,
        "paycheck_dt": paycheck_dt_db,
        "net_income_amt": float(net_income_amt or 0.0),
        "next_paycheck_dt": next_paycheck_dt.isoformat(),
        "coverage_month_start": month_start.isoformat(),
        "coverage_month_end": month_end.isoformat(),
        "total_joint_bills_for_month": round(total_joint_bills, 2),
        "per_person_monthly_share": round(per_person_monthly_share, 2),
        "recommended_joint_transfer": round(recommended_joint_transfer, 2),
        "remaining_after_joint_transfer": round(
            float(net_income_amt or 0.0) - recommended_joint_transfer, 2
        ),
    }

def get_people() -> list:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT person_nm
        FROM paychecks
        ORDER BY person_nm
    """)
    people = [row[0] for row in cur.fetchall()]

    conn.close()
    return people

def get_paycheck_from_person(selected_person: str) -> list:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT paycheck_date, net_income_amt
        FROM paychecks
        WHERE person_nm = ?
        ORDER BY paycheck_date DESC
        """,
        (selected_person,)
                )
    paycheck_rows = cur.fetchall()

    conn.close()
    return paycheck_rows