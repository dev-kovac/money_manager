import calendar
from datetime import date, datetime

def today_iso() -> str:
    return date.today().isoformat()


def parse_month_start(month_start_dt: str) -> date:
    return datetime.strptime(month_start_dt, "%Y-%m-%d").date()


def month_last_day(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def compute_charge_dt(year: int, month: int, charge_day: int) -> date:
    safe_day = min(max(int(charge_day), 1), month_last_day(year, month))
    return date(year, month, safe_day)


def previous_month_last_day(year: int, month: int) -> date:
    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1

    return date(year, month, month_last_day(year, month))


def compute_paycheck_dt(charge_dt: date) -> date:
    if charge_dt.day <= 15:
        return previous_month_last_day(charge_dt.year, charge_dt.month)
    else:
        return date(charge_dt.year, charge_dt.month, 15)
    

def parse_date(dt_str: str) -> date:
    return datetime.strptime(dt_str, "%Y-%m-%d").date()


def first_day_of_month(dt: date) -> date:
    return dt.replace(day=1)


def last_day_of_month(dt: date) -> date:
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(day=last_day)


def add_months(dt: date, months: int) -> date:
    year = dt.year + ((dt.month - 1 + months) // 12)
    month = ((dt.month - 1 + months) % 12) + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)

