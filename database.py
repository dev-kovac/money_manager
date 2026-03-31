import sqlite3

DB_PATH = "joint_finance.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS recurring_bills_dim (
        bill_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bill_nm TEXT NOT NULL,
        default_amt REAL NOT NULL,
        charge_day INTEGER NOT NULL,
        bill_source_nm TEXT NOT NULL DEFAULT 'joint_checking',
        active_flg INTEGER NOT NULL DEFAULT 1,
        debt_flg INTEGER NOT NULL DEFAULT 0,
        notes TEXT,
        bill_end_dt TEXT,
        insert_dt TEXT,
        update_dt TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS paychecks (
        paycheck_id INTEGER PRIMARY KEY AUTOINCREMENT,
        person_nm TEXT NOT NULL,
        paycheck_date TEXT NOT NULL,
        net_income_amt REAL,
        joint_contribution_amt REAL,
        allowance_amt REAL,
        personal_bills_amt REAL,
        savings_contribution_amt REAL,
        other_obligations_amt REAL,
        notes TEXT,
        UNIQUE(person_nm, paycheck_date)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS savings_dim (
        savings_id INTEGER PRIMARY KEY AUTOINCREMENT,
        savings_name TEXT NOT NULL,
        target_amt REAL NOT NULL,
        target_date TEXT,
        priority_nbr INTEGER,
        active_flg INTEGER NOT NULL DEFAULT 1,
        category_nm TEXT,
        goal_type_nm TEXT,
        notes TEXT,
        completed_flg INTEGER NOT NULL DEFAULT 0,
        completed_date TEXT,
        UNIQUE(savings_name)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS debt_dim (
        debt_id INTEGER PRIMARY KEY AUTOINCREMENT,
        debt_nm TEXT NOT NULL,
        person_nm TEXT NOT NULL,
        debt_amt REAL NOT NULL,                
        priority_nbr INTEGER,
        active_flg INTEGER NOT NULL DEFAULT 1,
        UNIQUE(debt_nm)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS paycheck_savings_alloc (
        paycheck_savings_alloc_id INTEGER PRIMARY KEY AUTOINCREMENT,
        paycheck_id INTEGER NOT NULL,
        savings_id INTEGER NOT NULL,
        alloc_amt REAL NOT NULL,
        alloc_dt TEXT,
        notes TEXT,
        FOREIGN KEY (paycheck_id) REFERENCES paychecks(paycheck_id),
        FOREIGN KEY (savings_id) REFERENCES savings_dim(savings_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS monthly_bills_fact (
        monthly_bill_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bill_id INTEGER NOT NULL,
        month_start_dt TEXT NOT NULL,
        bill_nm TEXT NOT NULL,
        bill_source_nm TEXT NOT NULL DEFAULT 'joint_checking',
        budgeted_amt REAL NOT NULL,
        actual_amt REAL,
        charge_dt TEXT NOT NULL,
        paycheck_dt TEXT NOT NULL,
        funded_by_paycheck_flg INTEGER NOT NULL DEFAULT 0,
        paid_flg INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'planned',
        FOREIGN KEY (bill_id) REFERENCES recurring_bills_dim(bill_id),
        UNIQUE(month_start_dt, bill_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS account_balances (
        account_balance_id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_nm TEXT NOT NULL,
        balance_amt REAL NOT NULL,
        balance_dt TEXT NOT NULL,
        notes TEXT
    )
    """)

    conn.commit()
    conn.close()

def run_migrations() -> None:
    conn = get_connection()
    cur = conn.cursor()

    # cur.execute("""
    # update monthly_bills_fact
    # set bill_source_nm = 'devin_checking'
    # where bill_source_nm = 'devin_personal'
    
    # """)

    recurring_cols = [row["name"] for row in cur.execute("PRAGMA table_info(recurring_bills_dim)").fetchall()]
    if "bill_source_nm" not in recurring_cols:
        cur.execute("""
            ALTER TABLE recurring_bills_dim
            ADD COLUMN bill_source_nm TEXT NOT NULL DEFAULT 'joint_checking'
        """)

    monthly_cols = [row["name"] for row in cur.execute("PRAGMA table_info(monthly_bills_fact)").fetchall()]
    if "bill_source_nm" not in monthly_cols:
        cur.execute("""
            ALTER TABLE monthly_bills_fact
            ADD COLUMN bill_source_nm TEXT NOT NULL DEFAULT 'joint_checking'
        """)

    debt_cols = [row["name"] for row in cur.execute("PRAGMA table_info(recurring_bills_dim)").fetchall()]
    if "debt_flg" not in debt_cols:
        cur.execute("""
            ALTER TABLE recurring_bills_dim
            ADD COLUMN debt_flg INTEGER NOT NULL DEFAULT 0
        """)

    conn.commit()
    conn.close()