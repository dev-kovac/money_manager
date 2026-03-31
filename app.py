from datetime import date
import streamlit as st
import pandas as pd
from database import init_db, run_migrations
from services_savings import (
    insert_savings_goal,
    get_savings_progress_df,
    mark_completed_savings_goals
)
from services_debts import insert_debt, get_debt_progress_df
from services_paychecks import upsert_paycheck, get_paychecks_df
from services_balances import (
    calculate_account_funding_status, 
    get_account_funding_summary_df,
    get_account_cash_coverage,
    save_account_balance,
    get_latest_account_balance
)
from services_main import (
    generate_monthly_bills,
    get_monthly_bills_df,
    get_upcoming_bills_until_next_paycheck_df,
    import_recurring_bills_from_csv,
    set_actual_amt_for_bill,
    mark_bill_paid
)
from services_allocation import (
    calculate_joint_transfer,
    get_people,
    get_paycheck_from_person
)


st.set_page_config(page_title="Joint Finance", layout="wide")


@st.cache_data(show_spinner=False)
def load_monthly_bills(month_start_dt: str) -> pd.DataFrame:
    return get_monthly_bills_df(month_start_dt)


@st.cache_data(show_spinner=False)
def load_paychecks() -> pd.DataFrame:
    return get_paychecks_df()

@st.cache_data(show_spinner=False)
def load_joint_funding() -> pd.DataFrame:
    return get_account_funding_summary_df("joint_checking")

@st.cache_data(show_spinner=False)
def load_devin_funding() -> pd.DataFrame:
    return get_account_funding_summary_df("devin_checking")

@st.cache_data(show_spinner=False)
def load_rieanna_funding() -> pd.DataFrame:
    return get_account_funding_summary_df("rieanna_checking")

@st.cache_data(show_spinner=False)
def load_savings_progress() -> pd.DataFrame:
    return get_savings_progress_df()

@st.cache_data(show_spinner=False)
def load_debt_progress() -> pd.DataFrame:
    return get_debt_progress_df()

@st.cache_data(show_spinner=False)
def load_joint_upcoming_bills_until_next_paycheck() -> pd.DataFrame:
    return get_upcoming_bills_until_next_paycheck_df("joint_checking")

@st.cache_data(show_spinner=False)
def load_devin_upcoming_bills_until_next_paycheck() -> pd.DataFrame:
    return get_upcoming_bills_until_next_paycheck_df("devin_checking")

@st.cache_data(show_spinner=False)
def load_rieanna_upcoming_bills_until_next_paycheck() -> pd.DataFrame:
    return get_upcoming_bills_until_next_paycheck_df("rieanna_checking")

@st.cache_data(show_spinner=False)
def load_joint_cash_coverage() -> dict:
    return get_account_cash_coverage("joint_checking")

@st.cache_data(show_spinner=False)
def load_devin_cash_coverage() -> dict:
    return get_account_cash_coverage("devin_checking")

@st.cache_data(show_spinner=False)
def load_rieanna_cash_coverage() -> dict:
    return get_account_cash_coverage("rieanna_checking")

@st.cache_data(show_spinner=False)
def load_people() -> pd.DataFrame:
    return get_people()


def clear_caches() -> None:
    load_monthly_bills.clear()
    load_paychecks.clear()
    load_joint_funding.clear()
    load_devin_funding.clear()
    load_rieanna_funding.clear()
    load_savings_progress.clear()
    load_joint_upcoming_bills_until_next_paycheck.clear()
    load_devin_upcoming_bills_until_next_paycheck.clear()
    load_rieanna_upcoming_bills_until_next_paycheck.clear()
    load_joint_cash_coverage.clear()
    load_devin_cash_coverage.clear()
    load_rieanna_cash_coverage.clear()
    load_people.clear()


def main() -> None:
    init_db()
    run_migrations()

    st.title("kovac money management")

    with st.sidebar:
        st.header("setup")

        if st.button("import recurring bills csv",key="import_recurring_bills_csv"):
            count = import_recurring_bills_from_csv("recurring_bills.csv")
            clear_caches()
            st.success(f"imported/updated {count} recurring bills")

        month_start_dt = st.text_input("month start", value="2026-03-01",key="monthly_start_dt")

        if st.button("generate monthly bills",key="generate_monthly_bills"):
            inserted = generate_monthly_bills(month_start_dt)
            calculate_account_funding_status("joint_checking")
            calculate_account_funding_status("devin_checking")
            calculate_account_funding_status("rieanna_checking")
            clear_caches()
            st.success(f"generated {inserted} monthly bills")

        if st.button("recalculate funding + savings",key="recalculate_funding_and_savings"):
            calculate_account_funding_status("joint_checking")
            calculate_account_funding_status("devin_checking")
            calculate_account_funding_status("rieanna_checking")
            mark_completed_savings_goals()
            clear_caches()
            st.success("recalculated")

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "monthly bills",
        "paychecks",
        "savings",
        "debts",
        "joint view",
        "devin view",
        "rieanna view",
        "allocate paychecks"
    ])

    with tab1:
        st.subheader("monthly bills")

        bills_df = load_monthly_bills(month_start_dt)
        st.dataframe(bills_df, use_container_width=True)

        with st.expander("mark bill paid"):
            unpaid_bills = bills_df[bills_df["paid_flg"] == 0]
            if unpaid_bills.empty:
                st.info("all bills are already marked as paid")
            else:
                bill_to_pay = st.selectbox(
                    "select bill",
                    unpaid_bills["bill_nm"].tolist()
                )

                if st.button("mark as paid",key="monthly_bills_mark_as_paid"):
                    mark_bill_paid(month_start_dt, bill_to_pay)
                    calculate_account_funding_status("joint_checking")
                    calculate_account_funding_status("devin_checking")
                    calculate_account_funding_status("rieanna_checking")
                    clear_caches()
                    st.success(f"{bill_to_pay} marked as paid")

        with st.expander("update actual amount"):
            bill_nm = st.text_input("bill name",key="monthly_view_bill_nm")
            actual_amt = st.number_input("actual amount", min_value=0.0, step=1.0,key="monthly_view_bill_actual_amt")

            if st.button("save actual amount",key="monthly_save_actual_amount"):
                set_actual_amt_for_bill(month_start_dt, bill_nm, actual_amt)
                calculate_account_funding_status("joint_checking")
                calculate_account_funding_status("devin_checking")
                calculate_account_funding_status("rieanna_checking")
                clear_caches()
                st.success("updated actual amount")

    with tab2:
        st.subheader("paychecks")

        paychecks_df = load_paychecks()
        st.dataframe(paychecks_df, use_container_width=True)

        with st.expander("add or update paycheck"):
            col1, col2 = st.columns(2)
            with col1:
                person_nm = st.selectbox("person", ["Devin", "Rieanna"],key="paychecks_person_nm")
                paycheck_date = st.text_input("paycheck date", value="2026-03-15",key="paychecks_paycheck_dt")
                net_income_amt = st.number_input("net income", min_value=0.0, step=1.0, key="paychecks_net_income_amt")
                joint_contribution_amt = st.number_input("joint contribution", min_value=0.0, step=1.0,key="paychecks_joint_contribution_amt")
            with col2:
                allowance_amt = st.number_input("allowance", min_value=0.0, step=1.0,key="paychecks_allowance_amt")
                personal_bills_amt = st.number_input("personal bills", min_value=0.0, step=1.0,key="paychecks_personal_bills_amt")
                savings_contribution_amt = st.number_input("savings contribution", min_value=0.0, step=1.0,key="paychecks_savings_contribution_amt")
                other_obligations_amt = st.number_input("other obligations", min_value=0.0, step=1.0,key="paychecks_other_obligations_amt")

            notes = st.text_input("notes", value="",key="paychecks_notes")

            if st.button("save paycheck",key="save_paycheck"):
                upsert_paycheck(
                    person_nm=person_nm,
                    paycheck_date=paycheck_date,
                    net_income_amt=net_income_amt,
                    joint_contribution_amt=joint_contribution_amt,
                    allowance_amt=allowance_amt,
                    personal_bills_amt=personal_bills_amt,
                    savings_contribution_amt=savings_contribution_amt,
                    other_obligations_amt=other_obligations_amt,
                    notes=notes or None,
                )
                calculate_account_funding_status("joint_checking")
                calculate_account_funding_status("devin_checking")
                calculate_account_funding_status("rieanna_checking")
                clear_caches()
                st.success("saved paycheck")

    with tab3:
        st.subheader("savings progress")

        savings_df = load_savings_progress()
        st.dataframe(savings_df, use_container_width=True)

        if not savings_df.empty:
            active_df = savings_df[savings_df["completed_flg"] == 0]
            st.metric("active goals", int(len(active_df)))
            st.metric("total remaining", float(active_df["remaining_amt"].sum()))

        # adding insert savings load 
        with st.expander("insert savings goal"):
            col1, col2 = st.columns(2)
            with col1:
                savings_name = st.text_input("savings goal", value="Vacation",key="savings_goal_nm")
                target_amt = st.number_input("goal amount", min_value=0.0, step=1.0,key="savings_goal_target_amt")
                target_date = st.text_input("target date",key="savings_target_dt")
            with col2:
                priority_nbr = st.number_input("priority number", min_value=0,key="savings_priority_nbr")
                category_nm = st.text_input("savings category", value="travel",key="savings_category_nm")
                goal_type_nm = st.text_input("goal type", value="one_time",key="savings_goal_type_nm")

            if st.button("save goal",key="savings_save_goal"):
                insert_savings_goal(
                    savings_name=savings_name,
                    target_amt=target_amt,
                    target_date=target_date,
                    priority_nbr=priority_nbr,
                    category_nm=category_nm,
                    goal_type_nm=goal_type_nm,
                )
                clear_caches()
                st.success("saved goal")
    with tab4:
        st.subheader("debt progress")

        debt_df = load_debt_progress()
        st.dataframe(debt_df, use_container_width=True)

        with st.expander("insert debt"):
            col1, col2 = st.columns(2)
            with col1:
                debt_nm = st.text_input("debt name",key="debts_debt_nm")
                person_nm = st.selectbox("person", ["Devin", "Rieanna", "Joint"],key="debts_person_nm")
            with col2:
                debt_amt = st.number_input("debt amount", min_value=0,key="debts_debt_amt")
                priority_nbr = st.number_input("priority rank", min_value=0,key="debts_priority_nbr")

            if st.button("save debt",key="debts_save_debt"):
                insert_debt(
                    debt_nm=debt_nm,
                    person_nm=person_nm,
                    debt_amt=debt_amt,
                    priority_nbr=priority_nbr
                )
                clear_caches()
                st.success("saved debt")

    with tab5:
        st.subheader("joint view")

        latest_balance = get_latest_account_balance("joint_checking")
        coverage = load_joint_cash_coverage()
        upcoming_df = load_joint_upcoming_bills_until_next_paycheck()

        with st.expander("update current joint checking balance", expanded=True):
            balance_amt = st.number_input("current balance", min_value=0.0, step=1.0,key="joint_current_balance_amt")
            balance_dt = st.text_input("balance date", value=date.today().isoformat(),key="joint_balance_dt")
            balance_notes = st.text_input("balance notes", value="",key="joint_balance_notes")

            if st.button("save joint checking balance",key="joint_save_joint_checking_balance"):
                save_account_balance(
                    account_nm="joint_checking",
                    balance_amt=balance_amt,
                    balance_dt=balance_dt,
                    notes=balance_notes or None,
                )
                clear_caches()
                st.success("saved balance")

        if latest_balance:
            st.write(
                f"latest balance: **${latest_balance['balance_amt']:,.2f}** "
                f"as of **{latest_balance['balance_dt']}**"
            )

        if not coverage["has_balance"]:
            st.info("add a joint checking balance to run the coverage check")
        else:
            col1, col2, col3, col4 = st.columns(4)

            col1.metric("current balance", f"${coverage['current_balance']:,.2f}")
            col2.metric("next paycheck", coverage["next_paycheck_dt"] or "none")
            col3.metric("upcoming bills", f"${coverage['upcoming_bills_total']:,.2f}")
            col4.metric("remaining after bills", f"${coverage['remaining_after_bills']:,.2f}")

            if coverage["covered_flg"]:
                st.success("you are covered until the next paycheck")
            else:
                st.error("you are not covered until the next paycheck")

            st.write("bills included in this check")
            st.dataframe(upcoming_df, use_container_width=True)

        st.subheader("joint funding")

        funding_df = load_joint_funding()
        st.dataframe(funding_df, use_container_width=True)

        if not funding_df.empty:
            underfunded_df = funding_df[funding_df["difference"] < 0]
            if not underfunded_df.empty:
                st.warning("some paycheck dates are underfunded")
                # st.dataframe(underfunded_df, use_container_width=True)


    with tab6:
        st.subheader("devin view")

        latest_balance = get_latest_account_balance("devin_checking")
        coverage = load_devin_cash_coverage()
        upcoming_df = load_devin_upcoming_bills_until_next_paycheck()

        with st.expander("update current checking balance", expanded=True, key="devin_update_curr_checking_balance"):
            balance_amt = st.number_input("current balance", min_value=0.0, step=1.0,key="devin_current_balance_amt")
            balance_dt = st.text_input("balance date", value=date.today().isoformat(),key="devin_balance_dt")
            balance_notes = st.text_input("balance notes", value="",key="devin_balance_notes")

            if st.button("save checking balance",key="devin_save_checking_balance"):
                save_account_balance(
                    account_nm="devin_checking",
                    balance_amt=balance_amt,
                    balance_dt=balance_dt,
                    notes=balance_notes or None,
                )
                clear_caches()
                st.success("saved balance")

        if latest_balance:
            st.write(
                f"latest balance: **${latest_balance['balance_amt']:,.2f}** "
                f"as of **{latest_balance['balance_dt']}**"
            )

        if not coverage["has_balance"]:
            st.info("add devin's checking balance to run the coverage check")
        else:
            col1, col2, col3, col4 = st.columns(4)

            col1.metric("current balance", f"${coverage['current_balance']:,.2f}")
            col2.metric("next paycheck", coverage["next_paycheck_dt"] or "none")
            col3.metric("upcoming bills", f"${coverage['upcoming_bills_total']:,.2f}")
            col4.metric("remaining after bills", f"${coverage['remaining_after_bills']:,.2f}")

            if coverage["covered_flg"]:
                st.success("you are covered until the next paycheck")
            else:
                st.error("you are not covered until the next paycheck")

            st.write("bills included in this check")
            st.dataframe(upcoming_df, use_container_width=True)

        st.subheader("devin checking funding")

        funding_df = load_devin_funding()
        st.dataframe(funding_df, use_container_width=True)

        if not funding_df.empty:
            underfunded_df = funding_df[funding_df["difference"] < 0]
            if not underfunded_df.empty:
                st.warning("some paycheck dates are underfunded")
                # st.dataframe(underfunded_df, use_container_width=True)


    with tab7:
        st.subheader("rieanna view")

        latest_balance = get_latest_account_balance("rieanna_checking")
        coverage = load_rieanna_cash_coverage()
        upcoming_df = load_rieanna_upcoming_bills_until_next_paycheck()

        with st.expander("update current checking balance", expanded=True, key="rieanna_update_curr_checking_balance"):
            balance_amt = st.number_input("current balance", min_value=0.0, step=1.0,key="rieanna_current_balance_amt")
            balance_dt = st.text_input("balance date", value=date.today().isoformat(),key="rieanna_balance_dt")
            balance_notes = st.text_input("balance notes", value="",key="rieanna_balance_notes")

            if st.button("save checking balance",key="rieanna_save_checking_balance"):
                save_account_balance(
                    account_nm="rieanna_checking",
                    balance_amt=balance_amt,
                    balance_dt=balance_dt,
                    notes=balance_notes or None,
                )
                clear_caches()
                st.success("saved balance")

        if latest_balance:
            st.write(
                f"latest balance: **${latest_balance['balance_amt']:,.2f}** "
                f"as of **{latest_balance['balance_dt']}**"
            )

        if not coverage["has_balance"]:
            st.info("add rieanna's checking balance to run the coverage check")
        else:
            col1, col2, col3, col4 = st.columns(4)

            col1.metric("current balance", f"${coverage['current_balance']:,.2f}")
            col2.metric("next paycheck", coverage["next_paycheck_dt"] or "none")
            col3.metric("upcoming bills", f"${coverage['upcoming_bills_total']:,.2f}")
            col4.metric("remaining after bills", f"${coverage['remaining_after_bills']:,.2f}")

            if coverage["covered_flg"]:
                st.success("you are covered until the next paycheck")
            else:
                st.error("you are not covered until the next paycheck")

            st.write("bills included in this check")
            st.dataframe(upcoming_df, use_container_width=True)

        st.subheader("rieanna checking funding")

        funding_df = load_rieanna_funding()
        st.dataframe(funding_df, use_container_width=True)

        if not funding_df.empty:
            underfunded_df = funding_df[funding_df["difference"] < 0]
            if not underfunded_df.empty:
                st.warning("some paycheck dates are underfunded")
                # st.dataframe(underfunded_df, use_container_width=True)

    with tab8:
        st.subheader("allocation of paychecks")

        with st.expander("calculate how much to move to joint checking from a paycheck", expanded=True):

            people = load_people()

            if not people:
                st.warning("No paychecks found yet.")
            else:
                selected_person = st.selectbox(
                    "Person",
                    options=people,
                    key="joint_transfer_person"
                )
                paycheck_rows = get_paycheck_from_person(selected_person=selected_person)

                if not paycheck_rows:
                    st.warning(f"No paychecks found for {selected_person}.")
                else:
                    paycheck_options = [
                        f"{row[0]}  |  ${float(row[1] or 0):,.2f}" for row in paycheck_rows
                    ]

                    selected_paycheck_label = st.selectbox(
                        "Pick paycheck",
                        options=paycheck_options,
                        key="joint_transfer_paycheck"
                    )

                    selected_paycheck_dt = selected_paycheck_label.split("|")[0].strip()

                    if st.button("Calculate joint transfer", key="calc_joint_transfer_btn"):
                        try:
                            result = calculate_joint_transfer(
                                person_nm=selected_person,
                                paycheck_dt=selected_paycheck_dt
                            )

                            st.success("Joint transfer calculated")

                            col1, col2 = st.columns(2)

                            with col1:
                                st.metric("Paycheck Net Income", f"${result['net_income_amt']:,.2f}")
                                st.metric("Recommended Transfer to Joint", f"${result['recommended_joint_transfer']:,.2f}")
                                st.metric("Remaining After Transfer", f"${result['remaining_after_joint_transfer']:,.2f}")

                            with col2:
                                st.metric("Coverage Month Total Joint Bills", f"${result['total_joint_bills_for_month']:,.2f}")
                                st.metric("Your Monthly Share", f"${result['per_person_monthly_share']:,.2f}")
                                st.metric("Next Paycheck", result["next_paycheck_dt"])

                            st.write("### Coverage Window")
                            st.write(
                                f"This paycheck helps fund bills from **{result['coverage_month_start']}** "
                                f"through **{result['coverage_month_end']}**."
                            )

                        except Exception as e:
                            st.error(f"Could not calculate joint transfer: {e}")


if __name__ == "__main__":
    main()