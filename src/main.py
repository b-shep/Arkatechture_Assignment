import os

from src.analyzer import balance_checking_accounts, balance_loan_accounts
from src.db import get_loan_accounts, get_checking_accounts, create_tables
from src.loader import load_metadata, insert_data

DATA_DIR = "data"
METADATA_FILE_PATH = os.path.join(DATA_DIR, "INFORMATION_SCHEMA.csv")

def main():

    #Phase 1
    tables = load_metadata(METADATA_FILE_PATH)
    create_tables(tables)
    insert_data(tables, DATA_DIR)

    #Phase 2
    #Assumptions for calculating sum total asset size:
        #assets = sum of all checking account balances (money the institution has) plus sum of all loan account balances (money the institution is owed)
        #overdrawn accounts should be added to the asset size - this is money the institution is owed and can likely be collected
        #overpaid loans should be subtracted from the asset size - this is money the institution owes to the loan account holder

    accounts = get_checking_accounts()
    loans = get_loan_accounts()
    (sum_checking, overdrawn_accounts) = balance_checking_accounts(accounts)
    (sum_loans, overpaid_loans) = balance_loan_accounts(loans)
    sum_total = sum_checking + sum_loans

    #Who has overdrawn checking accounts and what are their overdrawn balances?
    print(f"{len(overdrawn_accounts)} overdrawn accounts:")
    for account in overdrawn_accounts:
        print(f"Account {account.account_guid} belonging to {account.first_name} {account.last_name} has an overdrawn balance of {account.calculate_final_balance()}")

    #Who has overpaid their loans, and by how much?
    print(f"{len(overpaid_loans)} overpaid loans:")
    for loan in overpaid_loans:
        print(f"Account {loan.account_guid} owned by {loan.first_name} {loan.last_name} has an overpaid loan with a debt owed of {loan.calculate_final_balance()}")

    #What is the sum total asset size of this institution?
    print(f"Sum total asset size of this institution: {sum_total}")

if __name__ == '__main__':
    main()
