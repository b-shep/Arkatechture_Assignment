from src.models.checking_account import CheckingAccount
from src.models.loan_account import LoanAccount
from decimal import Decimal

def balance_checking_accounts(accounts:list[CheckingAccount]) -> (Decimal, list[CheckingAccount]):
    sum_total: Decimal = 0;
    overdrawn_accounts: list[CheckingAccount] = []

    for account in accounts:
        final_balance = account.calculate_final_balance()
        sum_total += final_balance
        if final_balance < 0:
            overdrawn_accounts.append(account)
            sum_total -= final_balance # overdrawn accounts are money owed to the institution, subtract the negative balance to add it to the asset size
    return sum_total, overdrawn_accounts

def balance_loan_accounts(loans:list[LoanAccount]) -> (Decimal, list[LoanAccount]):
    sum_total: Decimal= 0
    overpaid_loans: list[LoanAccount] = []

    for loan in loans:
        final_balance = loan.calculate_final_balance()
        sum_total += final_balance
        if final_balance < 0:
            overpaid_loans.append(loan)
            sum_total += final_balance # overpaid loans are money the institution owes to the loan account holder, add the negative balance to subtract it from the asset size
    return sum_total, overpaid_loans

