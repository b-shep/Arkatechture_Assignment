from decimal import Decimal
from src.models.account import Account

class LoanAccount(Account):
    def __init__(self, account_guid: str, starting_debt: Decimal, first_name: str, last_name: str):
        super().__init__(account_guid, starting_debt, first_name, last_name)

    #Questionable assumption here that positive transactions are payments against the debt so should be subtracted
    def calculate_final_balance(self) -> Decimal:
        for transaction in self.transactions:
            self.current_balance -= transaction
        return self.current_balance