from decimal import Decimal
from src.models.account import Account

class CheckingAccount(Account):
    def __init__(self, account_guid: str, starting_balance: Decimal, first_name: str, last_name: str):
        super().__init__(account_guid, starting_balance, first_name, last_name)

    def calculate_final_balance(self) -> Decimal:
        for transaction in self.transactions:
            self.current_balance += transaction
        return self.current_balance