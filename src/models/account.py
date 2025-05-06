from abc import ABC, abstractmethod
from decimal import Decimal

class Account(ABC):
    def __init__(self, account_guid: str, starting_balance: Decimal, first_name: str, last_name: str):
        self.account_guid: str = account_guid
        self.current_balance: Decimal = starting_balance
        self.first_name: str = first_name
        self.last_name: str = last_name
        self.transactions: list[Decimal] = []

    @abstractmethod
    def calculate_final_balance(self) -> Decimal:
        pass