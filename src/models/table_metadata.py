from src.models.column_metadata import ColumnMetadata

class TableMetadata:
    def __init__(self, name: str):
        self.name = name
        self.columns: list[ColumnMetadata] = []
