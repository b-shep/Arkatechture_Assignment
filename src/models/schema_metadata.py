from src.models.table_metadata import TableMetadata

class SchemaMetadata:
    def __init__(self, schema_name: str):
        self.schema_name = schema_name
        self.tables: dict[str, TableMetadata] = {}