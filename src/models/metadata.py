from src.models.schema_metadata import SchemaMetadata

class Metadata:
    def __init__(self):
        self.schemas: dict[str, SchemaMetadata] = {}

    def add_schema(self, schema: SchemaMetadata):
        self.schemas[schema.schema_name] = schema
