from loader import load_metadata, create_tables, insert_data
import os

DATA_DIR = "data"
METADATA_FILE_PATH = os.path.join(DATA_DIR, "INFORMATION_SCHEMA.csv")

def main():

    #Phase 1
    tables = load_metadata(METADATA_FILE_PATH)
    create_tables(tables)
    insert_data(tables, DATA_DIR)

if __name__ == '__main__':
    main()
