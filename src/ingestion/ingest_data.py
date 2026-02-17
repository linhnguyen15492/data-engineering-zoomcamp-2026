from src.db.pg_database import print_message


def from_ingest():
    print("This function is called from ingest_data.")
    print_message()


def main():
    from_ingest()


if __name__ == "__main__":
    main()
