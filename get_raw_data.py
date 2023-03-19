import asyncio
from os import getenv
from asyncio import Task
from pathlib import Path
from typing import Dict, List, Optional
from get_raw_data_utils import (
    DataEntry,
    query_data,
    data_extract,
    database_connect,
    database_populate_update,
    database_dump_schema,
)

# Set number of days, if None or < 0, then all the range will be processed,
# would be better solution to use argparse
RANGE_DAYS: Optional[int] = 14
# Set the database name
DATABASE_NAME: str = "financial/financial.db"
DATABASE_SCHEMA_NAME: str = "schema.sql"
# List desired tickers here, better solution is to pass them as argv and use argparse
tickers = (
    "IBM",
    "AAPL",
)


async def main():
    """Asyncronously pulls data from the source using api-key file.
    Synchronously unites data and populates database.
    """
    api_key = None
    api_env = getenv("API_KEY")
    key_path = Path(Path(__file__).resolve().parent, "api-key").resolve()
    key_path_exists, key_path_isfile = key_path.exists(), key_path.is_file()

    if all((api_env is None, not all((key_path_exists, key_path_isfile)))):
        print("Neither API_KEY env variable provided, nor api-key file found")
        print("Please provide api key for alphavantage")
        return
    elif isinstance(api_env, str):
        if len(api_env) > 0:
            api_key = api_env
    elif all((key_path_exists, key_path_isfile)):
        with open(key_path, "rt") as api_key_file:
            api_key = api_key_file.readline()  # Read only first line of the file
    else:
        print("Unknown error with API key")
        return

    data_fetched: Dict[str, Dict] = dict()

    # Get data for every source
    tasks: Dict[str, Task] = {
        ticker: asyncio.create_task(query_data(ticker, api_key)) for ticker in tickers
    }

    # Wait for data to be transferred
    data_fetched = {
        ticker: await task for ticker, task in tasks.items() if await task is not None
    }

    data_processed: List[DataEntry] = []

    if len(data_fetched) > 0:
        # Use RANGE_DAYS here to set up desired processed range
        data_processed = data_extract(data_fetched, RANGE_DAYS)
    else:
        print("Data from response was not processed")
        return

    if len(data_processed) > 0:
        connection = database_connect(DATABASE_NAME)

        if connection is None:
            print("Could not connect to database, skipping operations")
            return

        # Populate or update database with acquired data, sequentally
        database_populate_update(connection, data_processed)
        # Dump SQLite database schema to file
        database_dump_schema(connection, DATABASE_SCHEMA_NAME)

    else:
        print("No data to populate or update database")
        return


if __name__ == "__main__":
    asyncio.run(main())
