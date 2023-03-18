import asyncio
from asyncio import Task
from pathlib import Path
from typing import Dict
from get_raw_data_utils import data_extract, database_connect, database_populate_update


DATABASE_NAME = "financial.db"
tickers = (
    "IBM",
    "AAPL",
)


async def main():
    """Asyncronously pulls data from the source using api-key file.
    Synchronously unites data and populates database.
    """
    api_key_path = Path(Path(__file__).resolve().parent, "api-key").resolve()

    if not all((api_key_path.exists(), api_key_path.is_file())):
        print("api-key file is not found, please provide api key for alphavantage")
        return

    data_fetched: Dict[str, Dict] = dict()

    with open(api_key_path, "rt") as api_key_file:
        api_key = api_key_file.readline()  # Read only first line of the file

        # Get data for every source
        tasks: Dict[str, Task] = {
            ticker: asyncio.create_task(query_data(ticker, api_key))
            for ticker in tickers
        }

        # Wait for data to be transferred
        data_fetched = {
            ticker: await task
            for ticker, task in tasks.items()
            if await task is not None
        }

    data_processed = []

    if len(data_fetched) > 0:
        # Leave only last 2 weeks
        data_processed = data_extract(data_fetched, 14)
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

    else:
        print("No data to populate or update database")
        return


if __name__ == "__main__":
    asyncio.run(main())
