import asyncio
import sqlite3
import requests
from asyncio import Task
from pprint import pprint
from pathlib import Path
from typing import List, Dict, Optional

DATABASE_NAME = "financial.db"
tickers = ("IBM",)


async def query_data(ticker: str, api_key: str) -> Optional[Dict]:
    """Requests TIME_SERIES_DAILY_ADJUSTED data from www.alphavantage.co with
    provided ticker name and api key text file path.

    Parameters:
    -----------
    ticker : str
        Name of the stock market ticker.
    api_key : str
        Path to API key file which is acquired from
        https://www.alphavantage.co/support/#api-key.

    Returns:
    --------
    JSON Response : Optional[Dict]
        JSON Response from the server if connection and processing is successfull.
    """
    url = (
        "https://www.alphavantage.co/"
        "query?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={ticker}&apikey={api_key}"
    )

    data = None

    # Returned data can be invalid or missing
    try:
        r = requests.get(url)
        data = r.json()
    except Exception as e:
        print(f"Could not get data: {e}")
    finally:
        return data


def data_extract(data_fetched: Dict[str, Dict]) -> List[Dict[str, str]]:
    """Converts request JSON data to required data with all the data flatened to
    symbol, date, open_price, close_price, volume.

    Parameters:
    -----------
    data_fetched : Dict[str, Dict]
        Dictionary of mappings from ticker to related financial data.

    Returns:
    --------
    Entries : List[Dict[str,str]]
        List of entries with required fields.
    """
    data_extracted = []

    for ticker, data in data_fetched.items():
        for day, entry in data["Time Series (Daily)"].items():
            data_extracted.append(
                {
                    "symbol": ticker,  # Symbol and ticker mean the same in this context
                    "date": str(day),
                    "open_price": str(entry["1. open"]),
                    "close_price": str(entry["4. close"]),
                    "volume": str(entry["6. volume"]),
                }
            )

    return data_extracted


def database_connect(database_name: str) -> Optional[sqlite3.Connection]:
    """Tries to connect to local sqlite3 database.

    Parameters:
    -----------
    database_name : str
        Name for the database.

    Returns:
    --------
    connection : Optional[sqlite3.Connection]
        Connection to the database in case connection was successfull.
    """
    con = None
    try:
        con = sqlite3.connect(database_name)
    except Exception as e:
        print(f"Could not connect to database: {e}")
    finally:
        return con


def database_populate(
    con: sqlite3.Connection, data_processed: List[Dict[str, str]], sequential: bool
):
    """Populates database with new values.

    Parameters:
    -----------
    con : sqlite3.Connection
        Database connection.
    data_processed : List[Dict[str, str]]
        List of financial data entries.
    sequential : bool
        Insert entries one by one or simultaneously.
    """
    cur = con.cursor()
    res = cur.execute("SELECT name FROM sqlite_master")
    res = res.fetchone()

    if res in {
        None,
        (),
    }:
        res = cur.execute(
            "CREATE TABLE financial_data(symbol, date, open_price, close_price, volume)"
        )
        db_command = ""

        # Insert value into database, one by one
        if sequential:
            for d in data_processed:
                cur.execute(
                    "INSERT INTO financial_data VALUES\n\t"
                    "('{}', '{}', '{}', '{}', '{}')".format(
                        d["symbol"],
                        d["date"],
                        d["open_price"],
                        d["close_price"],
                        d["volume"],
                    )
                )

        # Extract values from the data_processed and create a db command
        # add first line, then construct the string from all values
        else:
            db_command = "INSERT INTO financial_data VALUES\n\t" + "\n\t".join(
                [
                    "('{}', '{}', '{}', '{}', '{}')".format(
                        d["symbol"],
                        d["date"],
                        d["open_price"],
                        d["close_price"],
                        d["volume"],
                    )
                    for d in data_processed
                ]
            )

            cur.execute(db_command)

        print(db_command)


async def main():
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
        # pprint(data_fetched)
        data_processed = data_extract(data_fetched)
    else:
        print("Data from response was not processed")
        return

    pprint(data_processed)

    if len(data_processed) > 0:
        connection = database_connect(DATABASE_NAME)
        if connection is None:
            print("No database to populate")
            return

        database_populate(connection, data_processed, False)

    else:
        print("No data to put into database")
        return


if __name__ == "__main__":
    asyncio.run(main())
