import asyncio
import sqlite3
import requests
from datetime import date
from asyncio import Task
from pprint import pprint
from pathlib import Path
from typing import List, Dict, Optional

DATABASE_NAME = "financial.db"
tickers = (
    "IBM",
    "AAPL",
)


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
    except Exception as e:
        print(f"Could not get data: {e}")
        return data

    try:
        data = r.json()
    except Exception as e:
        print(f"Could not process data: {e}")
        return data
    finally:
        return data


def data_extract(
    data_fetched: Dict[str, Dict], num_days: Optional[int] = None
) -> List[Dict[str, str]]:
    """Converts request JSON data to required data with all the data flatened to
    symbol, date, open_price, close_price, volume. If num_days is provided, specific
    amount of entries is stored for each symbol.

    Parameters:
    -----------
    data_fetched : Dict[str, Dict]
        Dictionary of mappings from ticker to related financial data.
    num_days : int
        Number of days to filter from the last. If num_days <= 0 or num_days is None,
        then fetched data won't be changed.

    Returns:
    --------
    Entries : List[Dict[str,str]]
        List of entries with required fields.
    """
    data_extracted = []
    ENTRIES_TYPE = "Time Series (Daily)"

    for ticker, data in data_fetched.items():
        if num_days is None or num_days <= 0:
            # Sanity check in case JSON does not have data
            if not check_dict_key(data, ENTRIES_TYPE):
                print(f"Skipping: '{ticker}': no data: {ENTRIES_TYPE}")
                break

            for day, entry in data[ENTRIES_TYPE].items():
                data_extracted.append(
                    {
                        # Symbol and ticker mean the same in this context
                        "symbol": ticker,
                        "date": str(day),
                        "open_price": str(entry["1. open"]),
                        "close_price": str(entry["4. close"]),
                        "volume": str(entry["6. volume"]),
                    }
                )

        else:
            # Sanity check in case JSON does not have data
            if not check_dict_key(data, ENTRIES_TYPE):
                print(f"Skipping: '{ticker}': no data: {ENTRIES_TYPE}")
                break

            # Sort dictionary by dates, enumerate each trade day
            # reverse the iterable and collect entries to data_extracted
            # until num_days is satisfied
            for i, (day, entry) in enumerate(
                (
                    sorted(
                        data[ENTRIES_TYPE].items(),
                        key=lambda item: date.fromisoformat(item[0]),
                        reverse=True,
                    )
                )
            ):
                if i >= num_days:
                    break

                data_extracted.append(
                    {
                        "symbol": ticker,
                        "date": str(day),
                        "open_price": str(entry["1. open"]),
                        "close_price": str(entry["4. close"]),
                        "volume": str(entry["6. volume"]),
                    }
                )

    return data_extracted


def check_dict_key(dict: Dict[str, str], key: str) -> bool:
    return key in dict


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


def database_populate_update(
    con: sqlite3.Connection, data_processed: List[Dict[str, str]]
):
    """Populates database's table with new values if table doesn't exist, otherwise
    updates table entries.

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

    # Populate database in case there is no table
    if res in {
        None,
        (),
    }:
        print(f"Table don't exist, creating {len(data_processed)} entries")
        res = cur.execute(
            "CREATE TABLE "
            "financial_data(symbol TEXT, date TEXT, "
            "open_price TEXT, close_price TEXT, volume TEXT)"
        )
    else:
        print(f"Table exists, updating {len(data_processed)} entries")

    # Insert values into database, one by one
    database_populate_sequential(cur, data_processed)

    con.commit()


def database_populate_sequential(
    cur: sqlite3.Cursor, data_processed: List[Dict[str, str]]
):
    """Checks that database table already has entries.
    Populates or updates entire database table entry by entry.

    Parameters:
    -----------
    cur : sqlite3.Cursor
        Database cursor.
    data_processed : List[Dict[str, str]]
        List of processed data entries.
    """
    for d in data_processed:
        # In SQLite first the check then command, to avoid adding duplicates
        # If resulting list is empty - insert new entry, otherwise update is
        # needed.
        cur.execute(
            "SELECT symbol FROM financial_data "
            f"""WHERE symbol = '{d["symbol"]}' AND date = '{d["date"]}';"""
        )

        res = cur.fetchall()

        if len(res) == 0:
            db_command = "".join(
                (
                    "INSERT INTO financial_data ",
                    "VALUES ('{}', '{}', '{}', '{}', '{}')\n".format(
                        d["symbol"],
                        d["date"],
                        d["open_price"],
                        d["close_price"],
                        d["volume"],
                    ),
                )
            )

            cur.execute(db_command)

        else:
            db_command = "".join(
                (
                    "UPDATE financial_data SET ",
                    "{} = '{}', {} = '{}', {} = '{}'\n".format(
                        "open_price",
                        d["open_price"],
                        "close_price",
                        d["close_price"],
                        "volume",
                        d["volume"],
                    ),
                    f"""WHERE symbol = '{d["symbol"]}' AND date = '{d["date"]}';""",
                )
            )

            cur.execute(db_command)


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
