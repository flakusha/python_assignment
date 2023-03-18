import requests
import sqlite3
from typing import List, Dict, Optional, Any
from pprint import pprint
from datetime import date
from financial.DataEntry import DataEntry


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
    data = None
    url = (
        "https://www.alphavantage.co/"
        "query?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={ticker}&apikey={api_key}"
    )

    # Returned data can be invalid or missing
    try:
        r = requests.get(url)
    except Exception as e:
        print(f"Could not get data: {e}")
        return None

    if r.status_code != 200:
        print(f"Failed connection with status code: {r.status_code}")
        return None

    try:
        data = r.json()
    except Exception as e:
        print(f"Could not process data: {e}")
    finally:
        return data


def data_extract(
    data_fetched: Dict[str, Dict], num_days: Optional[int] = None
) -> List[DataEntry]:
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
    ENTRIES_TYPE = "Time Series (Daily)"
    data_extracted: List[DataEntry] = []

    # In case num_days is not provided - capture all data
    if num_days is None:
        num_days = -1

    for ticker, data in data_fetched.items():
        # Sanity check in case JSON does not have data
        # This happens if there are many requests sent
        if not key_in_dict(data, ENTRIES_TYPE):
            print(f"Skipping: '{ticker}': no data: {ENTRIES_TYPE}")
            continue

        # Sort dictionary by dates, enumerate each trade day
        # traverse reversed iterable and collect entries to data_extracted
        # until num_days is satisfied
        for i, (day, entry) in enumerate(
            (
                sorted(
                    data[ENTRIES_TYPE].items(),
                    # Use day string to convert to ISO date format and compare
                    key=lambda item: date.fromisoformat(item[0]),
                    reverse=True,
                )
            )
        ):
            # If num_days < 0, it automatically cancells the enumerator check
            if all((i >= num_days, num_days > 0)):
                break

            data_extracted.append(
                DataEntry(
                    ticker,
                    str(day),
                    str(entry["1. open"]),
                    str(entry["4. close"]),
                    str(entry["6. volume"]),
                )
            )

    return data_extracted


def key_in_dict(dict: Dict[str, Any], key: str) -> bool:
    """Simple wrapper that returns True if key is in dictionary.

    Parameters:
    -----------
    dict : Dict[str, Any]
        Any dictionary with str keys.
    key : str
        Key to check.

    Returns:
    --------
    bool
        Key in dictionary.
    """
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


def database_populate_update(con: sqlite3.Connection, data_processed: List[DataEntry]):
    """Populates database's table with new values if table doesn't exist, otherwise
    updates table entries.

    Parameters:
    -----------
    con : sqlite3.Connection
        Database connection.
    data_processed : List[DataEntry]
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


def database_populate_sequential(cur: sqlite3.Cursor, data_processed: List[DataEntry]):
    """Checks that database table already has entries.
    Populates or updates entire database table entry by entry.

    Parameters:
    -----------
    cur : sqlite3.Cursor
        Database cursor.
    data_processed : List[DataEntry]
        List of processed data entries.
    """
    for d in data_processed:
        # In SQLite first the check needed, then command, to avoid adding duplicates
        # If resulting list is empty - insert new entry, otherwise update is needed.
        cur.execute(
            "SELECT symbol FROM financial_data "
            f"""WHERE symbol = '{d.symbol}' AND date = '{d.date}';"""
        )

        res = cur.fetchall()

        if len(res) == 0:
            db_command = "".join(
                (
                    "INSERT INTO financial_data ",
                    "VALUES ('{}', '{}', '{}', '{}', '{}')\n".format(
                        d.symbol, d.date, d.open_price, d.close_price, d.volume
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
                        d.open_price,
                        "close_price",
                        d.close_price,
                        "volume",
                        d.volume,
                    ),
                    f"""WHERE symbol = '{d.symbol}' AND date = '{d.date}';""",
                )
            )

            cur.execute(db_command)


def database_dump_schema(con: sqlite3.Connection, name: str):
    with open(name, "wt") as sc:
        cur = con.cursor()

        for line in cur.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'financial_data'"
        ).fetchall():
            print(line[0])
            sc.write(f"{line[0]}\n")