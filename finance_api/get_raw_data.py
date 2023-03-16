import asyncio
import requests
from asyncio import Task
from pprint import pprint
from pathlib import Path
from typing import List, Dict, Optional

tickers = ("IBM",)


async def query_data(ticker: str, api_key: str) -> Optional[Dict]:
    """Requests data from www.alphavantage.co with provided api-key

    Parameters:
    -----------
    ticker : str
        Name of the stock market ticker.
    api_key : str
        API key acquired from https://www.alphavantage.co/support/#api-key
    """
    url = (
        "https://www.alphavantage.co/"
        "query?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={ticker}&apikey={api_key}"
    )

    r = requests.get(url)
    data = None

    # Returned data can be invalid or missing
    try:
        data = r.json()
    except requests.exceptions.JSONDecodeError as e:
        print(f"Could not decode response: {e}")
    except Exception as e:
        print(f"Could not query_data: {e}")
    finally:
        return data


def extract_data(entry: Dict) -> List[Dict]:
    return [
        {0: 0},
    ]


async def main():
    api_key_path = Path(Path(__file__).resolve().parent, "api-key")
    fetched_data: List[Dict] = []

    with open(api_key_path, "rt") as api_key_file:
        api_key = api_key_file.readline()  # Read only first line of the file

        # Get data for every source
        tasks = [asyncio.create_task(query_data(ticker, api_key)) for ticker in tickers]

        # Wait for data to be transferred
        fetched_data = [await task for task in tasks if task is not None]

    pprint(fetched_data)


if __name__ == "__main__":
    asyncio.run(main())
