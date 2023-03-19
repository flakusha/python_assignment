import sqlite3
from DataEntry import DataEntry
from fastapi import FastAPI, status, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from typing import Optional, List
from math import ceil
from datetime import date
from dataclasses import asdict

# Start the app
APP = FastAPI(
    title="Get Financial Data and Statistics API",
    description="Task 2 for the python assignment",
)
DB_NAME = "financial.db"
DB_CONNECTION: Optional[sqlite3.Connection] = None


@APP.on_event("startup")
async def on_startup():
    try:
        DB_CONNECTION = sqlite3.connect(DB_NAME)
    except Exception as e:
        print(f"Could not connect to database: {e}")


@APP.exception_handler(RequestValidationError)
async def request_validation_error(req: Request, err: RequestValidationError):
    """Returns error information if request is incorrect"""
    return JSONResponse(
        content={
            "info": "\n".join(e["msg"] for e in err.errors()),
        },
        status_code=400,
    )


@APP.get("/api/status", tags=["api"])
async def report_status():
    """Reports that API works if request is acquired."""
    return JSONResponse(
        status_code=status.HTTP_200_OK, content={"info": "API is working"}
    )


@APP.get("/api/financial_data", tags=["api"])
async def on_financial_data_call(
    symbol: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    limit: Optional[int],
    page: Optional[int],
) -> JSONResponse:
    """API that returns financial data from database for the correct request,
    otherwise returns information about error.

    Date range is inclusive.

    Parameters:
    -----------
    symbol : Optional[str]
        Symbol/ticker name to request.
    start_date : Optional[str]
        ISO date string to request from database for the beginning of trading period.
    end_date : Optional[str]
        ISO date string to request from database for the end of trading period.

    Returns:
    --------
    response : JSONResponse
        JSON response for requested data.
    """
    # Check for data correctness
    check = check_request(symbol, start_date, end_date)

    if check is not None:
        return check

    # Default values for request, force to positive, amount of code should be
    # reduced
    if limit is None:
        limit = 5
    else:
        if isinstance(limit, int):
            limit = abs(limit)
        else:
            return JSONResponse(
                content={
                    "info": {"error": "Limit is in incompatible format"},
                },
                status_code=400,
            )
    if page is None:
        page = 1
    else:
        if isinstance(page, int):
            page = abs(page)
        else:
            return JSONResponse(
                content={
                    "info": {"error": "Page is in incompatible format"},
                },
                status_code=400,
            )

    DB_CONNECTION = sqlite3.connect(DB_NAME)

    if DB_CONNECTION is None:
        return JSONResponse(
            content={
                "info": {"error": "Internal DB connection failed"},
            },
            status_code=500,
        )

    cur = DB_CONNECTION.cursor()
    # Get data from DB, process using sql calls, count entries, return results and then
    # check that result exists and not empty
    cur.execute(
        " ".join(
            (
                "WITH req AS (SELECT * FROM financial_data",
                "WHERE symbol = '{}' AND date >= '{}' AND date <= '{}' ORDER BY date) ".format(
                    symbol, start_date, end_date
                ),
                "SELECT (SELECT count(*) FROM req), * FROM req LIMIT {} OFFSET {}".format(
                    limit, page
                ),
            )
        )
    )

    # Should results list of tuples
    ress = cur.fetchall()

    if len(ress) == 0:
        return JSONResponse(
            content={
                "info": {"error": "No information is found for this request"},
            },
            status_code=400,
        )

    entries: List[DataEntry] = []

    # Collect data from the database request to actual response
    for res in ress:
        entry = DataEntry(res[1], res[2], res[3], res[4], res[5])
        entries.append(asdict(entry))

    # As long as len >= 1, can fetch count and pages out of it by taking last counted
    count = int(ress[-1][0])
    pages = ceil(count / int(limit))

    return JSONResponse(
        content={
            "data": entries,
            "pagination": {
                "count": count,
                "page": page,
                "limit": limit,
                "pages": pages,
            },
            "info": {"error": ""},
        },
    )


@APP.get("/api/statistics", tags=["api"])
async def on_statistics_data_call(
    symbol: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> JSONResponse:
    """Calculates statistics for the selected symbol and date range.

    Date range is inclusive.

    Parameters:
    -----------
    symbol : Optional[str]
        Symbol/ticker name to request.
    start_date : Optional[str]
        ISO date string to request from database for the beginning of trading period.
    end_date : Optional[str]
        ISO date string to request from database for the end of trading period.

    Returns:
    --------
    response : JSONResponse
        JSON response for requested data.
    """
    # Check for data correctness
    check = check_request(symbol, start_date, end_date)

    if check is not None:
        return check

    DB_CONNECTION = sqlite3.connect(DB_NAME)

    if DB_CONNECTION is None:
        return JSONResponse(
            content={
                "info": {"error": "Internal DB connection failed"},
            },
            status_code=500,
        )

    cur = DB_CONNECTION.cursor()
    cur.execute(
        " ".join(
            (
                "SELECT * FROM financial_data",
                "WHERE symbol = '{}' AND date >= '{}' AND date <= '{}'".format(
                    symbol,
                    start_date,
                    end_date,
                ),
            ),
        )
    )

    # ress for results
    ress = cur.fetchall()
    ress_n = len(ress)

    if ress_n == 0:
        return JSONResponse(
            content={
                "info": {"error": "No information is found for this request"},
            },
            status_code=400,
        )

    avg_daily_open = sum(float(res[2]) for res in ress) / ress_n
    avg_daily_close = sum(float(res[3]) for res in ress) / ress_n
    avg_daily_volume = sum(int(res[4]) for res in ress) / ress_n

    return JSONResponse(
        content={
            "data": {
                "start_date": start_date,
                "end_date": end_date,
                "symbol": symbol,
                "average_daily_open_price": round(avg_daily_open, 2),
                "average_daily_close_price": round(avg_daily_close, 2),
                "average_daily_volume": int(avg_daily_volume),
            },
            "info": {"error": ""},
        }
    )


def check_request(
    symbol: Optional[str], start_date: Optional[str], end_date: Optional[str]
) -> Optional[JSONResponse]:
    """Checks that data in request can be used for database call.

    Parameters:
    -----------
    symbol : Optional[str]
        Symbol/ticker name to request.
    start_date : Optional[str]
        ISO date string to request from database for the beginning of trading period.
    end_date : Optional[str]
        ISO date string to request from database for the end of trading period.

    Returns:
    --------
    Error response : Optional[JSONResponse]
        Returns custom error response for each case, if there is no error,
        None returned.
    """
    # Provide checks for absent data
    if symbol is None:
        return JSONResponse(
            content={
                "info": {"error": "No symbol is provided, cannot create response"}
            },
            status_code=400,
        )

    elif isinstance(symbol, str):
        if len(symbol) == 0:
            return JSONResponse(
                content={"info": {"error": "Symbol name is empty"}}, status_code=400
            )

    elif start_date is None or end_date is None:
        return JSONResponse(
            content={"info": {"error": "No start_date or end_date is provided"}},
            status_code=400,
        )

    try:
        date.fromisoformat(start_date)
        date.fromisoformat(end_date)
    except Exception as e:
        return JSONResponse(
            content={"info": {"error": f"Request date format is incorrect: {e}"}}
        )

    return None
