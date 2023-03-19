# Take-Home Assignment

The goal of this take-home assignment is to evaluate your abilities to use API, data processing and transformation, SQL, and implement a new API service in Python.

You should first fork this repository, and then send us the code or the url of your forked repository via email.

**Please do not submit any pull requests to this repository.**

You need to perform the following **Two** tasks:

## Task1
### Problem Statement:
1. Retrieve the financial data of Two given stocks (IBM, Apple Inc.)for the most recently two weeks. Please using an free API provider named [AlphaVantage](https://www.alphavantage.co/documentation/) 
2. Process the raw API data response, a sample output after process should be like:
```
{
    "symbol": "IBM",
    "date": "2023-02-14",
    "open_price": "153.08",
    "close_price": "154.52",
    "volume": "62199013",
},
{
    "symbol": "IBM",
    "date": "2023-02-13",
    "open_price": "153.08",
    "close_price": "154.52",
    "volume": "59099013"
},
{
    "symbol": "IBM",
    "date": "2023-02-12",
    "open_price": "153.08",
    "close_price": "154.52",
    "volume": "42399013"
},
...
``` 
3. Insert the records above into a table named `financial_data` in your local database, column name should be same as the processed data from step 2 above (symbol, date, open_price, close_price, volume) 


## Task2
### Problem Statement:
1. Implement an Get financial_data API to retrieve records from `financial_data` table, please note that:
    - the endpoint should accept following parameters: start_date, end_date, symbol, all parameters are optional
    - the endpoint should support pagination with parameter: limit and page, if no parameters are given, default limit for one page is 5
    - the endpoint should return an result with three properties:
        - data: an array includes actual results
        - pagination: handle pagination with four properties
            
            - count: count of all records without panigation
            - page: current page index
            - limit: limit of records can be retrieved for single page
            - pages: total number of pages
        - info: includes any error info if applies
    

Sample Request:
```bash
curl -X GET 'http://localhost:5000/api/financial_data?start_date=2023-01-01&end_date=2023-01-14&symbol=IBM&limit=3&page=2'

```
Sample Response:
```
{
    "data": [
        {
            "symbol": "IBM",
            "date": "2023-01-05",
            "open_price": "153.08",
            "close_price": "154.52",
            "volume": "62199013",
        },
        {
            "symbol": "IBM",
            "date": "2023-01-06",
            "open_price": "153.08",
            "close_price": "154.52",
            "volume": "59099013"
        },
        {
            "symbol": "IBM",
            "date": "2023-01-09",
            "open_price": "153.08",
            "close_price": "154.52",
            "volume": "42399013"
        }
    ],
    "pagination": {
        "count": 20,
        "page": 2,
        "limit": 3,
        "pages": 7
    },
    "info": {'error': ''}
}

```

2. Implement an Get statistics API to perform the following calculations on the data in given period of time:
    - Calculate the average daily open price for the period
    - Calculate the average daily closing price for the period
    - Calculate the average daily volume for the period

    - the endpoint should accept following parameters: start_date, end_date, symbols, all parameters are required
    - the endpoint should return an result with two properties:
        - data: calculated statistic results
        - info: includes any error info if applies

Sample request:
```bash
curl -X GET http://localhost:5000/api/statistics?start_date=2023-01-01&end_date=2023-01-31&symbol=IBM

```
Sample response:
```
{
    "data": {
        "start_date": "2023-01-01",
        "end_date": "2023-01-31",
        "symbol": "IBM",
        "average_daily_open_price": 123.45,
        "average_daily_close_price": 234.56,
        "average_daily_volume": 1000000
    },
    "info": {'error': ''}
}

```

## What you should deliver:
Directory structure:
```
project-name/
├── model.py
├── schema.sql
├── get_raw_data.py
├── Dockerfile
├── docker-compose.yml
├── README.md
├── requirements.txt
└── financial/<Include API service code here>

```

1. A `get_raw_data.py` file in root folder

    Action: 
    
    Run 
    ```bash
    python get_raw_data.py
    ```

    Expectation: 
    
    1. Financial data will be retrieved from API and processed,then insert all processed records into table `financial_data` in local db
    2. Duplicated records should be avoided when executing get_raw_data multiple times, consider implementing your own logic to perform upsert operation if the database you select does not have native support for such operation.

2. A `schema.sql` file in root folder
    
    Define schema for financial_data table, if you prefer to use an ORM library, just **ignore** this deliver item and jump to item3 below.

    Action: Run schema definition in local db

    Expectation: A new table named `financial_data` should be created if not exists in db

3. (Optional) A `model.py` file: 
    
    If you perfer to use a ORM library instead of DDL, please include your model definition in `model.py`, and describe how to perform migration in README.md file

4. A `Dockerfile` file in root folder

    Build up your local API service

5. A `docker-compose.yml` file in root folder

    Two services should be defined in docker-compose.yml: Database and your API

    Action:

    ```bash
    docker-compose up
    ```

    Expectation:
    Both database and your API service is up and running in local development environment

6. A `financial` sub-folder:

    Put all API implementation related codes in here

7. `README.md`: 

    You should include:
    - A brief project description
    - Tech stack you are using in this project
    - How to run your code in local environment
    - Provide a description of how to maintain the API key to retrieve financial data from AlphaVantage in both local development and production environment.

8. A `requirements.txt` file:

    It should contain your dependency libraries.

## Requirements:

- The program should be written in Python 3.
- You are free to use any API and libraries you like, but should include a brief explanation of why you chose the API and libraries you used in README.
- The API key to retrieve financial data should be stored securely. Please provide a description of how to maintain the API key from both local development and production environment in README.
- The database in Problem Statement 1 could be created using SQLite/MySQL/.. with your own choice.
- The program should include error handling to handle cases where the API returns an error or the data is not in the correct format.
- The program should cover as many edge cases as possible, not limited to expectations from deliverable above.
- The program should use appropriate data structures and algorithms to store the data and perform the calculations.
- The program should include appropriate documentation, including docstrings and inline comments to explain the code.

## Evaluation Criteria:

Your solution will be evaluated based on the following criteria:

- Correctness: Does the program produce the correct results?
- Code quality: Is the code well-structured, easy to read, and maintainable?
- Design: Does the program make good use of functions, data structures, algorithms, databases, and libraries?
- Error handling: Does the program handle errors and unexpected input appropriately?
- Documentation: Is the code adequately documented, with clear explanations of the algorithms and data structures used?

## Additional Notes:

You have 7 days to complete this assignment and submit your solution.

# Notes on assignment:

## Description of dependencies:

### I have tried to opt out to minimum amount of dependencies for this assignment:

- SQLite3 - built-in Python database support
- Requests - default choice to get raw data from web
- FastAPI - Python async API server
- Uvicorn - ASGI web server implementation for Python best combined with FastAPI
- Docker - it's expected that Docker is installed and running on the system
- Alpine (in Docker) - lightweight distribution to install Python, manage dependencies
(as result image size is smaller than python-slim docker)

### What could be added to make code more readable

- SQLAlchemy would be much more user-friendly approach than sqlite3 to build database
queries

### Development:

- black - Python code formatting
- mypy and pylsp - code linting, type validity checks

## Actions needed to be performed to run and test application:

- Create `api-key` file with your key for Alphavantage OR pass `API_KEY` environmental
variable to Docker compose, so data can be loaded
- Run `docker-compose up -d --build` which will
    - Download alpine
    - Install python3 and pip3, add non-admin user
    - Create app folder, move only needed files from this git repo
    - Install required Python dependencies and load data using API key, place database
    near api.py
    - Set the correct working directory and run the uvicorn app on localhost:5000
- Now API can be tested, I recommend trying Firefox to get structured JSON responses
    - `http://localhost:5000/api/financial_data?start_date=2023-02-02&end_date=2023-03-17&symbol=IBM&limit=2&page=1`
    - `http://localhost:5000/api/statistics?start_date=2023-03-01&end_date=2023-03-15&symbol=IBM`

*NOTE:* Aside from providing API key, nothing is required from the user.

## Notes on development:

### Stored data types

I followed sample responses that were provided in task description.

Storing financial data and doing calculations in floats is discouraged, so in the best
case scenario custom numeric, e.g. Decimal should be used. Storing the same data in
strings is less convenient for developer as conversion is required every time data is
gathered from database.

### Exception handling

I have tried to provide exception handling for problems I encountered during development
and cases that would happen. For bigger application more request and response checks
will be needed and more cases should be taken in account.
For example, at the moment one can request huge amount of data from the API

### Managing API key

For API key management following approaches can be applied:
- Opt out for .env file to load keys in development
- Store and regenerate them in secret management service
- Store key in server environment only
- Mount a volume to Docker image with secret key(s), so keys are guaranteed to be stored
separately

Of cource, api keys are never to be commited and never hardcoded in source files

### Misc

- Docstrings and comments left around the code to give other people better understanding
of what's happening in application.
- SQL schema is dumped automatically at the moment and can be enchanced.