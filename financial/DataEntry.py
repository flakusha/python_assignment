from dataclasses import dataclass


@dataclass
class DataEntry:
    symbol: str
    date: str
    open_price: str
    close_price: str
    volume: str
