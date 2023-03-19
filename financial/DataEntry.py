import json
from dataclasses import dataclass


@dataclass
class DataEntry:
    symbol: str
    date: str
    open_price: str
    close_price: str
    volume: str

    def toJSON(self):
        """Converts itself to dictionary which is serializable to json."""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
