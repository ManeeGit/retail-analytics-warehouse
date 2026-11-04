"""Fixed fixtures, deliberately containing realistic edge cases (no random state)."""

import csv
from pathlib import Path

HEADERS = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]

ROWS = [
    ["10001", "MUG", "Blue mug", "2", "2024-01-01 10:00:00", "2.50", "101", "United Kingdom"],
    ["10001", "TEA", "Tea tin", "1", "2024-01-01 10:00:00", "4.99", "101", "United Kingdom"],
    ["10002", "MUG", "Blue mug", "3", "2024-01-02 11:00:00", "2.50", "102", "France"],
    ["10003", "BAG", "Canvas bag", "1", "2024-01-15 12:00:00", "8.00", "101", "United Kingdom"],
    ["10004", "MUG", "Blue mug", "1", "2024-02-02 09:00:00", "2.50", "101", "Germany"],
    ["C10004", "MUG", "Blue mug", "-1", "2024-02-03 09:00:00", "2.50", "101", "Germany"],
    ["C10002", "MUG", "Blue mug", "1", "2024-02-04 09:00:00", "2.50", "102", "France"],
    ["10005", "TEA", "", "2", "2024-02-05 09:00:00", "4.99", "", ""],
    ["10006", "TEA", "Tea tin", "-1", "2024-02-06 09:00:00", "4.99", "102", "France"],
    ["10007", "GIFT", "Free sample", "1", "2024-02-07 09:00:00", "0", "103", "Spain"],
    ["10008", "PEN", "Pen", "3", "2024-02-08 09:00:00", "0.335", "103", "Spain"],
    ["10009", "MUG", "Blue mug", "1", "2024-02-09 09:00:00", "2.50", "103", "Spain"],
    ["10009", "MUG", "Blue mug", "1", "2024-02-09 09:00:00", "2.50", "103", "Spain"],
    ["10010", "MUG", "Bad price", "1", "2024-02-10 09:00:00", "-1", "101", "Germany"],
    ["10011", "MUG", "Bad quantity", "oops", "2024-02-10 09:00:00", "2.5", "101", "Germany"],
    ["10012", "MUG", "Bad timestamp", "1", "not-a-date", "2.5", "101", "Germany"],
    ["", "MUG", "Missing invoice", "1", "2024-02-10 09:00:00", "2.5", "101", "Germany"],
]


def generate(directory: Path) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    batches = {"initial.csv": ROWS, "late.csv": [["09999", "TEA", "Late arriving tea", "2", "2024-01-03 09:00:00", "4.99", "102", "France"]]}
    paths = []
    for name, rows in batches.items():
        path = directory / name
        with path.open("w", newline="") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(HEADERS)
            writer.writerows(rows)
        paths.append(path)
    return paths
