import pandas as pd

class TransformationError(Exception):
    pass


REQUIRED_COLUMNS = {
    "Date", "Open", "High", "Low",
    "Close", "Adj Close", "Volume", "ticker"
}


def transform_prices(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    
    # Flatten MultiIndex columns if present
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0] if col[1] == '' else col[0] for col in data.columns]
    
    if not REQUIRED_COLUMNS.issubset(data.columns):
        missing = REQUIRED_COLUMNS - set(data.columns)
        raise TransformationError(f"Missing columns: {missing}, received {data.columns}")

    # Rename columns
    data = data.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume"
    })

    # Parse date
    data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.date

    # Numeric coercion
    numeric_cols = ["open", "high", "low", "close", "adj_close"]
    for col in numeric_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    data["volume"] = pd.to_numeric(data["volume"], errors="coerce", downcast="signed")

    # Drop invalid rows
    data = data.dropna(subset=["date", "ticker", "volume"])

    # Optional: sort for determinism
    data = data.sort_values(by=["ticker", "date"])

    if data.empty:
        raise TransformationError("All rows were dropped during transformation")

    return data.reset_index(drop=True)

if __name__ == "__main__":
    from extract import extract_ticker_history

    raw = extract_ticker_history("AAPL")
    clean = transform_prices(raw)

    print(clean.dtypes)
    print(clean.head())
