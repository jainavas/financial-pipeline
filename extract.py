import yfinance as yf
import pandas as pd

class ExtractionError(Exception):
    pass


def extract_ticker_history(
    ticker: str,
    start: str | None = None,
    end: str | None = None
) -> pd.DataFrame:
    try:
        df = yf.download(
            ticker,
            start=start,
            end=end,
            progress=False,
            auto_adjust=False,
            threads=False
        )
    except Exception as e:
        raise ExtractionError(f"Data download failed: {e}")

    if df.empty:
        raise ExtractionError("No data returned")

    df = df.reset_index()
    df["ticker"] = ticker

    return df

if __name__ == "__main__":
    try:
        df = extract_ticker_history("AAPL")
        print(df.head())
    except ExtractionError as e:
        print(f"Scraping failed: {e}")