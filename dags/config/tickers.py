"""
Configuration for stock tickers to process.
"""

# Tech giants
TECH_TICKERS = [
    "AAPL",   # Apple
    "MSFT",   # Microsoft
    "GOOGL",  # Alphabet
    "AMZN",   # Amazon
    "META",   # Meta
    "NVDA",   # NVIDIA
    "TSLA",   # Tesla
]

# Financial sector
FINANCE_TICKERS = [
    "JPM",    # JPMorgan Chase
    "BAC",    # Bank of America
    "WFC",    # Wells Fargo
    "GS",     # Goldman Sachs
]

# Healthcare
HEALTHCARE_TICKERS = [
    "JNJ",    # Johnson & Johnson
    "UNH",    # UnitedHealth
    "PFE",    # Pfizer
]

# Energy
ENERGY_TICKERS = [
    "XOM",    # Exxon Mobil
    "CVX",    # Chevron
]

# Default list for daily pipeline
DEFAULT_TICKERS = TECH_TICKERS + FINANCE_TICKERS

# All tickers for full refresh
ALL_TICKERS = DEFAULT_TICKERS + HEALTHCARE_TICKERS + ENERGY_TICKERS