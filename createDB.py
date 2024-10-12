import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("sqlite:///stocks.db")


def create_stock_table():
    with engine.connect() as conn:
        conn.execute(text())
    print("Stock symbols table created with symbol, name, and sector.")


def populate_stock_symbols(symbols):
    stock_data = []

    for symbol in symbols:
        try:

            stock_info = yf.Ticker(symbol).info
            stock_data.append(
                {
                    "symbol": symbol,
                    "name": stock_info.get("shortName", "N/A"),
                    "sector": stock_info.get("sector", "N/A"),
                }
            )
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

    stock_df = pd.DataFrame(stock_data)
    stock_df.to_sql("stock_symbols", engine, if_exists="replace", index=False)
    print("Stock symbols, names, and sectors inserted into the database.")


def setup_database():

    stock_symbols = ["AAPL"]

    create_stock_table()

    populate_stock_symbols(stock_symbols)


if __name__ == "__main__":
    setup_database()
