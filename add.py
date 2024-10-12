import yfinance as yf
from tqdm import tqdm
from sqlalchemy import create_engine, text
import logging

# Configure logging to display warnings and errors
logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Create a SQLite database engine
engine = create_engine("sqlite:///stocks.db", echo=False)


def read_stock_symbols_from_file(filename):
    """
    Reads stock symbols from a text file.

    Args:
        filename (str): The name of the file containing stock symbols, one per line.

    Returns:
        list: A list of stock symbols read from the file. If the file is not found, returns an empty list.
    """
    try:
        with open(filename, "r") as file:
            # Read symbols, stripping whitespace and ignoring empty lines
            stock_symbols = [line.strip() for line in file if line.strip()]
        return stock_symbols
    except FileNotFoundError:
        # Log an error if the file is not found
        logging.error(f"Error: {filename} not found.")
        return []


def insert_stock_to_db(symbol):
    """
    Inserts a stock symbol into the 'stock_symbols' table in the database.

    Args:
        symbol (str): The stock symbol to insert.

    Notes:
        Uses 'INSERT OR IGNORE' to avoid duplicate entries.
    """
    try:
        query = text(
            """
            INSERT OR IGNORE INTO stock_symbols (symbol)
            VALUES (:symbol)
            """
        )
        # Execute the query within a database transaction
        with engine.begin() as conn:
            conn.execute(query, {"symbol": symbol})
    except Exception as e:
        # Log any exceptions that occur during the insert operation
        logging.error(f"Error inserting stock {symbol}: {e}")


def add_stocks_with_progress(stock_symbols):
    """
    Adds stock symbols to the database, displaying a progress bar.

    Args:
        stock_symbols (list): A list of stock symbols to add to the database.
    """
    print(f"Adding {len(stock_symbols)} stock symbols to the database...")

    # Iterate over stock symbols with a progress bar
    for stock in tqdm(stock_symbols, desc="Processing Stocks", unit="stock"):
        insert_stock_to_db(stock)


def main():
    """
    Main function to read stock symbols from a file and insert them into the database.
    """
    # Read stock symbols from 'new.txt'
    stock_symbols = read_stock_symbols_from_file("new.txt")

    if stock_symbols:
        # Add stock symbols to the database with progress display
        add_stocks_with_progress(stock_symbols)
    else:
        # Print message if no symbols were found in the file
        print("No stock symbols found in new.txt.")


# Run the main function if the script is executed directly
if __name__ == "__main__":
    main()
