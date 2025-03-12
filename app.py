import yfinance as yf
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from scipy import stats
from datetime import datetime, timedelta
import logging
import logging.handlers
from tqdm import tqdm
import time
import os
import re
import create

# **Logging Control Variable**
LOGGING_ENABLED = True  # Set to False to disable logging

# Ensure the logs directory exists
os.makedirs("logs", exist_ok=True)

# Configure logging
logger = logging.getLogger()

if LOGGING_ENABLED:
    logger.setLevel(logging.DEBUG)  # Set the root logger level to DEBUG

    # Formatter for all handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s"
    )

    # Handler for DEBUG and INFO messages
    info_handler = logging.handlers.RotatingFileHandler(
        "logs/info.log", maxBytes=5 * 1024 * 1024, backupCount=0
    )
    info_handler.setLevel(logging.DEBUG)
    info_handler.setFormatter(formatter)
    info_handler.addFilter(lambda record: record.levelno <= logging.INFO)

    # Handler for WARNING and ERROR messages
    error_handler = logging.handlers.RotatingFileHandler(
        "logs/errors.log", maxBytes=5 * 1024 * 1024, backupCount=0
    )
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.handlers = []  # Clear existing handlers
    logger.addHandler(info_handler)
    logger.addHandler(error_handler)
else:
    # **Disable all logging by setting the level to a high value**
    logger.setLevel(logging.CRITICAL + 1)
    logger.handlers = []  # Remove any handlers that might have been added

# Suppress logging from 'yfinance' and 'urllib3' to reduce clutter
logging.getLogger("yfinance").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

# Create a database engine
engine = create_engine(
    "sqlite:///stocks.db", echo=False, connect_args={"check_same_thread": False}
)

# Define batch size for processing
BATCH_SIZE = 50


def get_stock_symbols():
    """
    Fetches stock symbols from the 'stock_symbols' table in the database.

    Returns:
        DataFrame: A pandas DataFrame with 'symbol', 'name', and 'sector' columns.
    """
    logging.info("Fetching stock symbols from the database.")
    try:
        query = "SELECT symbol, name, sector FROM stock_symbols"
        return pd.read_sql(query, engine)
    except Exception as e:
        logging.error(
            f"Error fetching stock symbols from the database: {e}", exc_info=True
        )
        return pd.DataFrame()


def fetch_stock_data(symbols):
    """
    Fetches stock data for given symbols using the yfinance API.

    Args:
        symbols (list): List of stock symbols.

    Returns:
        DataFrame: A pandas DataFrame with stock data, or None if no data is found.
    """
    logging.info(f"Fetching data for symbols: {symbols}")
    try:
        stock_data = yf.download(
            tickers=symbols,
            period="1mo",  # Use '1mo' period to fetch data for the last month
            interval="1d",
            progress=False,
            threads=True,
            auto_adjust=False,
            prepost=False,
            proxy=None,
        )
        if stock_data.empty:
            logging.warning(f"No stock data found for symbols: {symbols}")
            return None
        logging.debug(f"Fetched data columns: {stock_data.columns}")
        return stock_data
    except Exception as e:
        logging.error(
            f"Error fetching stock data for symbols {symbols}: {e}", exc_info=True
        )
        return None


def calculate_log_returns(prices, symbol):
    """
    Calculates the log returns for a stock's price data.

    Args:
        prices (Series): Pandas Series of stock prices.
        symbol (str): Stock symbol.

    Returns:
        Series: Log returns as a pandas Series.
    """
    try:
        if prices.isnull().any() or (prices == 0).any():
            logging.warning(f"Prices contain NaN or zero values for {symbol}.")
            return pd.Series(dtype=float)
        log_returns = np.log(prices / prices.shift(1)).dropna()
        logging.debug(f"Calculated log returns for {symbol}:\n{log_returns}")
        return log_returns
    except Exception as e:
        logging.error(f"Error calculating log returns for {symbol}: {e}", exc_info=True)
        return pd.Series(dtype=float)


def test_normality(log_returns, symbol):
    """
    Tests if the log returns follow a normal distribution using the Shapiro-Wilk test.

    Args:
        log_returns (Series): Log returns of the stock.
        symbol (str): Stock symbol.

    Returns:
        bool: True if the log returns follow a normal distribution, False otherwise.
    """
    try:
        if len(log_returns) < 3:
            logging.debug(f"Not enough data points for normality test for {symbol}.")
            return False
        if log_returns.std() == 0:
            logging.debug(
                f"Standard deviation is zero for {symbol}; cannot perform normality test."
            )
            return False
        stat, p_value = stats.shapiro(log_returns)
        logging.debug(f"Shapiro-Wilk test for {symbol}: stat={stat}, p_value={p_value}")
        result = p_value > 0.05
        logging.debug(f"Normality test result for {symbol}: {result}")
        return result
    except Exception as e:
        logging.error(
            f"Error performing normality test for {symbol}: {e}", exc_info=True
        )
        return False


def calculate_z_score(price_today, mean, std_dev, symbol):
    """
    Calculates the Z-score for the current price.

    Args:
        price_today (float): Today's price.
        mean (float): Mean of the prices.
        std_dev (float): Standard deviation of the prices.
        symbol (str): Stock symbol.

    Returns:
        float: The Z-score, or None if the calculation fails.
    """
    try:
        if std_dev == 0:
            logging.warning(
                f"Standard deviation is zero for {symbol}; cannot calculate Z-score."
            )
            return None
        z_score = (price_today - mean) / std_dev
        logging.debug(f"Calculated Z-score for {symbol}: {z_score}")
        return z_score
    except Exception as e:
        logging.error(f"Error calculating Z-score for {symbol}: {e}", exc_info=True)
        return None


def create_tables():
    """
    Calls the create_tables function from the 'create.py' module.
    Logs success or any errors encountered during execution.
    """
    logging.info("Creating tables using 'create.py'.")
    try:
        create.create_tables()
        logging.info("Tables created successfully.")
    except Exception as e:
        logging.error(f"Error creating tables: {e}", exc_info=True)


def analyze_stocks():
    """
    Main function to analyze stock data and store results in the database.
    """
    create_tables()
    stock_info = get_stock_symbols()
    if stock_info.empty:
        logging.error("No stock symbols found in the database.")
        return

    symbols = stock_info["symbol"].tolist()
    names_dict = stock_info.set_index("symbol")["name"].to_dict()
    sectors_dict = stock_info.set_index("symbol")["sector"].to_dict()

    # Remove invalid symbols
    invalid_symbols = ["FB", "BRK.B", "SIVB"]

    # **Filter out tickers ending with 'W', 'R', 'U', 'P', etc.**
    pattern = re.compile(r".+[WRUP]$")
    symbols = [
        sym for sym in symbols if sym not in invalid_symbols and not pattern.match(sym)
    ]

    total_symbols = len(symbols)
    logging.info(f"Starting analysis of {total_symbols} symbols.")

    # Initialize lists to collect results
    current_list = []
    abnormal_stocks_list = []

    with tqdm(total=total_symbols, desc="Processing symbols") as pbar:
        # Process symbols in batches
        for i in range(0, total_symbols, BATCH_SIZE):
            batch_symbols = symbols[i : i + BATCH_SIZE]
            logging.info(f"Processing batch symbols: {batch_symbols}")

            # Fetch data for batch
            stock_data = fetch_stock_data(batch_symbols)

            if stock_data is None or stock_data.empty:
                logging.warning(f"No data fetched for symbols: {batch_symbols}")
                # Mark all symbols in batch as abnormal
                for symbol in batch_symbols:
                    name = names_dict.get(symbol, "")
                    sector = sectors_dict.get(symbol, "")
                    abnormal_stocks_list.append(
                        {"symbol": symbol, "name": name, "sector": sector}
                    )
                    pbar.update(1)
                continue

            # Swap levels to have symbols at the outer level
            if isinstance(stock_data.columns, pd.MultiIndex):
                stock_data = stock_data.swaplevel(0, 1, axis=1)
            else:
                logging.error("Unexpected data structure returned by yfinance.")
                for symbol in batch_symbols:
                    name = names_dict.get(symbol, "")
                    sector = sectors_dict.get(symbol, "")
                    abnormal_stocks_list.append(
                        {"symbol": symbol, "name": name, "sector": sector}
                    )
                    pbar.update(1)
                continue

            for symbol in batch_symbols:
                name = names_dict.get(symbol, "")
                sector = sectors_dict.get(symbol, "")

                logging.info(f"Processing symbol: {symbol}")

                try:
                    # Get data for symbol
                    if symbol in stock_data.columns.get_level_values(0):
                        prices = stock_data[symbol]["Close"].dropna()
                    else:
                        logging.warning(f"No data for {symbol}")
                        abnormal_stocks_list.append(
                            {"symbol": symbol, "name": name, "sector": sector}
                        )
                        pbar.update(1)
                        continue

                    if prices.empty or len(prices) < 2:
                        logging.warning(
                            f"Not enough data for {symbol}. Adding to abnormal_stocks."
                        )
                        abnormal_stocks_list.append(
                            {"symbol": symbol, "name": name, "sector": sector}
                        )
                        pbar.update(1)
                        continue

                    log_returns = calculate_log_returns(prices, symbol)

                    if log_returns.empty or len(log_returns) < 3:
                        logging.warning(
                            f"Not enough log return data for {symbol}. Adding to abnormal_stocks."
                        )
                        abnormal_stocks_list.append(
                            {"symbol": symbol, "name": name, "sector": sector}
                        )
                        pbar.update(1)
                        continue

                    if test_normality(log_returns, symbol):
                        mean_price = prices.mean()
                        std_dev_price = prices.std()
                        price_today = prices.iloc[-1]

                        z_score = calculate_z_score(
                            price_today, mean_price, std_dev_price, symbol
                        )
                        if z_score is not None:
                            logging.info(
                                f"{symbol} follows a normal distribution. Z-Score: {z_score:.2f}"
                            )
                            current_list.append(
                                {"symbol": symbol, "name": name, "z_score": z_score}
                            )
                        else:
                            logging.warning(
                                f"Could not calculate Z-score for {symbol} due to zero standard deviation."
                            )
                            abnormal_stocks_list.append(
                                {"symbol": symbol, "name": name, "sector": sector}
                            )
                    else:
                        logging.info(f"{symbol} does not follow a normal distribution.")
                        abnormal_stocks_list.append(
                            {"symbol": symbol, "name": name, "sector": sector}
                        )

                    pbar.update(1)
                except Exception as e:
                    logging.error(
                        f"An error occurred while processing {symbol}: {e}",
                        exc_info=True,
                    )
                    abnormal_stocks_list.append(
                        {"symbol": symbol, "name": name, "sector": sector}
                    )
                    pbar.update(1)
                    continue

            time.sleep(0.1)  # Small delay to prevent overloading the data source

    # After processing all batches, insert data into the database
    logging.info(f"Number of records in current_list: {len(current_list)}")
    if current_list:
        df_current = pd.DataFrame(current_list)
        logging.debug(f"DataFrame to insert into 'current' table:\n{df_current}")
        try:
            with engine.begin() as conn:
                df_current.to_sql("current", conn, if_exists="append", index=False)
            logging.info(f"Inserted {len(df_current)} records into 'current' table.")
        except Exception as e:
            logging.error(f"Error inserting into 'current' table: {e}", exc_info=True)
    else:
        logging.warning(
            "current_list is empty. No data to insert into 'current' table."
        )

    if abnormal_stocks_list:
        df_abnormal = pd.DataFrame(abnormal_stocks_list)
        try:
            with engine.begin() as conn:
                df_abnormal.to_sql(
                    "abnormal_stocks", conn, if_exists="append", index=False
                )
            logging.info(
                f"Inserted {len(df_abnormal)} records into 'abnormal_stocks' table."
            )
        except Exception as e:
            logging.error(
                f"Error inserting into 'abnormal_stocks' table: {e}", exc_info=True
            )
    else:
        logging.info("No abnormal stocks to insert.")

    logging.info("Analysis complete.")


if __name__ == "__main__":
    analyze_stocks()
