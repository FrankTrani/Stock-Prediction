import sqlite3
import pandas as pd
from tabulate import tabulate

def retrieve_and_display_stocks():
    # Connect to the stocks.db database
    try:
        conn = sqlite3.connect('stocks.db')
        print("Connected to the database successfully.")
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return

    try:
        # Retrieve data from the 'current' table
        query = "SELECT symbol, z_score FROM current"
        df = pd.read_sql_query(query, conn)

        # Ensure z_score is of type float
        df['z_score'] = df['z_score'].astype(float)

        # Filter stocks with z_score ≤ -2
        candidates = df[df['z_score'] <= -2].copy()

        # Sort the DataFrame by z_score in descending order
        # This will sort from higher (less negative) to lower (more negative) z_scores
        candidates = candidates.sort_values(by='z_score', ascending=False)

        # Display the results using tabulate for nicer formatting
        print("\n*** Stocks with Z-Score ≤ -2 ***")
        if not candidates.empty:
            print(tabulate(candidates[['symbol', 'z_score']], headers='keys', tablefmt='psql', showindex=False))
        else:
            print("No stocks found with Z-Score ≤ -2.")

    except Exception as e:
        print(f"Error retrieving or processing data: {e}")

    finally:
        # Close the database connection
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    retrieve_and_display_stocks()
