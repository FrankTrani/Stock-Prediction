Here’s the updated version of your `README.md` based on your request:

---

# **Small Python Project to Predict Stock Movement and Best Buy Times**

## **Use**

When you run the script, it will create:
- A **SQLite database file (`stocks.db`)** to store stock data and analysis results.
- A **logs folder (`logs/`)** with separate logs for:
  - **Errors** (`logs/errors.log`)
  - **Info** (`logs/info.log`)

You can turn off logging by setting:

```python
LOGGING_ENABLED = False  
```

---

## **How It Works**

1. The script reads the list of stock symbols stored in the **`stock_symbols`** table within the database.

2. For each stock, it fetches **close prices from the past month** using the yFinance API.

3. If the stock's **price follows a bell curve (normal distribution)**:
   - It calculates the **mean** and **standard deviation**.

4. The script computes the **Z-score** for the latest price:
   - **Lower Z-scores** indicate the stock price is lower than usual, which could signal a **good time to buy** as there's a higher chance for the stock to bounce back.

---

## **Usage**

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. (Optional) Create a virtual environment:

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Run the analysis:

   ```bash
   python your_script_name.py
   ```

---

## **Database Structure**

- **`stock_symbols`**: Stores stock symbols, company names, and sectors.
- **`current`**: Stores stocks with normal distribution and their Z-scores.
- **`abnormal_stocks`**: Stores stocks that do not follow a normal distribution or have insufficient data.

---

## **Logging**

Logs are divided into two files:
- **Info Logs:** Track batch progress and analysis status (`logs/info.log`).
- **Error Logs:** Record warnings and exceptions (`logs/errors.log`).

---

## **Notes**
- **Lower Z-Score = Better Buy Opportunity:** A lower Z-score suggests the stock is trading below its mean price, which could indicate a good time to buy.
- Adjust the **`BATCH_SIZE`** if processing too many symbols causes performance issues.
- Invalid symbols like `"FB"`, `"BRK.B"`, or `"SIVB"` are excluded from analysis. Update the `invalid_symbols` list in the code to add/remove any symbols.

---

This `README.md` provides a concise overview of how the script works, what it does, and how to use it. Let me know if you need any further changes!