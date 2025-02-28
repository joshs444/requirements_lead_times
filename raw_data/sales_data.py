import pandas as pd
from sqlalchemy import create_engine

# -----------------------------
# Database Connection Setup
# -----------------------------
DB_TYPE = 'mssql+pyodbc'
DB_HOST = 'IPGP-OX-AGP02'  # Replace with your correct server/host if needed
DB_NAME = 'IPG-DW-PROTOTYPE'
DB_DRIVER = 'ODBC Driver 17 for SQL Server'
connection_string = f"{DB_TYPE}://@{DB_HOST}/{DB_NAME}?driver={DB_DRIVER}&trusted_connection=yes"
engine = create_engine(connection_string)

# -----------------------------
# Helper Function
# -----------------------------
def load_and_process_table(query, engine, rename_cols=None, additional_processing=None, **kwargs):
    """
    General-purpose function to run a SQL query and return a pandas DataFrame
    with optional renaming or post-processing.
    """
    try:
        df = pd.read_sql_query(query, con=engine)
        if rename_cols:
            df = df.rename(columns=rename_cols)
        if additional_processing:
            df = additional_processing(df, **kwargs)
        return df
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
        return None

# -----------------------------
# The FULL Sales Query
# -----------------------------
sales_query = """
SELECT
    [No_],
    [Sell-to Customer Name] AS Customer,
    [Document No_],
    CAST([Planned Delivery Date] AS date) AS [Date],
    SUM([Outstanding Quantity]) AS QTY
FROM dbo.stg_sales_header_booking_us_t
WHERE
    [Type] = 2
    AND [Outstanding Quantity] > 0
    AND [Planned Delivery Date] > DATEADD(MONTH, -6, GETDATE())
    AND [Document No_] IN ('S107738', 'S111036A')
GROUP BY
    [No_],
    [Sell-to Customer Name],
    [Document No_],
    CAST([Planned Delivery Date] AS date)
ORDER BY [No_], [Document No_], [Date];
"""

def get_sales_data():
    """
    Returns the DataFrame containing the sales data loaded from the SQL query above.
    """
    df = load_and_process_table(query=sales_query, engine=engine)
    return df

def get_sales_data_with_index():
    """
    Returns the sales data without merging with item data.
    (Index matching has been removed.)
    """
    # Simply return the raw sales data
    return get_sales_data()

# -----------------------------
# Execution (Optional)
# -----------------------------
if __name__ == "__main__":
    # Set display options for better visibility of data
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 250)

    sales_df = get_sales_data_with_index()
    if sales_df is not None:
        print("Sales Data Preview (first 10 rows):")
        print(sales_df.head(10))
        print("\nColumn Names:", sales_df.columns.tolist())
        print("\nTotal records:", len(sales_df))