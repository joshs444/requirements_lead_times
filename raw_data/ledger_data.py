import pandas as pd
from sqlalchemy import create_engine

# -----------------------------
# Database Connection Setup
# -----------------------------
DB_TYPE = 'mssql+pyodbc'
DB_HOST = 'IPGP-OX-AGP02'  # Update if needed
DB_NAME = 'IPG-DW-PROTOTYPE'
DB_DRIVER = 'ODBC Driver 17 for SQL Server'
connection_string = f"{DB_TYPE}://@{DB_HOST}/{DB_NAME}?driver={DB_DRIVER}&trusted_connection=yes"
engine = create_engine(connection_string)


# -----------------------------
# Helper Function
# -----------------------------
def load_and_process_table(query, engine, rename_cols=None, additional_processing=None, **kwargs):
    """
    General-purpose function to run a SQL query and return a pandas DataFrame.
    Allows optional renaming or post-processing.
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
# SQL Query for the Item Ledger Entry Table
# -----------------------------
ledger_query = """
SELECT 
    [Item No_],
    [Posting Date],
    [Entry Type],
    [Document No_],
    [Location Code],
    [Quantity],
    [Global Dimension 1 Code]
FROM [dbo].[IPG Photonics Corporation$Item Ledger Entry]
WHERE [Posting Date] >= DATEADD(YEAR, -3, GETDATE())
  AND [Entry Type] <> 4;
"""


# -----------------------------
# Function to Get the Item Ledger Data
# -----------------------------
def get_item_ledger_data():
    """
    Loads the Item Ledger Entry data from SQL,
    filtering for rows with Posting Date in the last 3 years
    and excluding rows with Entry Type equal to 4.
    """
    df = load_and_process_table(query=ledger_query, engine=engine)
    return df


# -----------------------------
# Optional: Direct Execution for Testing
# -----------------------------
if __name__ == "__main__":
    # Set display options to show all columns and a wide display
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)

    ledger_df = get_item_ledger_data()
    if ledger_df is not None:
        print("Item Ledger Data Preview:")
        print(ledger_df.head(10))
        print("\nColumn Names:", ledger_df.columns.tolist())
        print("\nTotal rows:", len(ledger_df))
