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
# The Item Query
# -----------------------------
item_query = """
WITH ItemCTE AS (
    SELECT 
         ROW_NUMBER() OVER (ORDER BY [No_], [Revision No_]) AS [Index],
         [No_],
         [Description],
         [Inventory Posting Group],
         [Unit Cost],
         CAST(
             CASE 
                 WHEN ISNUMERIC([Lead Time Calculation]) = 1 
                      AND [Lead Time Calculation] IS NOT NULL 
                 THEN CAST([Lead Time Calculation] AS FLOAT) * 7
                 ELSE 0 
             END AS INT) AS [Lead Time (Days)],
         [Global Dimension 1 Code],
         [Replenishment System],
         [Revision No_],
         [Item Source],
         [Common Item No_]
    FROM [dbo].[IPG Photonics Corporation$Item]
    WHERE [No_] <> ''
)
SELECT
    [Index],
    [No_],
    [Description],
    [Inventory Posting Group],
    [Unit Cost],
    [Lead Time (Days)],
    [Global Dimension 1 Code],
    CASE 
         WHEN [Replenishment System] = 0 THEN 'Purchase'
         WHEN [Replenishment System] = 1 THEN 'Output'
         WHEN [Replenishment System] = 2 THEN 'Assembly'
         ELSE 'Unknown'
    END AS [Replenishment System],
    [Revision No_] AS [Rev #],
    CASE CAST([Item Source] AS varchar(10))
         WHEN '0' THEN ''
         WHEN '3' THEN 'Made In-House'
         WHEN '1' THEN 'Third Party Purchase'
         WHEN '2' THEN 'Interco Purchase'
         ELSE CAST([Item Source] AS varchar(10))
    END AS [Item Source],
    [Common Item No_]
FROM ItemCTE
ORDER BY [Index];
"""


# -----------------------------
# get_item_data() Function
# -----------------------------
def get_item_data():
    """
    Returns the DataFrame containing the Item data,
    transformed similarly to the provided M code.
    """
    df = load_and_process_table(query=item_query, engine=engine)
    return df


# -----------------------------
# Optional Direct Execution for Testing
# -----------------------------
if __name__ == "__main__":
    # Set display options to show all columns and adjust the width
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 250)

    item_df = get_item_data()
    if item_df is not None:
        print("Item Data Preview (first 10 rows):")
        print(item_df.head(10))
        print("\nColumn Names:", item_df.columns.tolist())
        print("\nTotal records:", len(item_df))