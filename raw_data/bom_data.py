import pandas as pd
from sqlalchemy import create_engine

# Set display options to show all columns
pd.set_option('display.max_columns', None)

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
# SQL Query for the BOM Data
# -----------------------------
bom_query = """
SELECT 
    p.[Production BOM No_],
    p.[No_] AS [Component No_],
    SUM(p.[Quantity per]) AS [Total]
FROM 
    [dbo].[IPG Photonics Corporation$Production BOM Line] p
INNER JOIN 
    [dbo].[IPG Photonics Corporation$Item] i
    ON p.[Production BOM No_] = i.[No_] 
       AND p.[Version Code] = i.[Revision No_]
WHERE 
    p.[Quantity per] <> 0
    AND p.[No_] IS NOT NULL
GROUP BY 
    p.[Production BOM No_],
    p.[No_];

"""

# -----------------------------
# Function to Get the BOM Data
# -----------------------------
def get_bom_data():
    """
    Loads the BOM data from SQL by executing the specified query.
    """
    df = load_and_process_table(query=bom_query, engine=engine)
    return df

# -----------------------------
# Optional: Direct Execution for Testing
# -----------------------------
if __name__ == "__main__":
    bom_df = get_bom_data()
    if bom_df is not None:
        print("BOM Data Preview:")
        print(bom_df.head())
        print("Column Names:", bom_df.columns.tolist())
        print("Total rows:", len(bom_df))
