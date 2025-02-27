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
# The Vendor Query
# -----------------------------
vendor_query = """
SELECT 
    [No_],
    [Name],
    [City],
    [Contact],
    [Phone No_],
    [Vendor Posting Group],
    [Country_Region Code],
    [Gen_ Bus_ Posting Group],
    [County],
    [E-Mail],
    [Federal ID No_]
FROM [dbo].[IPG Photonics Corporation$Vendor];
"""


# -----------------------------
# get_vendor_data() Function
# -----------------------------
def get_vendor_data():
    """
    Returns the DataFrame containing the Vendor data,
    transformed similarly to the provided M code.
    """
    df = load_and_process_table(query=vendor_query, engine=engine)
    return df


# -----------------------------
# Optional Direct Execution for Testing
# -----------------------------
if __name__ == "__main__":
    # Set display options to show all columns and adjust the width
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 250)

    vendor_df = get_vendor_data()
    if vendor_df is not None:
        print("Vendor Data Preview (first 10 rows):")
        print(vendor_df.head(10))
        print("\nColumn Names:", vendor_df.columns.tolist())
        print("\nTotal records:", len(vendor_df))
