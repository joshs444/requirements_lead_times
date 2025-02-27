import pandas as pd
from sqlalchemy import create_engine

# Set display options to show all columns
pd.set_option('display.max_columns', None)

# -----------------------------
# Database Connection Setup
# -----------------------------
DB_TYPE = 'mssql+pyodbc'
DB_SERVER = 'ipgp-ox-dvsql02'
DB_NAME = 'PurchaseREQ_PRODCopy'
DB_USER = 'sql-prsRead'
DB_PASS = 'FhMEzNw7tcjTJGBDktko'
DB_DRIVER = 'ODBC Driver 17 for SQL Server'

# Build the connection string (URL-encode spaces in the driver name)
connection_string = (
    f"{DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_SERVER}/{DB_NAME}"
    f"?driver={DB_DRIVER.replace(' ', '+')}"
)
engine = create_engine(connection_string)

# -----------------------------
# SQL Query for the PR Data
# -----------------------------
pr_query = """
SELECT
    rl.REQID,
    rl.REQType,
    rl.PartNum,
    rl.PartRev,
    rl.CostCenter,
    rl.Description,
    rl.Units,
    rl.OrderQty,
    rl.ShipToLocation,
    rl.RequestDelivery,
    rl.UnitPrice,
    rl.LastDirectCost,
    rl.SubmitDate,
    rl.ExtPrice,
    rl.REQLineID,
    eh.VendorID,
    eh.ContactID,
    eh.Shipping,
    eh.ShipTo,
    eh.PurchaseType,
    eh.SubmitUser,
    eh.PaymentTerms,
    rid.REQDATE,
    rid.Status,
    rid.NAVID,
    rid.Department,
    rs.StatusDesc
FROM dbo.PR_REQLines rl
LEFT JOIN dbo.PREntryHeader eh ON rl.REQID = eh.REQID
LEFT JOIN dbo.PR_REQID rid ON rl.REQID = rid.REQID
LEFT JOIN dbo.PR_REQStatus rs ON rid.Status = rs.StatusID
WHERE rid.REQDATE > '2023-06-01'
  AND (rs.StatusDesc = 'Hold' OR rs.StatusDesc = 'Pending Approval')
  AND rl.REQType = 'Item'
ORDER BY rid.REQDATE DESC;
"""

# -----------------------------
# Helper Function
# -----------------------------
def load_and_process_table(query, engine):
    """
    Runs a SQL query and returns a pandas DataFrame.
    """
    try:
        df = pd.read_sql_query(query, con=engine)
        return df
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
        return None

# -----------------------------
# Function to Get the PR Data
# -----------------------------
def get_pr_data():
    """
    Loads the PR data from SQL by executing the specified query.
    """
    df = load_and_process_table(query=pr_query, engine=engine)
    return df

# -----------------------------
# Optional: Direct Execution for Testing
# -----------------------------
if __name__ == "__main__":
    pr_df = get_pr_data()
    if pr_df is not None:
        print("PR Data Preview:")
        print(pr_df.head())
        print("Column Names:", pr_df.columns.tolist())
        print("Total rows:", len(pr_df))
