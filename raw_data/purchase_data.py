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
def load_and_process_table(query, engine):
    """
    Runs a SQL query and returns a Pandas DataFrame.
    """
    try:
        df = pd.read_sql_query(query, con=engine)
        return df
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
        return None


# -----------------------------
# SQL Query for Purchase Data
# -----------------------------
purchase_query = """
WITH LineData AS (
    SELECT
        'HISTORY' AS [Status],
        [Document Type],
        [Document No_],
        [Line No_],
        [Shortcut Dimension 1 Code],
        [Buy-from Vendor No_],
        [Type],
        [No_],
        [Location Code],
        [Expected Receipt Date],
        NULL AS [Package Tracking No_],
        [Promised Receipt Date],
        [Planned Receipt Date],
        [Description],
        CASE 
            WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
            ELSE [Qty_ per Unit of Measure]
        END AS [Qty_ per Unit of Measure],
        [Quantity] * CASE 
                        WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                        ELSE [Qty_ per Unit of Measure] 
                     END AS [Quantity],
        [Outstanding Quantity] * CASE 
                                    WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                                    ELSE [Qty_ per Unit of Measure] 
                                 END AS [Outstanding Quantity],
        [Unit Cost (LCY)] / CASE 
                              WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                              ELSE [Qty_ per Unit of Measure] 
                           END AS [Unit Cost (LCY)],
        [Requested Receipt Date],
        CASE 
            WHEN 'HISTORY' = 'HISTORY' THEN ([Quantity] - [Outstanding Quantity]) * [Unit Cost (LCY)]
            ELSE [Quantity] * [Unit Cost (LCY)]
        END AS [Total],
        ([Quantity] - [Outstanding Quantity]) * CASE 
                                                WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                                                ELSE [Qty_ per Unit of Measure] 
                                             END AS [Quantity Delivered]
    FROM
        [dbo].[IPG Photonics Corporation$Purchase History Line]
    WHERE
        [Order Date] > '2019-01-01' AND
        [Quantity] > 0 AND
        [Unit Cost (LCY)] > 0 AND
        ([Document Type] = 1 OR [Document Type] = 5) AND
        ([Type] = 1 OR [Type] = 2 OR [Type] = 4) AND
        NOT ('HISTORY' = 'HISTORY' AND [Quantity] - [Outstanding Quantity] = 0)
    UNION ALL
    SELECT
        'OPEN' AS [Status],
        [Document Type],
        [Document No_],
        [Line No_],
        [Shortcut Dimension 1 Code],
        [Buy-from Vendor No_],
        [Type],
        [No_],
        [Location Code],
        [Expected Receipt Date],
        [Package Tracking No_],
        [Promised Receipt Date],
        [Planned Receipt Date],
        [Description],
        CASE 
            WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
            ELSE [Qty_ per Unit of Measure]
        END AS [Qty_ per Unit of Measure],
        [Quantity] * CASE 
                        WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                        ELSE [Qty_ per Unit of Measure] 
                     END AS [Quantity],
        [Outstanding Quantity] * CASE 
                                    WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                                    ELSE [Qty_ per Unit of Measure] 
                                 END AS [Outstanding Quantity],
        [Unit Cost (LCY)] / CASE 
                              WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                              ELSE [Qty_ per Unit of Measure] 
                           END AS [Unit Cost (LCY)],
        [Requested Receipt Date],
        [Quantity] * [Unit Cost (LCY)] AS [Total],
        ([Quantity] - [Outstanding Quantity]) * CASE 
                                                WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                                                ELSE [Qty_ per Unit of Measure] 
                                             END AS [Quantity Delivered]
    FROM
        [dbo].[IPG Photonics Corporation$Purchase Line]
    WHERE
        [Order Date] > '2019-01-01' AND
        [Quantity] > 0 AND
        [Unit Cost (LCY)] > 0 AND
        ([Document Type] = 1 OR [Document Type] = 5) AND
        ([Type] = 1 OR [Type] = 2 OR [Type] = 4)
),
HeaderData AS (
    SELECT
        [Document Type],
        [No_],
        [Order Date],
        [Posting Date],
        [Assigned User ID],
        [Order Confirmation Date],
        [Purchaser Code]
    FROM
        [dbo].[IPG Photonics Corporation$Purchase History Header]
    WHERE
        [Order Date] > '2018-12-31' AND
        ([Document Type] = 1 OR [Document Type] = 5) AND
        [Buy-from Vendor No_] <> ''
    UNION ALL
    SELECT
        [Document Type],
        [No_],
        [Order Date],
        [Posting Date],
        [Assigned User ID],
        [Order Confirmation Date],
        [Purchaser Code]
    FROM
        [dbo].[IPG Photonics Corporation$Purchase Header]
    WHERE
        [Order Date] > '2018-12-31' AND
        ([Document Type] = 1 OR [Document Type] = 5) AND
        [Buy-from Vendor No_] <> ''
),
ReceiptTable AS (
    SELECT 
        [Line No_],
        [Order No_] AS [Order #],
        [No_] AS [Item #],
        [Posting Date]
    FROM (
        SELECT 
            [Line No_],
            [Order No_],
            [No_],
            [Posting Date],
            ROW_NUMBER() OVER (PARTITION BY [Line No_], [Order No_], [No_] ORDER BY [Posting Date]) AS RowNumber
        FROM [dbo].[IPG Photonics Corporation$Purch_ Rcpt_ Line]
        WHERE [Quantity] > 0
    ) AS InnerQuery
    WHERE InnerQuery.RowNumber = 1 AND YEAR([Posting Date]) > 2017
)
SELECT
    LineData.[Status],
    LineData.[Document Type],
    LineData.[Document No_],
    LineData.[Line No_],
    LineData.[Buy-from Vendor No_],
    LineData.[Type],
    LineData.[No_],
    LineData.[Shortcut Dimension 1 Code] AS [Cost Center],
    LineData.[Location Code],
    LineData.[Expected Receipt Date],
    LineData.[Package Tracking No_],
    LineData.[Promised Receipt Date],
    LineData.[Description],
    LineData.[Qty_ per Unit of Measure],
    LineData.[Quantity],
    LineData.[Outstanding Quantity],
    LineData.[Unit Cost (LCY)],
    LineData.[Requested Receipt Date],
    LineData.[Total],
    LineData.[Planned Receipt Date],
    LineData.[Quantity Delivered],
    HeaderData.[Order Date],
    ReceiptTable.[Posting Date] AS [Receipt Posting Date],
    HeaderData.[Assigned User ID],
    HeaderData.[Order Confirmation Date],
    HeaderData.[Purchaser Code]
FROM
    LineData
JOIN HeaderData
    ON LineData.[Document No_] = HeaderData.[No_]
LEFT JOIN ReceiptTable
    ON LineData.[Document No_] = ReceiptTable.[Order #] 
       AND LineData.[Line No_] = ReceiptTable.[Line No_] 
       AND LineData.[No_] = ReceiptTable.[Item #]
ORDER BY
    LineData.[Document No_], LineData.[Line No_];
"""


# -----------------------------
# Function to Get Purchase Data
# -----------------------------
def get_purchase_data():
    """
    Returns a DataFrame containing the purchase data, which combines
    purchase history/open lines with header and receipt information.
    """
    df = load_and_process_table(query=purchase_query, engine=engine)
    return df


# -----------------------------
# Optional: Direct Execution for Testing
# -----------------------------
if __name__ == "__main__":
    # Set display options for better visibility of data
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)

    purchase_df = get_purchase_data()
    if purchase_df is not None:
        print("Purchase Data Preview:")
        print(purchase_df.head(10))
        print("\nColumn Names:", purchase_df.columns.tolist())
        print("\nTotal records:", len(purchase_df))
