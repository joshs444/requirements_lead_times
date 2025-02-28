import pandas as pd
from raw_data.purchase_data import get_purchase_data
from configured_tables.item_table import final_item_table  # Cached final item table
from datetime import datetime
from dateutil.relativedelta import relativedelta

def load_configured_purchase_data():
    """
    Loads, filters, and merges purchase data with the final item table.

    Steps:
      1. Load the raw purchase data.
      2. Convert "Expected Receipt Date" to datetime.
      3. Filter rows to include only:
           - Status == 'OPEN'
           - Type == 2
           - Document Type == 1
           - Expected Receipt Date > today + 3 months and < today + 1 year
      4. Rename 'Outstanding Quantity' to 'QTY'.
      5. Keep only columns: No_, Expected Receipt Date, Document No_, QTY.
      6. Remove rows with QTY equal to 0.
      7. Merge on No_ with final_item_table to get the Index.
      8. Return a DataFrame with columns: Index, No_, Expected Receipt Date, Document No_, QTY.
    """
    # Load raw purchase data
    purchase_df = get_purchase_data().copy()

    # Define today and the date thresholds
    today = datetime.today().date()
    three_months_later = today + relativedelta(months=3)
    one_year_later = today + relativedelta(years=1)

    # Convert "Expected Receipt Date" to datetime
    purchase_df["Expected Receipt Date"] = pd.to_datetime(purchase_df["Expected Receipt Date"])

    # Filter for the specified conditions, including the new date range filter
    purchase_filtered = purchase_df[
        (purchase_df["Status"] == "OPEN") &
        (purchase_df["Type"] == 2) &
        (purchase_df["Document Type"] == 1) &
        (purchase_df["Expected Receipt Date"].dt.date > three_months_later) &
        (purchase_df["Expected Receipt Date"].dt.date < one_year_later)
    ]

    # Rename 'Outstanding Quantity' to 'QTY'
    purchase_filtered = purchase_filtered.rename(columns={"Outstanding Quantity": "QTY"})

    # Keep only the necessary columns
    purchase_filtered = purchase_filtered[["No_", "Expected Receipt Date", "Document No_", "QTY"]]

    # Remove rows where QTY is 0
    purchase_filtered = purchase_filtered[purchase_filtered["QTY"] != 0]

    # Merge with the final item table to get the Index
    merged = pd.merge(
        purchase_filtered,
        final_item_table[["No_", "Index"]],
        on="No_",
        how="left"
    )

    # Reorder columns: Index, No_, Expected Receipt Date, Document No_, QTY
    final_purchase = merged[["Index", "No_", "Expected Receipt Date", "Document No_", "QTY"]].copy()

    return final_purchase

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    configured_purchase = load_configured_purchase_data()
    print("Configured Purchase Data:")
    print(configured_purchase.head())
    print("\nColumn Names:", configured_purchase.columns.tolist())
    print("\nTotal rows:", len(configured_purchase))