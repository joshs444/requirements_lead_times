import pandas as pd
from raw_data.purchase_data import get_purchase_data
from configured_tables.item_table import final_item_table  # Cached final item table

def load_configured_purchase_data():
    """
    Loads, filters, and merges purchase data with the final item table.

    Steps:
      1. Load the raw purchase data.
      2. Filter rows to include only:
           - Status == 'OPEN'
           - Type == 2
           - Document Type == 1
      3. Rename 'Outstanding Quantity' to 'QTY'.
      4. Keep only columns: No_, Expected Receipt Date, Document No_, QTY.
      5. Remove rows with QTY equal to 0.
      6. Merge on No_ with final_item_table to get the Index.
      7. Return a DataFrame with columns: Index, No_, Expected Receipt Date, Document No_, QTY.
    """
    # Load raw purchase data
    purchase_df = get_purchase_data().copy()

    # Filter for the specified conditions
    purchase_filtered = purchase_df[
        (purchase_df["Status"] == "OPEN") &
        (purchase_df["Type"] == 2) &
        (purchase_df["Document Type"] == 1)
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
