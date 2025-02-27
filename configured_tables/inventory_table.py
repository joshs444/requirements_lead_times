import pandas as pd
from raw_data.inventory_data import get_inventory_data  # Function to load raw inventory data
from configured_tables.item_table import final_item_table  # Cached final item table

def load_configured_inventory_data():
    """
    Configures the inventory data as follows:
      - Loads raw inventory data.
      - Aggregates by 'Item No_' and sums 'Quantity'.
      - Merges the aggregated inventory data with the cached final item table using:
            final_item_table["No_"] = Inventory["Item No_"]
      - Fills missing inventory quantities with 0.
      - Returns a DataFrame with only 'Item No_', 'Index', and 'Total Quantity'.
    """
    # Load raw inventory data
    inv_df = get_inventory_data().copy()

    # Aggregate inventory by 'Item No_' summing the 'Quantity'
    inv_agg = inv_df.groupby("Item No_", as_index=False)["Quantity"].sum()
    inv_agg = inv_agg.rename(columns={"Quantity": "Total Quantity"})

    # Merge aggregated inventory with the final item table mapping
    merged = pd.merge(
        final_item_table[["No_", "Index"]],
        inv_agg,
        left_on="No_",
        right_on="Item No_",
        how="left"
    )

    # Fill missing Total Quantity values with 0
    merged["Total Quantity"] = merged["Total Quantity"].fillna(0)

    # Drop the duplicate inventory key column (from inv_agg)
    merged.drop(columns=["Item No_"], inplace=True)

    # Rename 'No_' to 'Item No_' and select the desired columns
    final_inv = merged.rename(columns={"No_": "Item No_"})[["Item No_", "Index", "Total Quantity"]].copy()
    return final_inv

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    configured_inventory = load_configured_inventory_data()
    print("Configured Inventory Data:")
    print(configured_inventory.head())
    print("\nColumn Names:", configured_inventory.columns.tolist())
    print("\nTotal records:", len(configured_inventory))
