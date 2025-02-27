import pandas as pd
from raw_data.sales_data import get_sales_data
from configured_tables.item_table import final_item_table  # Cached final item table

def load_configured_sales_data():
    """
    Loads raw sales data and adds an 'Item Index' column by referencing
    the final item table's 'Index' for each item number.

    Steps:
      1. Load raw sales data (which contains 'No_' for item numbers).
      2. Build a mapping dictionary from final_item_table, mapping No_ -> Index.
      3. Create a new column 'Item Index' by mapping sales_df['No_'] to the item index.
      4. Fill missing mappings with 0 (or another value) and convert to int.

    Returns:
        A DataFrame containing the original sales columns plus 'Item Index'.
    """
    # Load the raw sales data
    sales_df = get_sales_data().copy()

    # Create a mapping dictionary from the final item table: No_ -> Index
    item_index_map = (
        final_item_table
        .set_index("No_")["Index"]
        .astype(int)          # Ensure it's an integer
        .to_dict()
    )

    # Map the 'No_' in the sales data to the item index
    sales_df["Index"] = sales_df["No_"].map(item_index_map).fillna(0).astype(int)

    return sales_df

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    configured_sales = load_configured_sales_data()
    print("Configured Sales Data with 'Item Index':")
    print(configured_sales.head())
    print("\nColumn Names:", configured_sales.columns.tolist())
    print("\nTotal rows:", len(configured_sales))
