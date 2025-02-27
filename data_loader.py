import pandas as pd

# Set pandas options to display all columns and adjust the display width
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

from configured_tables.bom_table import load_configured_bom_table
from configured_tables.inventory_table import load_configured_inventory_data
from configured_tables.purchase_table import load_configured_purchase_data
from configured_tables.sales_table import load_configured_sales_data
from configured_tables.item_table import final_item_table  # Cached final item table


def load_all_data():
    """
    Loads all required data tables from the configured_tables modules.

    Returns:
        dict: A dictionary containing:
            - 'bom_data': Configured BOM table with Parent and Child indices.
            - 'inventory': Configured aggregated Inventory data.
            - 'purchases': Configured Purchase data.
            - 'sales_orders': Configured Sales data with 'Item Index'.
            - 'final_item_data': Cached Final Item table.
    """
    data = {}

    try:
        data["bom_data"] = load_configured_bom_table()
    except Exception as e:
        print(f"Error loading BOM data: {e}")
        data["bom_data"] = None

    try:
        data["inventory"] = load_configured_inventory_data()
    except Exception as e:
        print(f"Error loading Inventory data: {e}")
        data["inventory"] = None

    try:
        data["purchases"] = load_configured_purchase_data()
    except Exception as e:
        print(f"Error loading Purchase data: {e}")
        data["purchases"] = None

    try:
        data["sales_orders"] = load_configured_sales_data()
    except Exception as e:
        print(f"Error loading Sales data: {e}")
        data["sales_orders"] = None

    try:
        # final_item_table is already computed and cached
        data["final_item_data"] = final_item_table
    except Exception as e:
        print(f"Error loading Final Item data: {e}")
        data["final_item_data"] = None

    return data


if __name__ == "__main__":
    all_data = load_all_data()
    for key, df in all_data.items():
        print(f"\n--- {key} ---")
        if df is not None:
            print("Column Names:", df.columns.tolist())
            print("First 5 Rows:")
            print(df.head(5).to_string(index=False))
        else:
            print("Data not loaded.")
