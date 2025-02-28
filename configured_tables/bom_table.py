import pandas as pd
from raw_data.bom_data import get_bom_data
from configured_tables.item_table import final_item_table  # Cached final item table

def load_configured_bom_table():
    """
    Configures the BOM table as follows:
      - Loads raw BOM data.
      - Uses the cached final item table to look up each item's Index and Purchase/Output value.
      - Filters BOM rows to keep only those where the Production BOM item's Purchase/Output equals "Output".
      - Creates new columns:
          Parent Index = Index of the Production BOM item
          Child Index  = Index of the Component item
      - Keeps the original columns (Production BOM No_, Component No_, Total)
        and returns them in this order: [Production BOM No_, Component No_, Total, Parent Index, Child Index].
    """
    # Load a copy of the raw BOM data
    bom_df = get_bom_data().copy()

    # Create mapping dictionaries from the cached final item table
    # Convert 'Index' to integer to ensure type consistency
    item_index_map = final_item_table.set_index("No_")["Index"].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int).to_dict()
    item_po_map = final_item_table.set_index("No_")["Purchase/Output"].to_dict()

    # Filter BOM rows: only keep rows where the Production BOM item's Purchase/Output is "Output"
    bom_df["Prod_PO"] = bom_df["Production BOM No_"].map(item_po_map)
    bom_df = bom_df[bom_df["Prod_PO"] == "Output"].copy()

    # Create new columns "Parent Index" and "Child Index" from the mapping
    bom_df["Parent Index"] = bom_df["Production BOM No_"].map(item_index_map)
    bom_df["Child Index"] = bom_df["Component No_"].map(item_index_map)

    # Ensure indices are integers, replacing any missing values with 0
    bom_df["Parent Index"] = bom_df["Parent Index"].fillna(0).astype(int)
    bom_df["Child Index"] = bom_df["Child Index"].fillna(0).astype(int)

    # Drop the temporary filter column
    bom_df.drop(columns=["Prod_PO"], inplace=True)

    # Reorder columns to: Production BOM No_, Component No_, Total, Parent Index, Child Index
    final_bom = bom_df[["Production BOM No_", "Component No_", "Total", "Parent Index", "Child Index"]].copy()

    return final_bom

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    configured_bom = load_configured_bom_table()
    print("Configured BOM Table with Parent and Child indices renamed:")
    print(configured_bom.head())
    print("\nColumn Names:", configured_bom.columns.tolist())
    print("\nTotal rows:", len(configured_bom))