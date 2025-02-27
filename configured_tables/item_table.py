import pandas as pd
from raw_data.ledger_data import get_item_ledger_data  # Loads raw ledger data
from raw_data.purchase_data import get_purchase_data  # Loads raw purchase data
from raw_data.item_data import get_item_data  # Loads the raw item table


def load_aggregated_ledger_data():
    """
    Aggregate ledger and purchase data by item.

    - Converts Posting Date, filters for the past 2 years & Entry Types 0/6.
    - Computes absolute quantities and pivots to get 'Output' (Type 6) and 'Purchase' (Type 0).
    - Filters purchase data for OPEN, Type 2, Document Type 1, groups by item and renames quantity as 'Open'.
    - Merges the two aggregated datasets on 'Item No_' and fills missing values with 0.
    """
    # Process Ledger Data
    ledger_df = get_item_ledger_data()
    ledger_df["Posting Date"] = pd.to_datetime(ledger_df["Posting Date"])
    two_years_ago = pd.Timestamp.today() - pd.DateOffset(years=2)
    ledger_df = ledger_df[ledger_df["Posting Date"] >= two_years_ago]
    ledger_df = ledger_df[ledger_df["Entry Type"].isin([0, 6])]
    ledger_df["abs_quantity"] = ledger_df["Quantity"].abs()

    ledger_agg = ledger_df.pivot_table(
        index="Item No_",
        columns="Entry Type",
        values="abs_quantity",
        aggfunc="sum",
        fill_value=0
    ).reset_index()
    ledger_agg = ledger_agg.rename(columns={6: "Output", 0: "Purchase"})
    ledger_agg.columns.name = None

    # Process Purchase Data
    purchase_df = get_purchase_data()
    purchase_filtered = purchase_df[
        (purchase_df["Status"] == "OPEN") &
        (purchase_df["Type"] == 2) &
        (purchase_df["Document Type"] == 1)
        ]
    purchase_agg = purchase_filtered.groupby("No_", as_index=False)["Outstanding Quantity"].sum()
    purchase_agg = purchase_agg.rename(columns={"No_": "Item No_", "Outstanding Quantity": "Open"})

    # Merge Aggregated Data
    merged_df = pd.merge(ledger_agg, purchase_agg, on="Item No_", how="outer")
    for col in ["Output", "Purchase", "Open"]:
        merged_df[col] = merged_df[col].fillna(0)
    merged_df = merged_df.reset_index(drop=True)
    merged_df.index = merged_df.index + 1

    return merged_df


def load_final_item_data():
    """
    Merge aggregated ledger data with the raw item table and compute the 'Purchase/Output' flag.

    - Left-joins the item table with aggregated data.
    - Normalizes 'Item Source' and sets 'Main' (falling back to 'Replenishment System' if blank).
    - Computes 'Purchase/Output' based on the conditions:
         * If Open > 0, it's "Purchase".
         * Else, if Main is "Purchase" and Output > Purchase, it's "Output".
         * Else, if Main is "Output" and Purchase > Output, it's "Purchase".
         * Otherwise, it remains as Main.
    - Returns only the columns: Index, No_, and Purchase/Output.
    """
    aggregated_df = load_aggregated_ledger_data()
    item_df = get_item_data()

    merged = pd.merge(item_df, aggregated_df, left_on="No_", right_on="Item No_", how="left")
    for col in ["Purchase", "Output", "Open"]:
        merged[col] = merged[col].fillna(0)
    if "Item No_" in merged.columns:
        merged = merged.drop(columns=["Item No_"])

    # Normalize Item Source values
    merged["Item Source"] = merged["Item Source"].replace({
        "Made In-House": "Output",
        "Third Party Purchase": "Purchase",
        "Interco Purchase": "Purchase"
    })

    # Create 'Main' column (fallback to Replenishment System if Item Source is blank)
    merged = merged.copy()
    merged["Main"] = merged["Item Source"]
    merged.loc[merged["Main"].str.strip() == "", "Main"] = merged["Replenishment System"]

    # Define logic for 'Purchase/Output'
    def compute_purchase_output(row):
        if row["Open"] > 0:
            return "Purchase"
        if row["Main"] == "Purchase" and row["Output"] > row["Purchase"]:
            return "Output"
        if row["Main"] == "Output" and row["Purchase"] > row["Output"]:
            return "Purchase"
        return row["Main"]

    merged["Purchase/Output"] = merged.apply(compute_purchase_output, axis=1)

    final_result = merged[["Index", "No_", "Purchase/Output", "Lead Time (Days)"]].copy()
    return final_result


# Cache the final item table at module load time.
final_item_table = load_final_item_data()

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    print("Final Merged Item Data (Cached):")
    print(final_item_table.head())
    print("\nColumn Names:", final_item_table.columns.tolist())
    print("\nTotal records:", len(final_item_table))
