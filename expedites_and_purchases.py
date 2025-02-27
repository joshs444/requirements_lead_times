import pandas as pd

def calculate_expedites_and_purchases(final_df, item_table_df, lead_times_df=None, current_date=None):
    """
    Calculate expedites and additional purchases based on MRP planned order releases, supply timing, and lead times.

    Parameters:
    - final_df (DataFrame): MRP output with columns 'Item', 'Date', 'Net Requirements', 'Transaction Type',
                            'Scheduled Receipts', 'Planned Order Releases'.
    - item_table_df (DataFrame): Item master data with columns 'No_' and 'Purchase/Output'.
    - lead_times_df (DataFrame, optional): Lead times with columns 'Item', 'Lead Time (Days)'.
    - current_date (datetime, optional): Date to determine past due vs. future actions; defaults to min date in final_df.

    Returns:
    - expedites_df (DataFrame): Expedite requirements with 'Item', 'Required Date', 'Expedite Quantity'.
    - purchases_df (DataFrame): Purchase orders with 'Item', 'Purchase Quantity', 'Placement Date', 'Expected Receipt Date'.
    """
    # Check for required columns
    required_cols = ['Item', 'Date', 'Net Requirements', 'Transaction Type', 'Scheduled Receipts', 'Planned Order Releases']
    for col in required_cols:
        if col not in final_df.columns:
            raise ValueError(f"Missing required column in final_df: {col}")

    # Ensure 'Date' is datetime
    final_df = final_df.copy()
    final_df['Date'] = pd.to_datetime(final_df['Date'], errors='coerce')

    # Set current_date if not provided
    if current_date is None:
        current_date = final_df['Date'].min()
    current_date = pd.to_datetime(current_date)

    # Filter items where 'Purchase/Output' == 'Purchase'
    purchase_items = item_table_df[item_table_df['Purchase/Output'] == 'Purchase']['No_'].unique()
    mrp_df = final_df[(final_df['Transaction Type'] == 'MRP') &
                      (final_df['Item'].isin(purchase_items)) &
                      (final_df['Planned Order Releases'] > 0)].copy()

    if mrp_df.empty:
        return pd.DataFrame(columns=['Item', 'Required Date', 'Expedite Quantity']), \
               pd.DataFrame(columns=['Item', 'Purchase Quantity', 'Placement Date', 'Expected Receipt Date'])

    # Map lead times; default to 0 if not provided or missing
    if lead_times_df is not None and 'Item' in lead_times_df.columns and 'Lead Time (Days)' in lead_times_df.columns:
        lead_times = lead_times_df.set_index('Item')['Lead Time (Days)']
        mrp_df['Lead Time'] = mrp_df['Item'].map(lead_times).fillna(0)
    else:
        mrp_df['Lead Time'] = 0

    # Calculate expected receipt date
    mrp_df['Expected Receipt Date'] = mrp_df['Date'] + pd.to_timedelta(mrp_df['Lead Time'], unit='D')

    # Expedites: past due planned order releases
    expedites_df = mrp_df[mrp_df['Date'] < current_date][
        ['Item', 'Expected Receipt Date', 'Planned Order Releases']
    ].rename(columns={
        'Expected Receipt Date': 'Required Date',
        'Planned Order Releases': 'Expedite Quantity'
    })

    # Purchases: future planned order releases, aggregated by release date
    future_releases = mrp_df[mrp_df['Date'] >= current_date]
    purchases_df = future_releases.groupby(['Item', 'Date']).agg({
        'Planned Order Releases': 'sum',
        'Expected Receipt Date': 'first'  # Same release date has same receipt date
    }).reset_index().rename(columns={
        'Date': 'Placement Date',
        'Planned Order Releases': 'Purchase Quantity',
        'Expected Receipt Date': 'Expected Receipt Date'
    })[['Item', 'Purchase Quantity', 'Placement Date', 'Expected Receipt Date']]

    # Ensure output DataFrames have consistent columns even if empty
    if expedites_df.empty:
        expedites_df = pd.DataFrame(columns=['Item', 'Required Date', 'Expedite Quantity'])
    if purchases_df.empty:
        purchases_df = pd.DataFrame(columns=['Item', 'Purchase Quantity', 'Placement Date', 'Expected Receipt Date'])

    return expedites_df, purchases_df