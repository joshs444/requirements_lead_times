import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta

def prepare_inventory(inventory_df):
    """Prepare the inventory DataFrame with initial setup."""
    inventory_df['Initial Inventory'] = inventory_df['Total Quantity'].copy()
    inventory_df['Used'] = 0.0
    inventory_df['Available'] = inventory_df['Total Quantity'].astype('float64')
    inventory_df.set_index('Index', inplace=True)
    return inventory_df

def calculate_mrp_plan(item_index, gross_req_series, scheduled_receipts_series, initial_inventory, lead_time, date_range):
    """Calculate the MRP plan for an item over a date range."""
    mrp_df = pd.DataFrame(index=date_range, columns=[
        'Gross Requirements', 'Scheduled Receipts', 'Projected Inventory',
        'Net Requirements', 'Planned Order Receipts', 'Planned Order Releases'
    ], dtype='float64').fillna(0)

    mrp_df['Gross Requirements'] = gross_req_series.reindex(date_range, fill_value=0).astype('float64')
    mrp_df['Scheduled Receipts'] = scheduled_receipts_series.reindex(date_range, fill_value=0).astype('float64')

    projected_inventory = initial_inventory
    for i in range(len(mrp_df)):
        date = mrp_df.index[i]
        available = projected_inventory + mrp_df.at[date, 'Scheduled Receipts']
        gross_req = mrp_df.at[date, 'Gross Requirements']

        if available < gross_req:
            net_req = gross_req - available
            mrp_df.at[date, 'Net Requirements'] = net_req
            mrp_df.at[date, 'Planned Order Receipts'] = net_req
            release_date = date - timedelta(days=int(lead_time))
            if release_date >= date_range[0]:
                if release_date in mrp_df.index:
                    release_date_label = release_date
                else:
                    release_idx = mrp_df.index.get_indexer([release_date], method='nearest')[0]
                    release_date_label = mrp_df.index[release_idx]
                print(f"Setting {net_req} on release_date {release_date_label} for receipt on {date}")
                mrp_df.at[release_date_label, 'Planned Order Releases'] += net_req
            else:
                print(f"Setting {net_req} on earliest date {mrp_df.index[0]} for receipt on {date}")
                mrp_df.at[mrp_df.index[0], 'Planned Order Releases'] += net_req
            projected_inventory = 0
        else:
            projected_inventory = available - gross_req

        mrp_df.at[date, 'Projected Inventory'] = projected_inventory

    return mrp_df

def prepare_sales_orders(sales_orders_df, inventory_df, sales_order_key='Index'):
    """Prepare sales orders by sorting by date and adjusting inventory usage."""
    sales_orders_df = sales_orders_df.rename(columns={'QTY': 'Open Sales QTY'})
    sales_orders_df = sales_orders_df.sort_values(by='Date')
    sales_orders_df[['Production QTY', 'Inventory Used', 'Starting Inventory']] = sales_orders_df.apply(
        lambda row: pd.Series({
            'Production QTY': row['Open Sales QTY'],
            'Inventory Used': 0.0,
            'Starting Inventory': inventory_df.at[row[sales_order_key], 'Available']
                if row[sales_order_key] in inventory_df.index else 0.0  # Updated from np.nan to 0.0
        }), axis=1)
    return sales_orders_df

def compute_levels(bom_df, root_items, valid_items):
    """Compute BOM levels for items starting from root items, only including valid items."""
    levels = {item: 0 for item in root_items if item in valid_items}
    changed = True
    while changed:
        changed = False
        for _, row in bom_df.iterrows():
            parent = row['Parent Index']
            child = row['Child Index']
            if pd.notnull(parent) and parent in levels and child in valid_items:
                new_level = levels[parent] + 1
                if child not in levels or new_level > levels[child]:
                    levels[child] = new_level
                    changed = True
    return levels

def clean_lead_time(x):
    """Clean and convert lead time to an integer, defaulting to 5 if invalid."""
    if pd.isnull(x):
        return 5
    if isinstance(x, str):
        parts = x.split("\x04")
        if parts and parts[0].strip():
            try:
                return int(parts[0].strip())
            except ValueError:
                s = re.sub(r'\D', '', x)
                return int(s) if s else 5
        else:
            s = re.sub(r'\D', '', x)
            return int(s) if s else 5
    try:
        return int(x)
    except Exception:
        return 5

def process_transactions(fully_blow_out_df, data):
    """Process all transactions (sales, purchases, MRP) and generate final DataFrames."""
    sales_orders_df = data["sales_orders"]
    inventory_df = data["inventory"][['Index', 'Total Quantity']].copy()
    item_table_df = data["final_item_data"]
    purchases_df = data["purchases"]

    # Define valid items from item_table_df
    valid_items = set(item_table_df['Index'])

    # Clean lead times
    item_table_df['Lead Time (Days)'] = item_table_df['Lead Time (Days)'].apply(clean_lead_time)

    # Determine sales order key and filter root items to valid items only
    sales_order_key = 'Index' if 'Index' in sales_orders_df.columns else 'Item Index'
    root_items = [item for item in sales_orders_df[sales_order_key].unique() if item in valid_items]
    levels = compute_levels(fully_blow_out_df, root_items, valid_items)
    for item in root_items:
        if item not in levels:
            levels[item] = 0
    max_level = max(levels.values()) if levels else 0

    # Define planning horizon with proper date formatting
    min_sales_date = pd.to_datetime(sales_orders_df['Date']).min().date()
    max_sales_date = pd.to_datetime(sales_orders_df['Date']).max().date()
    min_receipt_date = pd.to_datetime(purchases_df['Expected Receipt Date']).min().date() if not purchases_df.empty else min_sales_date
    max_receipt_date = pd.to_datetime(purchases_df['Expected Receipt Date']).max().date() if not purchases_df.empty else max_sales_date
    max_lead_time = item_table_df['Lead Time (Days)'].max()
    if max_lead_time > 1000:
        max_lead_time = 5
    planning_horizon_start = min(min_sales_date, min_receipt_date) - timedelta(days=30)
    planning_horizon_end = max(max_sales_date, max_receipt_date) + timedelta(days=int(max_lead_time) + 30)
    date_range = pd.date_range(start=planning_horizon_start, end=planning_horizon_end, freq='D')

    # Debugging output
    print(f"Planning horizon: {planning_horizon_start} to {planning_horizon_end}")
    print(f"Sales dates range: {min_sales_date} to {max_sales_date}")
    print(f"Receipt dates range: {min_receipt_date} to {max_receipt_date}")

    # Prepare inventory and sales orders
    inventory_df = prepare_inventory(inventory_df)
    initial_inventory = inventory_df['Available'].to_dict()
    sales_orders_df = prepare_sales_orders(sales_orders_df, inventory_df, sales_order_key)

    # Aggregate gross requirements, skipping invalid items
    gross_requirements = {}
    for _, row in sales_orders_df.iterrows():
        item_index = row[sales_order_key]
        if item_index not in valid_items:
            print(f"Warning: Sales order item {item_index} not in valid_items. Skipping.")
            continue
        qty = row['Open Sales QTY']
        req_date = pd.to_datetime(row['Date']).normalize()
        if req_date in date_range:
            if item_index not in gross_requirements:
                gross_requirements[item_index] = pd.Series(0.0, index=date_range, dtype='float64')
            gross_requirements[item_index].loc[req_date] += qty
        else:
            print(f"Warning: Sales order date {req_date} for item {item_index} outside planning horizon, skipped.")

    # Aggregate scheduled receipts, skipping invalid items
    scheduled_receipts = {}
    for _, row in purchases_df.iterrows():
        item_index = row['Index']
        if item_index not in valid_items:
            print(f"Warning: Purchase item {item_index} not in valid_items. Skipping.")
            continue
        qty = row['QTY']
        receipt_date = pd.to_datetime(row['Expected Receipt Date']).normalize()
        if receipt_date in date_range:
            if item_index not in scheduled_receipts:
                scheduled_receipts[item_index] = pd.Series(0.0, index=date_range, dtype='float64')
            scheduled_receipts[item_index].loc[receipt_date] += qty
        else:
            print(f"Warning: Receipt date {receipt_date} for item {item_index} outside planning horizon, skipped.")

    # Calculate MRP plans by level, processing only valid items
    mrp_plans = {}
    for level in range(max_level + 1):
        items_at_level = [item for item, lvl in levels.items() if lvl == level]
        for item in items_at_level:
            if item not in valid_items:
                print(f"Warning: Item {item} not in valid_items. Skipping.")
                continue
            print(f"Processing item: {item}")
            lead_time = item_table_df[item_table_df['Index'] == item]['Lead Time (Days)'].iloc[0]
            init_inv = initial_inventory.get(item, 0.0)
            gross_req_series = gross_requirements.get(item, pd.Series(0.0, index=date_range, dtype='float64'))
            sched_receipts_series = scheduled_receipts.get(item, pd.Series(0.0, index=date_range, dtype='float64'))
            mrp_plan = calculate_mrp_plan(item, gross_req_series, sched_receipts_series, init_inv, lead_time, date_range)
            mrp_plans[item] = mrp_plan
            # Explode requirements to child items, skipping invalid children
            for _, row in fully_blow_out_df.iterrows():
                if row['Parent Index'] == item:
                    child = row['Child Index']
                    if child not in valid_items:
                        print(f"Warning: Child item {child} not in valid_items. Skipping.")
                        continue
                    qty_per = row['QTY Per']
                    for release_date, release_qty in mrp_plan['Planned Order Releases'][mrp_plan['Planned Order Releases'] > 0].items():
                        if release_date >= date_range[0]:
                            if child not in gross_requirements:
                                gross_requirements[child] = pd.Series(0.0, index=date_range, dtype='float64')
                            gross_requirements[child].loc[release_date] += release_qty * qty_per

    # Process MRP transactions with correct starting and ending inventory
    processed_orders = []
    for item, mrp_plan in mrp_plans.items():
        for i, date in enumerate(mrp_plan.index):
            if i == 0:
                starting_inventory = initial_inventory.get(item, 0.0)
            else:
                previous_date = mrp_plan.index[i-1]
                starting_inventory = mrp_plan.at[previous_date, 'Projected Inventory']
            processed_orders.append({
                'Transaction Type': 'MRP',
                'Item': item,
                'Date': date,
                'Gross Requirements': mrp_plan.at[date, 'Gross Requirements'],
                'Scheduled Receipts': mrp_plan.at[date, 'Scheduled Receipts'],
                'Net Requirements': mrp_plan.at[date, 'Net Requirements'],
                'Planned Order Receipts': mrp_plan.at[date, 'Planned Order Receipts'],
                'Planned Order Releases': mrp_plan.at[date, 'Planned Order Releases'],
                'Starting Inventory': starting_inventory,
                'Ending Inventory': mrp_plan.at[date, 'Projected Inventory']
            })

    # Combine all transactions into a single list
    all_transactions = processed_orders

    # Convert to DataFrame and sort by 'Date' and then 'Item'
    final_df = pd.DataFrame(all_transactions)
    final_df = final_df.sort_values(by=['Date', 'Item'])

    # Assign sequential 'Order' numbers based on sorted order
    final_df['Order'] = range(1, len(final_df) + 1)

    # Map item indices to item numbers
    item_index_to_no = dict(zip(item_table_df['Index'], item_table_df['No_']))
    final_df['Item'] = final_df['Item'].map(item_index_to_no)

    # Reorder columns
    cols = final_df.columns.tolist()
    new_order = ['Transaction Type', 'Order', 'Item', 'Date', 'Gross Requirements',
                 'Scheduled Receipts', 'Net Requirements', 'Planned Order Receipts',
                 'Planned Order Releases', 'Starting Inventory', 'Ending Inventory']
    final_df = final_df[new_order]

    # Generate updated inventory DataFrame
    updated_inventory_df = pd.DataFrame({
        'No_': list(item_index_to_no.values()),
        'Ending Inventory': [
            mrp_plans[item]['Projected Inventory'].iloc[-1] if item in mrp_plans else initial_inventory.get(item, 0.0)
            for item in item_index_to_no.keys()
        ]
    })

    # Debugging output
    print("Final DataFrame Rows for C and D:")
    print(final_df[final_df['Item'].isin(['C', 'D'])][
          ['Item', 'Date', 'Gross Requirements', 'Net Requirements', 'Scheduled Receipts',
           'Starting Inventory', 'Ending Inventory']])

    return final_df, updated_inventory_df