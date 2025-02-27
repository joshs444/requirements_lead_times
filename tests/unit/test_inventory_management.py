import pandas as pd
from inventory_management import process_transactions  # Adjust import as needed

def test_process_transactions():
    """
    Test the MRP logic in process_transactions with a multi-level BOM, sales orders,
    purchase orders, and inventory updates.
    """
    # Define input DataFrames
    inventory_df = pd.DataFrame({
        'Index': [1, 2, 3, 4],
        'Total Quantity': [5.0, 10.0, 20.0, 30.0]
    })

    sales_orders_df = pd.DataFrame({
        'Index': [1, 1],
        'QTY': [10.0, 15.0],
        'Date': ['2023-01-10', '2023-01-20'],
        'Transaction Type': ['Production Items', 'Production Items']
    })

    purchases_df = pd.DataFrame({
        'Index': [3, 4],
        'QTY': [10.0, 20.0],
        'Expected Receipt Date': ['2023-01-05', '2023-01-08']
    })

    item_table_df = pd.DataFrame({
        'Index': [1, 2, 3, 4],
        'No_': ['A', 'B', 'C', 'D'],
        'Lead Time (Days)': [5, 3, 2, 1]
    })

    fully_blow_out_df = pd.DataFrame({
        'Parent Index': [1, 1, 2],
        'Child Index': [2, 3, 4],
        'QTY Per': [1, 1, 2]
    })

    # Prepare input data dictionary
    data = {
        "sales_orders": sales_orders_df,
        "inventory": inventory_df,
        "final_item_data": item_table_df,
        "purchases": purchases_df
    }

    # Call the function
    final_df, updated_inventory_df = process_transactions(fully_blow_out_df, data)

    # Convert Date to datetime for consistency
    final_df['Date'] = pd.to_datetime(final_df['Date'])

    # Filter MRP rows (purchases are integrated into MRP rows)
    mrp_rows = final_df[final_df['Transaction Type'] == 'MRP']

    ### Assertions for MRP Logic and Sales Orders
    # Item A on 2023-01-10 (Sales order demand)
    a_0110 = mrp_rows[(mrp_rows['Item'] == 'A') &
                      (mrp_rows['Date'] == pd.to_datetime('2023-01-10'))]
    assert len(a_0110) == 1, "Expected one MRP row for A on 2023-01-10"
    assert a_0110['Gross Requirements'].iloc[0] == 10, "Gross Requirements for A on 2023-01-10 should be 10"
    assert a_0110['Net Requirements'].iloc[0] == 5, "Net Requirements for A on 2023-01-10 should be 5 (10 - 5 initial inventory)"
    assert a_0110['Planned Order Receipts'].iloc[0] == 5, "Planned Order Receipts for A on 2023-01-10 should be 5"
    assert a_0110['Planned Order Releases'].iloc[0] == 0, "Planned Order Releases for A on 2023-01-10 should be 0"

    # Item A on 2023-01-05 (Planned order release due to 5-day lead time)
    a_0105 = mrp_rows[(mrp_rows['Item'] == 'A') &
                      (mrp_rows['Date'] == pd.to_datetime('2023-01-05'))]
    assert len(a_0105) == 1, "Expected one MRP row for A on 2023-01-05"
    assert a_0105['Planned Order Releases'].iloc[0] == 5, "Planned Order Releases for A on 2023-01-05 should be 5"

    # Item A on 2023-01-20 (Second sales order)
    a_0120 = mrp_rows[(mrp_rows['Item'] == 'A') &
                      (mrp_rows['Date'] == pd.to_datetime('2023-01-20'))]
    assert len(a_0120) == 1, "Expected one MRP row for A on 2023-01-20"
    assert a_0120['Gross Requirements'].iloc[0] == 15, "Gross Requirements for A on 2023-01-20 should be 15"
    assert a_0120['Net Requirements'].iloc[0] == 15, "Net Requirements for A on 2023-01-20 should be 15"
    assert a_0120['Planned Order Receipts'].iloc[0] == 15, "Planned Order Receipts for A on 2023-01-20 should be 15"

    # Item A on 2023-01-15 (Release for 2023-01-20 demand)
    a_0115 = mrp_rows[(mrp_rows['Item'] == 'A') &
                      (mrp_rows['Date'] == pd.to_datetime('2023-01-15'))]
    assert len(a_0115) == 1, "Expected one MRP row for A on 2023-01-15"
    assert a_0115['Planned Order Releases'].iloc[0] == 15, "Planned Order Releases for A on 2023-01-15 should be 15"

    ### Assertions for Multi-Level BOM Handling
    # Item B on 2023-01-05 (Driven by A's release, QTY Per = 1)
    b_0105 = mrp_rows[(mrp_rows['Item'] == 'B') &
                      (mrp_rows['Date'] == pd.to_datetime('2023-01-05'))]
    assert len(b_0105) == 1, "Expected one MRP row for B on 2023-01-05"
    assert b_0105['Gross Requirements'].iloc[0] == 5, "Gross Requirements for B on 2023-01-05 should be 5"

    # Item B on 2023-01-15 (Driven by A's second release)
    b_0115 = mrp_rows[(mrp_rows['Item'] == 'B') &
                      (mrp_rows['Date'] == pd.to_datetime('2023-01-15'))]
    assert len(b_0115) == 1, "Expected one MRP row for B on 2023-01-15"
    assert b_0115['Gross Requirements'].iloc[0] == 15, "Gross Requirements for B on 2023-01-15 should be 15"
    assert b_0115['Net Requirements'].iloc[0] == 10, "Net Requirements for B on 2023-01-15 should be 10 (15 - 5 remaining inventory)"
    assert b_0115['Planned Order Receipts'].iloc[0] == 10, "Planned Order Receipts for B on 2023-01-15 should be 10"

    # Item B on 2023-01-12 (Release due to 3-day lead time)
    b_0112 = mrp_rows[(mrp_rows['Item'] == 'B') &
                      (mrp_rows['Date'] == pd.to_datetime('2023-01-12'))]
    assert len(b_0112) == 1, "Expected one MRP row for B on 2023-01-12"
    assert b_0112['Planned Order Releases'].iloc[0] == 10, "Planned Order Releases for B on 2023-01-12 should be 10"

    # Item C on 2023-01-05 (Driven by A, with purchase receipt)
    c_0105 = mrp_rows[(mrp_rows['Item'] == 'C') &
                      (mrp_rows['Date'] == pd.to_datetime('2023-01-05'))]
    assert len(c_0105) == 1, "Expected one MRP row for C on 2023-01-05"
    assert c_0105['Gross Requirements'].iloc[0] == 5, "Gross Requirements for C on 2023-01-05 should be 5"
    assert c_0105['Scheduled Receipts'].iloc[0] == 10, "Scheduled Receipts for C on 2023-01-05 should be 10"
    assert c_0105['Net Requirements'].iloc[0] == 0, "Net Requirements for C on 2023-01-05 should be 0 (20 initial + 10 receipt - 5 demand)"

    # Item C on 2023-01-15 (Driven by A's second release)
    c_0115 = mrp_rows[(mrp_rows['Item'] == 'C') &
                      (mrp_rows['Date'] == pd.to_datetime('2023-01-15'))]
    assert len(c_0115) == 1, "Expected one MRP row for C on 2023-01-15"
    assert c_0115['Gross Requirements'].iloc[0] == 15, "Gross Requirements for C on 2023-01-15 should be 15"

    # Item D on 2023-01-12 (Driven by B, QTY Per = 2)
    d_0112 = mrp_rows[(mrp_rows['Item'] == 'D') &
                      (mrp_rows['Date'] == pd.to_datetime('2023-01-12'))]
    assert len(d_0112) == 1, "Expected one MRP row for D on 2023-01-12"
    assert d_0112['Gross Requirements'].iloc[0] == 20, "Gross Requirements for D on 2023-01-12 should be 20 (10 * 2)"

    # Item D on 2023-01-08 (Purchase receipt)
    d_0108 = mrp_rows[(mrp_rows['Item'] == 'D') &
                      (mrp_rows['Date'] == pd.to_datetime('2023-01-08'))]
    assert len(d_0108) == 1, "Expected one MRP row for D on 2023-01-08"
    assert d_0108['Scheduled Receipts'].iloc[0] == 20, "Scheduled Receipts for D on 2023-01-08 should be 20"

    ### Assertions for Inventory Levels
    assert updated_inventory_df[updated_inventory_df['No_'] == 'A']['Ending Inventory'].iloc[0] == 0, \
           "Ending inventory for A should be 0 (all demand met)"
    assert updated_inventory_df[updated_inventory_df['No_'] == 'B']['Ending Inventory'].iloc[0] == 0, \
           "Ending inventory for B should be 0 (all demand met)"
    assert updated_inventory_df[updated_inventory_df['No_'] == 'C']['Ending Inventory'].iloc[0] == 10, \
           "Ending inventory for C should be 10 (20 initial + 10 receipt - 5 used on 01-05 - 15 used on 01-15)"
    assert updated_inventory_df[updated_inventory_df['No_'] == 'D']['Ending Inventory'].iloc[0] == 30, \
           "Ending inventory for D should be 30 (30 initial + 20 receipt - 20 used)"

    print("All MRP tests passed!")

# Run the test
if __name__ == "__main__":
    test_process_transactions()