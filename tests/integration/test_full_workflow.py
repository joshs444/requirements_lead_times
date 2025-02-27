import pandas as pd
from bom_explosion import create_bom_hierarchy  # Import the BOM hierarchy function
from inventory_management import process_transactions  # Import the transaction processing function

def test_full_workflow_multilevel():
    """Test end-to-end workflow with a two-level BOM and time-phased MRP logic."""
    # Define test data
    data = {
        'bom_data': pd.DataFrame({
            'Parent Index': [1, 2],
            'Child Index': [2, 3],
            'Total': [2, 3]  # Quantity per parent (e.g., 1 A needs 2 B, 1 B needs 3 C)
        }),
        'inventory': pd.DataFrame({
            'Index': [3],
            'Total Quantity': [5.0]  # Initial inventory for item C
        }),
        'sales_orders': pd.DataFrame({
            'Index': [1],
            'QTY': [10.0],  # Sales order for 10 units of A
            'Date': ['2023-01-10'],  # Date of the sales order
            'Document No_': ['SO001']
        }),
        'purchases': pd.DataFrame(columns=['Index', 'QTY', 'Expected Receipt Date', 'Document No_']),
        'final_item_data': pd.DataFrame({
            'Index': [1, 2, 3],
            'No_': ['A', 'B', 'C'],  # Item identifiers
            'Lead Time (Days)': [3, 2, 1]  # Lead times for A, B, C
        })
    }

    # Build BOM hierarchy starting from top-level item A (Index 1)
    hierarchy_df, circular_refs = create_bom_hierarchy(data['bom_data'], [1])
    assert len(circular_refs) == 0, "No circular references should be detected"
    assert len(hierarchy_df) == 2, "Hierarchy should have two rows (B and C)"

    # Process transactions using time-phased MRP logic
    final_df, updated_inventory_df = process_transactions(hierarchy_df, data)

    # Check net requirements for item C (child of B)
    c_rows = final_df[final_df['Item'] == 'C']
    c_net = c_rows[c_rows['Date'] == pd.to_datetime('2023-01-05')]['Net Requirements'].sum()
    assert c_net == 55.0, "C should need 55 (10 A * 2 B * 3 C = 60 - 5 inventory) on '2023-01-05'"

    # Check net requirements for item B (child of A)
    b_rows = final_df[final_df['Item'] == 'B']
    b_net = b_rows[b_rows['Date'] == pd.to_datetime('2023-01-07')]['Net Requirements'].sum()
    assert b_net == 20.0, "B should need 20 (10 A * 2 B) on '2023-01-07'"

    # Check ending inventory for item C
    c_inv = updated_inventory_df[updated_inventory_df['No_'] == 'C']
    assert c_inv['Ending Inventory'].iloc[0] == 0.0, "C inventory should be 0 at end of horizon"

def test_full_workflow_three_levels():
    """Test end-to-end workflow with a three-level BOM and time-phased MRP logic."""
    # Define test data
    data = {
        'bom_data': pd.DataFrame({
            'Parent Index': [1, 2, 3],
            'Child Index': [2, 3, 4],
            'Total': [2, 3, 4]  # Quantity per parent (e.g., 1 A needs 2 B, 1 B needs 3 C, 1 C needs 4 D)
        }),
        'inventory': pd.DataFrame({
            'Index': [4],
            'Total Quantity': [10.0]  # Initial inventory for item D
        }),
        'sales_orders': pd.DataFrame({
            'Index': [1],
            'QTY': [10.0],  # Sales order for 10 units of A
            'Date': ['2023-01-10'],  # Date of the sales order
            'Document No_': ['SO001']
        }),
        'purchases': pd.DataFrame(columns=['Index', 'QTY', 'Expected Receipt Date', 'Document No_']),
        'final_item_data': pd.DataFrame({
            'Index': [1, 2, 3, 4],
            'No_': ['A', 'B', 'C', 'D'],  # Item identifiers
            'Lead Time (Days)': [3, 2, 1, 1]  # Lead times for A, B, C, D
        })
    }

    # Build BOM hierarchy starting from top-level item A (Index 1)
    hierarchy_df, circular_refs = create_bom_hierarchy(data['bom_data'], [1])
    assert len(circular_refs) == 0, "No circular references should be detected"
    assert len(hierarchy_df) == 3, "Hierarchy should have three rows (B, C, D)"

    # Process transactions using time-phased MRP logic
    final_df, updated_inventory_df = process_transactions(hierarchy_df, data)

    # Check net requirements for item D (child of C)
    d_rows = final_df[final_df['Item'] == 'D']
    d_net = d_rows[d_rows['Date'] == pd.to_datetime('2023-01-04')]['Net Requirements'].sum()
    assert d_net == 230.0, "D should need 230 (10 A * 2 B * 3 C * 4 D = 240 - 10 inventory) on '2023-01-04'"

    # Check net requirements for item C (child of B)
    c_rows = final_df[final_df['Item'] == 'C']
    c_net = c_rows[c_rows['Date'] == pd.to_datetime('2023-01-05')]['Net Requirements'].sum()
    assert c_net == 60.0, "C should need 60 (20 B * 3 C) on '2023-01-05'"

    # Check net requirements for item B (child of A)
    b_rows = final_df[final_df['Item'] == 'B']
    b_net = b_rows[b_rows['Date'] == pd.to_datetime('2023-01-07')]['Net Requirements'].sum()
    assert b_net == 20.0, "B should need 20 (10 A * 2 B) on '2023-01-07'"

    # Check ending inventory for item D
    d_inv = updated_inventory_df[updated_inventory_df['No_'] == 'D']
    assert d_inv['Ending Inventory'].iloc[0] == 0.0, "D inventory should be 0 at end of horizon"

# Optional: Run the tests if this file is executed directly
if __name__ == "__main__":
    test_full_workflow_multilevel()
    test_full_workflow_three_levels()
    print("All tests passed successfully!")