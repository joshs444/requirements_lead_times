import os
import pandas as pd
import numpy as np
from datetime import datetime
from data_loader import load_all_data
from bom_explosion import create_bom_hierarchy
from inventory_management import process_transactions
from expedites_and_purchases import calculate_expedites_and_purchases

def main():
    print("MRP Process Started.\n")

    # Load all data using the data loader
    data = load_all_data()
    bom_data = data.get("bom_data")
    sales_orders_df = data.get("sales_orders")
    lead_times_df = data.get("lead_times", None)  # Optional lead times data

    if bom_data is None or sales_orders_df is None:
        print("Error loading BOM or Sales Orders. Exiting.")
        return

    # Use top-level indices from the sales orders using the "Index" column
    top_level_indices = sales_orders_df['Index'].tolist()

    # Build the BOM hierarchy using the loaded BOM data and computed top-level indices
    bom_hierarchy_df, circular_refs = create_bom_hierarchy(bom_data, top_level_indices)

    # Display BOM hierarchy details
    print("BOM Hierarchy Column Names:")
    print(bom_hierarchy_df.columns.tolist())
    print("\nFirst 5 Rows of BOM Hierarchy:")
    print(bom_hierarchy_df.head(5).to_string(index=False))

    if circular_refs:
        print("\nCircular References Detected:")
        print(circular_refs)
    else:
        print("\nNo circular references detected.")

    # Process transactions using the BOM hierarchy and the full data dictionary
    final_df, updated_inventory_df = process_transactions(bom_hierarchy_df, data)
    print("\nTransactions processed successfully.")

    # Calculate expedites and purchases, including lead times if available
    expedites_df, purchases_df = calculate_expedites_and_purchases(final_df, data.get("final_item_data"), lead_times_df)

    # Define output folder (relative to project directory)
    project_dir = os.path.dirname(os.path.abspath(__file__))
    output_folder = os.path.join(project_dir, "Outputs")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Get current date in mm-dd-yy format
    date_str = datetime.now().strftime("%m-%d-%y")

    # Export Requirements and Inventory in one Excel workbook with separate tabs
    # Requirements will be broken into chunks (Requirements1, Requirements2, etc.)
    req_inv_filename = os.path.join(output_folder, f"requirements_and_inventory_{date_str}.xlsx")
    MAX_ROWS_PER_CHUNK = 1000000  # Increased from 500,000 to 1,000,000
    num_chunks = int(np.ceil(len(final_df) / MAX_ROWS_PER_CHUNK))
    print(f"Rows in final_df: {len(final_df)}, Chunks at 1M rows: {num_chunks}")  # Added for debugging
    try:
        with pd.ExcelWriter(req_inv_filename, engine='openpyxl') as writer:
            for i in range(num_chunks):
                start_row = i * MAX_ROWS_PER_CHUNK
                end_row = min((i + 1) * MAX_ROWS_PER_CHUNK, len(final_df))
                chunk = final_df.iloc[start_row:end_row]
                sheet_name = f"Requirements{i + 1}"
                chunk.to_excel(writer, sheet_name=sheet_name, index=False)
            # Write inventory on its own tab called Inventory, without resetting index
            updated_inventory_df.to_excel(writer, sheet_name='Inventory', index=False)
        print(f"Requirements and Inventory saved in '{req_inv_filename}' with {num_chunks} Requirements tab(s) and an Inventory tab.")
    except Exception as e:
        print(f"Error saving requirements and inventory: {e}")

    # Export Purchases and Expedites in one Excel workbook with separate tabs
    pur_exp_filename = os.path.join(output_folder, f"expedites_and_purchases_{date_str}.xlsx")
    try:
        with pd.ExcelWriter(pur_exp_filename, engine='openpyxl') as writer:
            purchases_df.to_excel(writer, sheet_name='Purchases', index=False)
            expedites_df.to_excel(writer, sheet_name='Expedites', index=False)
        print(f"Expedites and Purchases saved in '{pur_exp_filename}' with a Purchases tab and an Expedites tab.")
    except Exception as e:
        print(f"Error saving expedites and purchases: {e}")

    print("Calculation and adjustment complete. Check the output files in the specified folder.")
    return final_df, updated_inventory_df

if __name__ == '__main__':
    main()