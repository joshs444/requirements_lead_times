import streamlit as st
import pandas as pd
from data_loader import load_all_data
from bom_explosion import create_bom_hierarchy
from inventory_management import process_transactions
from expedites_and_purchases import calculate_expedites_and_purchases

st.title("MRP Process Dashboard")


@st.cache
def get_data():
    return load_all_data()


data = get_data()

if data is None:
    st.error("Error loading data!")
else:
    sales_orders_df = data.get("sales_orders")
    bom_data = data.get("bom_data")

    if sales_orders_df is None or bom_data is None:
        st.error("Required data (Sales Orders or BOM) is missing!")
    else:
        # Create a searchable multi-select widget for customers
        unique_customers = sorted(sales_orders_df['Customer'].unique())
        selected_customers = st.multiselect(
            "Search and select customer(s):",
            options=unique_customers,
            default=[]  # No default selection
        )

        st.write("Selected Customer(s):", selected_customers)

        # Run process only when a customer is selected and the run button is pressed
        if st.button("Run MRP Process for Selected Customer(s)"):
            if not selected_customers:
                st.error("Please select at least one customer!")
            else:
                with st.spinner("Processing data, please wait..."):
                    # Filter sales orders based on selected customers
                    filtered_sales_orders_df = sales_orders_df[sales_orders_df['Customer'].isin(selected_customers)]

                    if filtered_sales_orders_df.empty:
                        st.error("No sales orders found for the selected customer(s)!")
                    else:
                        # Use the "Item Index" from the filtered sales orders as top-level indices
                        top_level_indices = filtered_sales_orders_df['Item Index'].tolist()

                        # Build the BOM hierarchy
                        bom_hierarchy_df, circular_refs = create_bom_hierarchy(bom_data, top_level_indices)

                        st.write("**BOM Hierarchy Column Names:**", bom_hierarchy_df.columns.tolist())
                        st.write("**First 5 Rows of BOM Hierarchy:**")
                        st.dataframe(bom_hierarchy_df.head(5))

                        if circular_refs:
                            st.warning("Circular References Detected:")
                            st.write(circular_refs)
                        else:
                            st.success("No circular references detected.")

                        # Process transactions using the BOM hierarchy and the full data dictionary
                        final_df, updated_inventory_df = process_transactions(bom_hierarchy_df, data)
                        st.success("Transactions processed successfully.")

                        # Calculate expedites and purchases
                        expedites_df, purchases_df = calculate_expedites_and_purchases(final_df,
                                                                                       data.get("final_item_data"))

                        # Save results to Excel files
                        expedites_filename = 'Expedites.xlsx'
                        purchases_filename = 'Purchases.xlsx'
                        expedites_df.to_excel(expedites_filename, index=False)
                        purchases_df.to_excel(purchases_filename, index=False)
                        st.write(
                            f"Expedites and purchases calculated. Files saved as **{expedites_filename}** and **{purchases_filename}**.")

                        # Display final results in the app
                        st.subheader("Final Net Requirements")
                        st.dataframe(final_df)

                        st.subheader("Updated Inventory")
                        updated_inventory_df = updated_inventory_df.loc[:, ~updated_inventory_df.columns.duplicated()]
                        st.dataframe(updated_inventory_df)
