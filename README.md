# BOM Explosion and Inventory Management System

## Overview

This Python-based system is designed to explode Bills of Materials (BOMs), manage inventory, and calculate net requirements based on sales orders and purchase data. It processes BOM data to create a detailed component hierarchy, adjusts production quantities using available inventory, computes net requirements across multiple BOM levels, and updates inventory levels with incoming purchases. The system integrates data from a SQL Server database and exports results into two Excel files:

- **Final_Net_Requirements_Based_on_Inventory.xlsx**: Contains the calculated net requirements for each component.
- **Updated_Inventory.xlsx**: Reflects the updated inventory levels after processing.

The system handles complex BOM structures, detects circular references, and supports manufacturing and procurement planning by ensuring accurate inventory and requirement calculations.

## Project Structure

The project is organized into directories and files, each with a specific role in the data processing workflow:

### **raw_data/**
Contains scripts to extract raw data from a SQL Server database (IPG-DW-PROTOTYPE on host IPGP-OX-AGP02):
- **bom_data.py**: Loads BOM relationships, including production items, component items, and quantities per unit.
- **inventory_data.py**: Loads and aggregates current inventory levels by item and location.
- **item_data.py**: Retrieves item master data, including replenishment system and source details.
- **ledger_data.py**: Fetches item ledger entries for the past 3 years (excludes Entry Type = 4).
- **purchase_data.py**: Loads purchase order data (open and historical) from 2019 onward.
- **sales_data.py**: Extracts sales order data from the past 6 months with outstanding quantities.

### **configured_tables/**
Transforms raw data into structured, usable tables:
- **bom_table.py**: Configures BOM data with Parent Index and Child Index, filtering for "Output" items.
- **inventory_table.py**: Aggregates inventory by item and merges with item indices.
- **item_table.py**: Processes item data, computes a Purchase/Output flag, and caches the result as final_item_table.
- **purchase_table.py**: Filters open purchase orders and adds item indices.
- **sales_table.py**: Enhances sales data with Item Index.

### **Core Scripts**
- **data_loader.py**: Loads all configured tables into a dictionary for downstream processing.
- **bom_explosion.py**: Constructs the indented BOM hierarchy and detects circular references.
- **inventory_management.py**: Manages inventory, processes transactions, and calculates net requirements.
- **main.py**: Orchestrates the entire workflow, from data loading to output generation.

## Data Sources

The system retrieves raw data from the SQL Server database using queries defined in the raw_data/ scripts. Below are the key data sources:

- **bom_data.py**:
  - **Source**: [IPG Photonics Corporation$Production BOM Line] joined with [IPG Photonics Corporation$Item].
  - **Columns**: Production BOM No_, Component No_, Total (sum of Quantity per).
  - **Filters**: Non-zero quantities and non-null component numbers.

- **inventory_data.py**:
  - **Source**: dbo.item_ledger_entry_all_v.
  - **Columns**: Subsidiary, Item No_, Location Code, Inventory_Cost_LC, Quantity.
  - **Filters**: Subsidiary = 'US010', excludes locations with 'MRB', non-zero quantities.

- **item_data.py**:
  - **Source**: [IPG Photonics Corporation$Item].
  - **Columns**: Index, No_, Description, Inventory Posting Group, Unit Cost, Lead Time Calculation, Global Dimension 1 Code, Replenishment System, Rev #, Item Source, Common Item No_.
  - **Transformations**: Assigns Index, maps Replenishment System (0=Purchase, 1=Output, 2=Assembly), and normalizes Item Source.

- **ledger_data.py**:
  - **Source**: [IPG Photonics Corporation$Item Ledger Entry].
  - **Columns**: Item No_, Posting Date, Entry Type, Document No_, Location Code, Quantity, Global Dimension 1 Code.
  - **Filters**: Last 3 years, excludes Entry Type = 4.

- **purchase_data.py**:
  - **Source**: [IPG Photonics Corporation$Purchase Line], [IPG Photonics Corporation$Purchase History Line], [IPG Photonics Corporation$Purchase Header], [IPG Photonics Corporation$Purchase History Header], [IPG Photonics Corporation$Purch_ Rcpt_ Line].
  - **Columns**: Status, Document Type, Document No_, Line No_, Buy-from Vendor No_, Type, No_, Cost Center, Location Code, Expected Receipt Date, Package Tracking No_, Promised Receipt Date, Description, Qty_ per Unit of Measure, Quantity, Outstanding Quantity, Unit Cost (LCY), Requested Receipt Date, Total, Planned Receipt Date, Quantity Delivered, Order Date, Receipt Posting Date, Assigned User ID, Order Confirmation Date, Purchaser Code.
  - **Filters**: Orders since 2019, non-zero quantities and costs, specific document and type codes.

- **sales_data.py**:
  - **Source**: dbo.stg_sales_header_booking_us_t.
  - **Columns**: No_, Customer, Document No_, Date, QTY (sum of Outstanding Quantity).
  - **Filters**: Type = 2, non-zero outstanding quantities, last 6 months.

## Data Flow and Structure

The system transforms raw data into actionable outputs through a clear workflow:

1. **Raw Data Loading**:
   - Scripts in raw_data/ use SQLAlchemy to fetch data into pandas DataFrames.

2. **Data Transformation (Configured Tables)**:
   - **item_table.py**:
     - Aggregates ledger data (last 2 years, Entry Type 0/6) and purchase data (open, Type = 2, Document Type = 1).
     - Merges with raw item data to compute Purchase/Output flag based on transaction volumes and item source.
     - Caches result as final_item_table with Index, No_, and Purchase/Output.
   - **bom_table.py**: Filters BOM data for "Output" items, adds Parent Index and Child Index using final_item_table.
   - **inventory_table.py**: Aggregates inventory by Item No_, merges with final_item_table for Index.
   - **purchase_table.py**: Filters open purchases, renames Outstanding Quantity to QTY, merges with final_item_table for Index.
   - **sales_table.py**: Adds Item Index to sales data from final_item_table.

3. **Data Consolidation**:
   - **data_loader.py** loads all configured tables into a dictionary:
     - bom_data, inventory, purchases, sales_orders, final_item_data.

4. **BOM Explosion**:
   - **bom_explosion.py**:
     - Recursively builds BOM hierarchy from top-level sales items.
     - Calculates total quantities and detects circular references.

5. **Inventory Management**:
   - **inventory_management.py**:
     - Prepares inventory with Used and Available columns.
     - Adjusts sales order production quantities based on inventory.
     - Processes transactions level-by-level, updating inventory and net requirements.
     - Integrates purchases to increase inventory.
     - Exports results to Excel.

6. **Main Workflow**:
   - **main.py**:
     - Loads data, builds BOM hierarchy, processes transactions, and generates outputs.

## Calculations and Logic

### **BOM Explosion**
- **Process**: Recursively traverses BOM from top-level sales items using build_indented_bom.
- **Calculation**: Multiplies QTY Per by parent quantities to compute Total Quantity.
- **Circular Reference Detection**: Checks for components in the current path, logged in circular_references.

### **Inventory Adjustment**
- **Process**: Reduces Open Sales QTY by available inventory in adjust_production_qty.
- **Logic**:
  - If 10 units are needed and 4 are available, Production QTY = 6, Inventory Used = 4.
  - Updates Used and Available in inventory.

### **Net Requirements**
- **Process**: Calculates level-by-level in process_order.
- **Logic**:
  - Initial Net Requirements = Total Quantity * Production QTY.
  - Applies Ratio Prior Level (stock ratio from parent) to propagate requirements.
  - Subtracts available inventory, setting Net Requirements = max(net - used_inventory, 0).

### **Purchase Integration**
- **Process**: Adds purchase QTY to inventory in process_purchase.
- **Logic**: Increases Available and Total Quantity, sets Net Requirements = 0.

## Installation and Execution

**Install Dependencies:**

Install the required Python packages using pip:

bash
pip install pandas numpy openpyxl sqlalchemy pyodbc