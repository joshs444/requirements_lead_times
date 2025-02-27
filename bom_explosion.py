# bom_explosion.py
import pandas as pd


def check_for_circular_reference(path, component_index):
    """
    Returns True if the component_index is already in the current path,
    indicating a circular reference.
    """
    return component_index in path


def build_indented_bom(bom, main_number, parent_index, level=0, parent_qty=1,
                       bom_hierarchy=None, path=None, circular_references=None):
    """
    Recursively builds an indented BOM hierarchy using the BOM DataFrame.

    Parameters:
        bom (DataFrame): BOM table with columns:
                         ['Production BOM No_', 'Component No_', 'Total',
                          'Parent Index', 'Child Index']
        main_number (int): The top-level production index identifier.
        parent_index (int): The current parent index being processed.
        level (int): Current depth level in the BOM hierarchy.
        parent_qty (numeric): Cumulative quantity from parent(s).
        bom_hierarchy (list): List to collect hierarchy rows.
        path (list): List tracking the current traversal path (for circular detection).
        circular_references (set): Set collecting detected circular references.

    Returns:
        tuple: (bom_hierarchy, circular_references)
    """
    if bom_hierarchy is None:
        bom_hierarchy = []
    if path is None:
        path = []
    if circular_references is None:
        circular_references = set()

    # Track the current path
    path.append(parent_index)

    # Get components where the Parent Index matches the current parent_index
    components = bom[bom['Parent Index'] == parent_index]
    for _, component in components.iterrows():
        component_index = component['Child Index']
        # Skip if a circular reference is detected
        if check_for_circular_reference(path, component_index):
            circular_references.add((parent_index, component_index))
            continue

        # Use the "Total" column as the quantity per unit (output as "QTY Per")
        qty_per = component['Total']
        component_total_qty = qty_per * parent_qty

        bom_hierarchy.append({
            'Production Index': main_number,
            'Level': level,
            'Parent Index': parent_index,
            'Child Index': component_index,
            'QTY Per': qty_per,
            'Total Quantity': component_total_qty
        })

        # Recursively process subcomponents if any exist
        if bom['Parent Index'].eq(component_index).any():
            build_indented_bom(bom, main_number, component_index, level + 1,
                               component_total_qty, bom_hierarchy, path, circular_references)
    path.pop()
    return bom_hierarchy, circular_references


def create_bom_hierarchy(bom_data, top_level_indices):
    """
    Creates a BOM hierarchy DataFrame from the BOM data and top-level indices.

    Parameters:
        bom_data (DataFrame): Configured BOM table.
        top_level_indices (list): List of top-level indices to start the explosion.

    Returns:
        tuple: (bom_hierarchy_df, circular_references_set)
               - bom_hierarchy_df: DataFrame with the BOM hierarchy and an 'Order' column.
               - circular_references_set: Set of detected circular reference tuples.
    """
    bom_hierarchy_list = []
    circular_references_set = set()
    processed_indices = set()

    for index in top_level_indices:
        if index not in processed_indices:
            hierarchy, circular_refs = build_indented_bom(bom_data, index, index)
            bom_hierarchy_list.extend(hierarchy)
            circular_references_set.update(circular_refs)
            processed_indices.add(index)

    # Convert the list into a DataFrame and add an Order column
    bom_hierarchy_df = pd.DataFrame(bom_hierarchy_list)
    bom_hierarchy_df.insert(0, 'Order', range(1, len(bom_hierarchy_df) + 1))
    bom_hierarchy_df.reset_index(drop=True, inplace=True)
    return bom_hierarchy_df, circular_references_set


def save_bom_index(bom_hierarchy_df, output_file):
    """
    Saves the BOM hierarchy DataFrame to an Excel file.

    Parameters:
        bom_hierarchy_df (DataFrame): DataFrame containing the BOM hierarchy.
        output_file (str): Path to the output Excel file.
    """
    bom_hierarchy_df.to_excel(output_file, index=False)


if __name__ == "__main__":
    # Example usage:
    # The data is expected to be loaded elsewhere (e.g., via main.py),
    # so this block can be used for standalone testing if desired.

    # For demonstration only:
    # import data_loader
    # data = data_loader.load_all_data()
    # bom_data = data.get("bom_data")
    # top_level_indices = [ ... ]  # Define as needed
    # hierarchy_df, circular_refs = create_bom_hierarchy(bom_data, top_level_indices)
    # print(hierarchy_df)
    # print(circular_refs)
    pass
