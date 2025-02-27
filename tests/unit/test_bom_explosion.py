import pandas as pd
from bom_explosion import check_for_circular_reference, build_indented_bom, create_bom_hierarchy

def test_check_for_circular_reference():
    """Test circular reference detection in BOM traversal."""
    path = [1, 2, 3]
    # Component not in path should return False
    assert check_for_circular_reference(path, 4) is False, "Should return False for non-existent component"
    # Component in path should return True
    assert check_for_circular_reference(path, 2) is True, "Should detect circular reference"

def test_build_indented_bom_simple():
    """Test BOM hierarchy construction for a single-level BOM."""
    bom_data = pd.DataFrame({
        'Parent Index': [1],
        'Child Index': [2],
        'Total': [2]
    })
    hierarchy, circular_refs = build_indented_bom(bom_data, 1, 1, level=0, parent_qty=1)
    # Should have one child component
    assert len(hierarchy) == 1, "Expected one component in hierarchy"
    # Verify key fields
    assert hierarchy[0]['Production Index'] == 1, "Production Index should match top-level item"
    assert hierarchy[0]['Parent Index'] == 1, "Parent Index should match input"
    assert hierarchy[0]['Child Index'] == 2, "Child Index should match BOM data"
    assert hierarchy[0]['Level'] == 0, "Level should be 0 for first child"
    assert hierarchy[0]['QTY Per'] == 2, "QTY Per should match Total from BOM"
    assert hierarchy[0]['Total Quantity'] == 2, "Total Quantity should be 2 (1 * 2)"
    assert len(circular_refs) == 0, "No circular references expected"

def test_build_indented_bom_multilevel():
    """Test BOM hierarchy construction for a multi-level BOM."""
    bom_data = pd.DataFrame({
        'Parent Index': [1, 2],
        'Child Index': [2, 3],
        'Total': [2, 3]
    })
    hierarchy, circular_refs = build_indented_bom(bom_data, 1, 1, level=0, parent_qty=1)
    # Should have two levels (A -> B -> C)
    assert len(hierarchy) == 2, "Expected two components in hierarchy"
    # Level 0: B under A
    assert hierarchy[0]['Level'] == 0, "First component should be at Level 0"
    assert hierarchy[0]['Child Index'] == 2, "First child should be B"
    assert hierarchy[0]['Total Quantity'] == 2, "B should have 2 units (1 * 2)"
    # Level 1: C under B
    assert hierarchy[1]['Level'] == 1, "Second component should be at Level 1"
    assert hierarchy[1]['Child Index'] == 3, "Second child should be C"
    assert hierarchy[1]['Total Quantity'] == 6, "C should have 6 units (2 * 3)"
    assert len(circular_refs) == 0, "No circular references expected"

def test_build_indented_bom_circular():
    """Test BOM hierarchy with a circular reference."""
    bom_data = pd.DataFrame({
        'Parent Index': [1, 2],
        'Child Index': [2, 1],
        'Total': [1, 1]
    })
    hierarchy, circular_refs = build_indented_bom(bom_data, 1, 1, level=0, parent_qty=1)
    # Should only include non-circular component (A -> B)
    assert len(hierarchy) == 1, "Should skip circular reference"
    assert hierarchy[0]['Child Index'] == 2, "Only B should be included"
    assert len(circular_refs) >= 1, "Should detect at least one circular reference"
    assert (1, 2) in circular_refs or (2, 1) in circular_refs, "Circular reference between 1 and 2 should be logged"

def test_create_bom_hierarchy():
    """Test full BOM hierarchy creation with Order column."""
    bom_data = pd.DataFrame({
        'Parent Index': [1],
        'Child Index': [2],
        'Total': [2]
    })
    top_level_indices = [1]
    hierarchy_df, circular_refs = create_bom_hierarchy(bom_data, top_level_indices)
    # Should have one row
    assert len(hierarchy_df) == 1, "Expected one row in hierarchy"
    # Check columns and order
    assert 'Order' in hierarchy_df.columns, "Order column should be present"
    assert hierarchy_df['Order'].iloc[0] == 1, "Order should start at 1"
    assert hierarchy_df['Production Index'].iloc[0] == 1, "Production Index should match"
    assert hierarchy_df['Child Index'].iloc[0] == 2, "Child Index should match BOM"
    assert hierarchy_df['Total Quantity'].iloc[0] == 2, "Total Quantity should be 2"
    assert len(circular_refs) == 0, "No circular references expected"