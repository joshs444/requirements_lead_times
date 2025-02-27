import pandas as pd
from expedites_and_purchases import calculate_expedites_and_purchases

def test_calculate_expedites_and_purchases():
    # Define input DataFrames
    item_table_df = pd.DataFrame({
        'No_': ['Item1', 'Item2', 'Item3'],
        'Purchase/Output': ['Purchase', 'Output', 'Purchase']
    })

    final_df = pd.DataFrame({
        'Date': ['2023-01-01', '2023-01-05', '2023-01-08', '2023-01-03', '2023-01-02', '2023-01-04'],
        'Transaction Type': ['MRP', 'MRP', 'Purchase', 'MRP', 'Purchase', 'MRP'],
        'Item': ['Item1', 'Item1', 'Item1', 'Item2', 'Item3', 'Item3'],
        'Net Requirements': [10, 15, 0, 5, 0, 8],
        'Scheduled Receipts': [0, 0, 20, 0, 10, 0],
        'Planned Order Releases': [10, 15, 0, 5, 0, 8]
    })
    final_df['Date'] = pd.to_datetime(final_df['Date'])

    # Set current_date
    current_date = pd.to_datetime('2023-01-03')

    # Call the function
    expedites_df, purchases_df = calculate_expedites_and_purchases(final_df, item_table_df, current_date=current_date)

    # Define expected DataFrames
    expected_expedites = pd.DataFrame({
        'Item': ['Item1'],
        'Required Date': pd.to_datetime(['2023-01-01']),
        'Expedite Quantity': [10]
    })

    expected_purchases = pd.DataFrame({
        'Item': ['Item1', 'Item3'],
        'Purchase Quantity': [15, 8],
        'Placement Date': pd.to_datetime(['2023-01-05', '2023-01-04']),
        'Expected Receipt Date': pd.to_datetime(['2023-01-05', '2023-01-04'])
    })

    # Assertions
    pd.testing.assert_frame_equal(expedites_df.reset_index(drop=True), expected_expedites.reset_index(drop=True), check_dtype=False)
    pd.testing.assert_frame_equal(purchases_df.reset_index(drop=True), expected_purchases.reset_index(drop=True), check_dtype=False)
    print("All tests passed!")

if __name__ == "__main__":
    test_calculate_expedites_and_purchases()