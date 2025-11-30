import pandas as pd

# Read first 5 rows
df = pd.read_csv('lmp_scanner/lmp_data_merged.csv', nrows=5)

print("Column names:")
print(df.columns.tolist())
print("\nFirst 5 rows:")
print(df.head(5))
print("\nFirst 5 IDs:")
print(df[['pnode_id_da', 'pnode_id_rt']])
print("\nFirst 5 Time Stamps:")
print(df[['datetime_da', 'datetime_rt', 'datetime']])
print("\nData types:")
print(df.dtypes)
print("\nFile info:")
print(f"Columns: {len(df.columns)}")