import pandas as pd
from sqlalchemy import create_engine

# Database Connection
engine = create_engine('postgresql+psycopg2://postgres:suga7@localhost:5432/stock_db')

# Load the CSV you uploaded
df_sector = pd.read_csv('Sector_data - Sheet1.csv')

# Clean the 'Symbol' column (e.g., 'SBI: SBIN' -> 'SBIN')
df_sector['symbol'] = df_sector['Symbol'].str.split(': ').str[-1]

# Save to PostgreSQL
df_sector[['symbol', 'sector']].to_sql('stock_sectors', engine, if_exists='replace', index=False)
print("✅ Sector data uploaded to PostgreSQL!")