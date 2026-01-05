import pandas as pd
import yaml
import glob
import os
from sqlalchemy import create_engine

# --- Configuration ---
YAML_DATA_ROOT = r'C:\Users\abira\Documents\GuviProjects\2-Data-Driven Stock Analysis\YamlData'
OUTPUT_CSV_DIR = 'processed_csv'
# Replace 'your_password' with your actual PostgreSQL password
DATABASE_URI = 'postgresql+psycopg2://postgres:suga7@localhost:5432/stock_db'

def extract_and_transform_data():
    print("Starting data extraction...")
    all_stock_data = {}

    search_path = os.path.join(YAML_DATA_ROOT, '**', '*.yaml')
    yaml_files = glob.glob(search_path, recursive=True)
    
    if not yaml_files:
        print(f"❌ No YAML files found in {YAML_DATA_ROOT}. Please check the path.")
        return None

    print(f"📂 Found {len(yaml_files)} files. Processing entries...")

    for file_path in yaml_files:
        try:
            with open(file_path, 'r') as file:
                data = yaml.safe_load(file)
                
            if data and isinstance(data, list):
                for entry in data:
                    # 1. TRY TO FIND THE SYMBOL AUTOMATICALLY
                    symbol = entry.get('symbol') or entry.get('tradingsymbol') or \
                             entry.get('Symbol') or entry.get('ticker')
                    
                    # 2. If still None, look for any uppercase string (e.g., "SBIN")
                    if not symbol:
                        for val in entry.values():
                            if isinstance(val, str) and val.isupper() and 2 <= len(val) <= 15:
                                symbol = val
                                break
                    
                    # Skip if we still can't find a symbol or date
                    date = entry.get('date') or entry.get('Date')
                    if not symbol or not date:
                        continue
                        
                    daily_data = {
                        'Date': date,
                        'Open': entry.get('open') or entry.get('Open'),
                        'Close': entry.get('close') or entry.get('Close'),
                        'High': entry.get('high') or entry.get('High'),
                        'Low': entry.get('low') or entry.get('Low'),
                        'Volume': entry.get('volume') or entry.get('Volume')
                    }

                    if symbol not in all_stock_data:
                        all_stock_data[symbol] = []
                    all_stock_data[symbol].append(daily_data)
        except Exception:
            continue 

    return all_stock_data

def clean_and_save(all_stock_data):
    if not all_stock_data:
        print("⚠️ No valid stock data was extracted. Check your YAML format.")
        return
        
    os.makedirs(OUTPUT_CSV_DIR, exist_ok=True)
    
    try:
        engine = create_engine(DATABASE_URI)
        print("✅ Connected to PostgreSQL.")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    print(f"📊 Creating files for {len(all_stock_data)} unique stocks...")

    for symbol, entries in all_stock_data.items():
        df = pd.DataFrame(entries)
        
        # Data Cleaning
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date').drop_duplicates()
        
        # Convert columns to numbers
        for col in ['Open', 'Close', 'High', 'Low', 'Volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.ffill().dropna()

        if not df.empty:
            # Save CSV
            csv_path = os.path.join(OUTPUT_CSV_DIR, f'{symbol}.csv')
            df.to_csv(csv_path, index=False)
            
            # Save to SQL
            table_name = f"{str(symbol).lower().replace('-', '_')}_daily"
            df.to_sql(table_name, engine, if_exists='replace', index=False)

    print("\n--- ✅ Pipeline Complete! ---")
    print(f"Check the '{OUTPUT_CSV_DIR}' folder. You should see files like SBIN.csv, RELIANCE.csv, etc.")

if __name__ == '__main__':
    raw_data = extract_and_transform_data()
    clean_and_save(raw_data)

