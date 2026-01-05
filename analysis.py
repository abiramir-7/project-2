import pandas as pd
from sqlalchemy import create_engine, text  # 1. Added 'text' here

engine = create_engine('postgresql+psycopg2://postgres:suga7@localhost:5432/stock_db')

def calculate_master_metrics():
    # 2. Wrap the query in text()
    query = text("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE '%_daily'")
    
    # 3. Use engine.connect() to execute the query
    with engine.connect() as conn:
        tables = pd.read_sql(query, conn)
    
    summary_list = []

    for table in tables['table_name']:
        # Wrap this query too
        data_query = text(f'SELECT * FROM "{table}" ORDER BY "Date"')
        with engine.connect() as conn:
            df = pd.read_sql(data_query, conn)
            
        if len(df) < 2: 
            continue

        symbol = table.replace('_daily', '').upper()
        
        # Calculate Yearly Return [cite: 20]
        start_price = df.iloc[0]['Close']
        end_price = df.iloc[-1]['Close']
        yearly_return = (end_price - start_price) / start_price
        
        # Calculate Volatility (Standard Deviation of Daily Returns) [cite: 27, 34]
        df['daily_return'] = df['Close'].pct_change()
        volatility = df['daily_return'].std()

        summary_list.append({
            'symbol': symbol,
            'yearly_return': yearly_return,
            'volatility': volatility,
            'avg_volume': df['Volume'].mean(), # [cite: 25]
            'status': 'Green' if yearly_return > 0 else 'Red' # 
        })

    summary_df = pd.DataFrame(summary_list)
    # to_sql usually works fine without text()
    summary_df.to_sql('stock_performance_summary', engine, if_exists='replace', index=False)
    print("✅ Performance Summary Table Created!")

if __name__ == '__main__':
    calculate_master_metrics()