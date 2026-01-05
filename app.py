import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text

# Setup
engine = create_engine('postgresql+psycopg2://postgres:suga7@localhost:5432/stock_db')
st.set_page_config(page_title="Stock Analysis Dashboard", layout="wide")

# --- DATA FETCHING FUNCTIONS ---
@st.cache_data
def get_all_data():
    with engine.connect() as conn:
        # Load the summary we created earlier
        summary = pd.read_sql(text("SELECT * FROM stock_performance_summary"), conn)
        # Load sector mapping
        sectors = pd.read_sql(text("SELECT * FROM stock_sectors"), conn)
        return summary.merge(sectors, on='symbol', how='left')

# 1. Market Overview [cite: 9, 22]
st.title("📈 Nifty 50 Data-Driven Analysis")
data = get_all_data()

col1, col2, col3 = st.columns(3)
col1.metric("Average Market Return", f"{round(data['yearly_return'].mean()*100, 2)}%")
col2.metric("Green Stocks", len(data[data['status'] == 'Green']))
col3.metric("Red Stocks", len(data[data['status'] == 'Red']))

# 2. Top 10 Green and Loss Stocks [cite: 20, 21]
st.header("Top 10 Performance Ranking")
tab1, tab2 = st.tabs(["Top 10 Green", "Top 10 Loss"])
with tab1:
    st.bar_chart(data.sort_values('yearly_return', ascending=False).head(10), x='symbol', y='yearly_return')
with tab2:
    st.bar_chart(data.sort_values('yearly_return', ascending=True).head(10), x='symbol', y='yearly_return')

# 3. Volatility Analysis [cite: 27, 35]
st.header("Risk Assessment: Volatility")
vol_data = data.sort_values('volatility', ascending=False).head(10)
st.bar_chart(vol_data, x='symbol', y='volatility')

# 4. Sector-wise Performance [cite: 55, 59]
st.header("Sector-wise Performance")
sector_perf = data.groupby('sector')['yearly_return'].mean().reset_index()
st.bar_chart(sector_perf, x='sector', y='yearly_return')

# 5. Stock Price Correlation (Heatmap) [cite: 61, 67, 69]
st.header("Stock Price Correlation Heatmap")
# Logic: Fetch all tables, join them by date, and run .corr()
@st.cache_data
def get_correlation():
    with engine.connect() as conn:
        # Note: For speed in a demo, we select a few major stocks
        tables = ['sbin_daily', 'tcs_daily', 'reliance_daily', 'infy_daily', 'itc_daily']
        combined = pd.DataFrame()
        for t in tables:
            df = pd.read_sql(text(f'SELECT "Date", "Close" FROM {t}'), conn)
            df = df.rename(columns={'Close': t.replace('_daily', '').upper()})
            if combined.empty: combined = df
            else: combined = pd.merge(combined, df, on='Date')
    return combined.drop(columns=['Date']).corr()

corr_matrix = get_correlation()
fig, ax = plt.subplots()
sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', ax=ax)
st.pyplot(fig)

# --- REPLACED SECTION 6: 12-Month Performance Analysis ---
st.header("📅 12-Month Performance Breakdown")

@st.cache_data
def get_monthly_performance():
    monthly_data_list = []
    
    # 1. Get all table names from the database
    query = text("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE '%_daily'")
    with engine.connect() as conn:
        tables = pd.read_sql(query, conn)['table_name'].tolist()
        
        for table in tables:
            symbol = table.replace('_daily', '').upper()
            # Fetch date and price for the specific stock
            df = pd.read_sql(text(f'SELECT "Date", "Close" FROM "{table}"'), conn)
            df['Date'] = pd.to_datetime(df['Date'])
            df['Month'] = df['Date'].dt.month
            df = df.sort_values('Date')
            
            # Calculate Monthly Return: (Last Price of Month - First Price of Month) / First Price
            m_perf = df.groupby('Month')['Close'].agg(['first', 'last']).reset_index()
            m_perf['monthly_return'] = (m_perf['last'] - m_perf['first']) / m_perf['first']
            m_perf['symbol'] = symbol
            
            monthly_data_list.append(m_perf[['Month', 'symbol', 'monthly_return']])
            
    return pd.concat(monthly_data_list)

# Fetch the monthly processed data
monthly_perf_df = get_monthly_performance()

# Mapping month numbers to names for better display
month_names = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

# Loop through all 12 months to display the charts
for m_num in range(1, 13):
    m_name = month_names.get(m_num)
    
    # Use an expander for each month to keep the dashboard organized
    with st.expander(f"Detailed Analysis for {m_name}"):
        m_subset = monthly_perf_df[monthly_perf_df['Month'] == m_num]
        
        if not m_subset.empty:
            col_left, col_right = st.columns(2)
            
            with col_left:
                st.subheader(f"Top 5 Gainers")
                top_5 = m_subset.nlargest(5, 'monthly_return')
                st.bar_chart(top_5, x='symbol', y='monthly_return', color='#2ecc71') # Greenish
                
            with col_right:
                st.subheader(f"Top 5 Losers")
                bottom_5 = m_subset.nsmallest(5, 'monthly_return')
                st.bar_chart(bottom_5, x='symbol', y='monthly_return', color='#e74c3c') # Reddish
        else:
            st.write(f"No data available for the month of {m_name}.")

# --- NEW SECTION: Cumulative Returns for Top 5 Stocks ---
st.header("Top 5 Stocks: Cumulative Return Over Time")

@st.cache_data
def get_top_5_cumulative_returns(top_symbols):
    all_cumulative_data = pd.DataFrame()
    
    with engine.connect() as conn:
        for symbol in top_symbols:
            # Table names are stored as lowercase with _daily suffix (e.g., sbin_daily)
            table_name = f"{symbol.lower()}_daily"
            
            try:
                # Fetch Date and Close price
                query = text(f'SELECT "Date", "Close" FROM "{table_name}" ORDER BY "Date"')
                df = pd.read_sql(query, conn)
                
                # Convert Date to datetime and set as index
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.set_index('Date')
                
                # Calculate Daily Return
                df['daily_return'] = df['Close'].pct_change()
                
                # Calculate Cumulative Return: (1 + r).cumprod() - 1
                # We fillna(0) for the first day to start at 0%
                df[symbol] = (1 + df['daily_return'].fillna(0)).cumprod() - 1
                
                # Merge into the main dataframe
                if all_cumulative_data.empty:
                    all_cumulative_data = df[[symbol]]
                else:
                    all_cumulative_data = all_cumulative_data.join(df[[symbol]], how='outer')
            except Exception as e:
                st.warning(f"Could not fetch data for {symbol}: {e}")
                
    return all_cumulative_data

# 1. Identify Top 5 Symbols from the 'data' summary dataframe
# Assuming 'yearly_return' is the column name in your summary table
if not data.empty:
    top_5_symbols = data.nlargest(5, 'yearly_return')['symbol'].tolist()
    
    # 2. Fetch and calculate cumulative returns
    cumulative_df = get_top_5_cumulative_returns(top_5_symbols)
    
    # 3. Display the Line Chart
    if not cumulative_df.empty:
        # Streamlit line chart uses the index (Date) for the X-axis automatically
        st.line_chart(cumulative_df)
        st.caption("This chart shows the percentage growth of the top 5 stocks over the past year.")
    else:
        st.error("No cumulative data available to plot.")