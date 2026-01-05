# project-2
Data-Driven-Stock-Analysis.

##  Project Overview
This project analyzes Nifty 50 stocks over the past year using a full data pipeline. It extracts raw YAML data, processes it via Python, stores it in a SQL database, and visualizes insights through Streamlit and Power BI.

##  Basic Workflow & Execution
1. **Extraction:** Run `data.py` to transform raw YAML into 50 symbol-wise CSVs.
2. **Database:** Run `uploading.py` to move cleaned data and sector mappings into PostgreSQL.
3. **Analysis:** Run `analysis.py` to generate yearly returns and volatility metrics.
4. **Visualization:** - Launch the Streamlit app: `streamlit run app.py`
   - Open the Power BI dashboard: `Power bi visuvalisation.pbix`

##  Key Insights
- **Top Performers:** Identified Top 10 Green stocks based on yearly returns.
- **Risk Assessment:** Visualized volatility using standard deviation of daily returns.
- **Sector Analysis:** Compared average performance across different industries.
