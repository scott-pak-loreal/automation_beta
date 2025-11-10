import pandas as pd
from pathlib import Path

# ---- File Paths ----
sales_file = Path("Biolage Sales Data.xlsx")

# ---- Load Sales Data ----
df_sales = pd.read_excel(sales_file, sheet_name="Raw Data")

# ---- Clean Date Column ----
df_sales['Week End'] = pd.to_datetime(df_sales['Week End'], errors='coerce').dt.date

# Drop 'Week' column if exists
if 'Week' in df_sales.columns:
    df_sales = df_sales.drop(columns=['Week'])

# Rename 'Week End' to 'Date'
df_sales = df_sales.rename(columns={'Week End': 'Date'})

print("✅ Sales Data Loaded:")
print(df_sales.head(), "\n")


print("Sales Columns:", df_sales.columns)
#Summarize Sales Data
def summarize_sales(df):
    """
    Prints and returns total units and retail sales.
    Columns: ST_Units and ST_Retail_$.
    """
    total_units = df['ST_Units'].sum()
    total_sales = df['ST_Retail_$'].sum()
    
    print("📊 BIOLAGE SALES SUMMARY")
    print("----------------------------")
    print(f"Total Units: {total_units:,.0f}")
    print(f"Total Sales: ${total_sales:,.2f}")
    
    return total_units, total_sales
# ========== Run when file is executed directly ==========
if __name__ == "__main__":
    print("\n✅ Starting Biolage Sales Summary...\n")
    summarize_sales(df_sales)
    print("\n✅ Done.\n")
