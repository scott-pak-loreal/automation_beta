import pandas as pd
from pathlib import Path

# ---- File Path ----
sales_file = Path("Biolage Sales Data.xlsx")

# ---- Load the 'Raw Data_Cleaned' sheet ----
df_sales = pd.read_excel(sales_file, sheet_name="Raw Data_Cleaned")

# ---- Clean date column ----
df_sales['Week End'] = pd.to_datetime(df_sales['Week End'], errors='coerce').dt.date
if 'Week' in df_sales.columns:
    df_sales = df_sales.drop(columns=['Week'])
df_sales = df_sales.rename(columns={'Week End': 'Date'})

# ---- Ensure numeric columns are clean ----
df_sales['ST_Units'] = pd.to_numeric(df_sales['ST_Units'], errors='coerce')
df_sales['ST_Retail_$'] = pd.to_numeric(df_sales['ST_Retail_$'], errors='coerce')

# ---- Drop duplicates ----
df_sales = df_sales.drop_duplicates()

print("✅ Raw Data_Cleaned Loaded Successfully\n")
print(df_sales.head(), "\n")

# ---- Summarize sales ----
def summarize_sales(df):
    """
    Prints overall totals and summaries by Franchise and Year.
    """
    total_units = df['ST_Units'].sum()
    total_sales = df['ST_Retail_$'].sum()

    print("\n📊 BIOLAGE SALES SUMMARY (Overall)")
    print("----------------------------------")
    print(f"Total Units: {total_units:,.0f}")
    print(f"Total Sales: ${total_sales:,.2f}\n")

    # ---- Group by Franchise ----
    print("🏢 Summary by Franchise:")
    franchise_summary = (
        df.groupby('Franchise', as_index=False)[['ST_Units', 'ST_Retail_$']]
        .sum()
        .sort_values(by='ST_Retail_$', ascending=False)
    )
    print(franchise_summary.to_string(index=False), "\n")

    # ---- Group by Year ----
    print("📅 Summary by Year:")
    year_summary = (
        df.groupby('Year', as_index=False)[['ST_Units', 'ST_Retail_$']]
        .sum()
        .sort_values(by='Year')
    )
    print(year_summary.to_string(index=False), "\n")

    return total_units, total_sales, franchise_summary, year_summary


# ---- Run script ----
if __name__ == "__main__":
    print("\n✅ Starting Biolage Sales QA...\n")
    summarize_sales(df_sales)
    print("✅ Done.\n")


