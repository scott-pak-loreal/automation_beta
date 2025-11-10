import pandas as pd
from pathlib import Path

# -----------------------------
# Config
# -----------------------------
INPUT_XLSX  = Path("Biolage Sales Data.xlsx")
INPUT_SHEET = "Raw Data_Cleaned"
OUTPUT_XLSX = Path("Biolage Sales Data_Filtered.xlsx")

# -----------------------------
# Load
# -----------------------------
df = pd.read_excel(INPUT_XLSX, sheet_name=INPUT_SHEET)

# -----------------------------
# Date prep
# -----------------------------
df['Week End'] = pd.to_datetime(df['Week End'], errors='coerce')
if 'Week' in df.columns:
    df = df.drop(columns=['Week'])
df = df.rename(columns={'Week End': 'Week'})

# -----------------------------
# Numeric hygiene
# -----------------------------
df['ST_Units']     = pd.to_numeric(df['ST_Units'], errors='coerce')
df['ST_Retail_$']  = pd.to_numeric(df['ST_Retail_$'], errors='coerce')

# Remove exact duplicate rows
dup_count_before = df.duplicated().sum()
df = df.drop_duplicates()

# Sort newest → oldest
df = df.sort_values('Week', ascending=False).reset_index(drop=True)

# -----------------------------
# Week Mapping (TTM / LY / PY)
# -----------------------------
unique_weeks = sorted(df['Week'].dropna().unique(), reverse=True)

week_map = {}
for i, wk in enumerate(unique_weeks):
    if i < 52:
        week_map[wk] = 'TTM'
    elif i < 104:
        week_map[wk] = 'LY'
    else:
        week_map[wk] = 'PY'

df['Week Mapping'] = df['Week'].map(week_map)

# Include / Exclude flag
df['Include'] = df['Week Mapping'].apply(lambda x: 'Include' if x in {'TTM','LY'} else 'Exclude')

# -----------------------------
# Filter to included rows
# -----------------------------
df_out = df[df['Include'] == 'Include'].copy()

# Rename for clarity in outputs (keep originals intact in df if you wish)
df_out = df_out.rename(columns={'ST_Retail_$': 'Sales', 'ST_Units': 'Units'})

# Ensure expected helper columns exist (you said they’re already in the data)
# If not, uncomment this to derive Year from Week:
# if 'Year' not in df_out.columns:
#     df_out['Year'] = df_out['Week'].dt.year

# -----------------------------
# QA Summaries
# -----------------------------
# Overall totals (included set)
overall_units = df_out['Units'].sum()
overall_sales = df_out['Sales'].sum()

# By Franchise
franchise_summary = (
    df_out.groupby('Franchise', as_index=False)[['Units','Sales']].sum()
      .sort_values('Sales', ascending=False)
)

# By Year
year_summary = (
    df_out.groupby('Year', as_index=False)[['Units','Sales']].sum()
      .sort_values('Year')
)

# By Week Mapping (TTM vs LY)
wm_summary = (
    df_out.groupby('Week Mapping', as_index=False)[['Units','Sales']].sum()
      .sort_values('Week Mapping', ascending=False)
)

# Franchise × Week Mapping pivot
franchise_wm_pivot_units = (
    df_out.pivot_table(index='Franchise', columns='Week Mapping', values='Units', aggfunc='sum', fill_value=0)
)
franchise_wm_pivot_sales = (
    df_out.pivot_table(index='Franchise', columns='Week Mapping', values='Sales', aggfunc='sum', fill_value=0)
)

# Weekly summary (for quick spot checks)
weekly_summary = (
    df_out.groupby('Week', as_index=False)[['Units','Sales']].sum()
      .sort_values('Week', ascending=False)
)

# -----------------------------
# Data Quality sheet
# -----------------------------
nulls = df[['Week','Franchise','ST_Retail_$','ST_Units','Year','Week Mapping']].isna().sum()
dq_nulls = nulls.to_frame(name='Null_Count').reset_index().rename(columns={'index':'Column'})

neg_units  = (df['ST_Units']    < 0).sum()
neg_sales  = (df['ST_Retail_$'] < 0).sum()

dq_summary = pd.DataFrame({
    'Metric': [
        'Duplicate rows removed',
        'Negative ST_Units rows',
        'Negative ST_Retail_$ rows',
        'Distinct weeks (full data)',
        'Distinct weeks (included only)',
        'Latest week (full data)',
        'Earliest week (full data)',
        'Overall Units (included)',
        'Overall Sales (included)'
    ],
    'Value': [
        int(dup_count_before),
        int(neg_units),
        int(neg_sales),
        int(df['Week'].nunique()),
        int(df_out['Week'].nunique()),
        df['Week'].max(),
        df['Week'].min(),
        f"{overall_units:,.0f}",
        f"${overall_sales:,.2f}"
    ]
})

# -----------------------------
# Write Excel
# -----------------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine='openpyxl') as writer:
    # Final included dataset, with the exact headers you asked
    cols_final = ['Week','Week Mapping','Franchise','Sales','Units','Year','Include']
    df_out[cols_final].to_excel(writer, sheet_name='TTM_LY_Only', index=False)

    # QA tabs
    franchise_summary.to_excel(writer, sheet_name='Franchise_Summary', index=False)
    year_summary.to_excel(writer, sheet_name='Year_Summary', index=False)
    wm_summary.to_excel(writer, sheet_name='WeekMapping_Summary', index=False)
    franchise_wm_pivot_units.to_excel(writer, sheet_name='Fr×WM_Units')
    franchise_wm_pivot_sales.to_excel(writer, sheet_name='Fr×WM_Sales')
    weekly_summary.to_excel(writer, sheet_name='Weekly_Summary', index=False)

    # Data Quality
    dq_summary.to_excel(writer, sheet_name='Data_Quality', index=False, startrow=0)
    dq_nulls.to_excel(writer, sheet_name='Data_Quality', index=False, startrow=len(dq_summary)+3)

print("✅ File created:")
print(f"   {OUTPUT_XLSX}")
print(f"   Rows exported (included only): {len(df_out):,}")
print(f"   Unique weeks (included only):  {df_out['Week'].nunique()}")
