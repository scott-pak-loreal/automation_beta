import pandas as pd
from pathlib import Path
from prophet import Prophet

# =============================
# Config
# =============================
INPUT_XLSX         = Path("Biolage Sales Data.xlsx")
INPUT_SHEET        = "Raw Data_Cleaned"
OUTPUT_XLSX        = Path("Biolage Sales Data_Filtered.xlsx")
OUTPUT_ANALYTICAL  = Path("Analytical Table.xlsx")
OUTPUT_PROPHET_IN  = Path("Biolage Prophet Input.xlsx")
OUTPUT_PROPHET_OUT = Path("Biolage Prophet Output.xlsx")

# Toggles
RUN_PROPHET_EXPORT = True   # Turn OFF → False to skip Prophet input file
RUN_PROPHET_MODEL  = True   # Turn OFF → False to skip Prophet modeling


# =============================
# Load
# =============================
df = pd.read_excel(INPUT_XLSX, sheet_name=INPUT_SHEET)

# =============================
# Date prep (remove time)
# =============================
df['Week End'] = pd.to_datetime(df['Week End'], errors='coerce').dt.normalize()
if 'Week' in df.columns:
    df = df.drop(columns=['Week'])
df = df.rename(columns={'Week End': 'Week'})

# =============================
# Numeric hygiene
# =============================
df['ST_Units']    = pd.to_numeric(df['ST_Units'], errors='coerce')
df['ST_Retail_$'] = pd.to_numeric(df['ST_Retail_$'], errors='coerce')

# Remove exact duplicate rows
dup_count_before = df.duplicated().sum()
df = df.drop_duplicates()

# Sort newest → oldest
df = df.sort_values('Week', ascending=False).reset_index(drop=True)

# =============================
# Week Mapping (TTM / LY / PY)
# =============================
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
df['Include'] = df['Week Mapping'].apply(lambda x: 'Include' if x in {'TTM', 'LY'} else 'Exclude')

# =============================
# Subset + rename measures (keep only included)
# =============================
keep_cols = ['Week', 'Week Mapping', 'Franchise', 'ST_Retail_$', 'ST_Units', 'Include']
if 'Year' in df.columns:  # pass through Year if it exists
    keep_cols.append('Year')

df_out = (
    df.loc[df['Include'] == 'Include', keep_cols]
      .rename(columns={'ST_Retail_$': 'Sales', 'ST_Units': 'Units'})
      .copy()
)

# Ensure Year exists for QA if it wasn't in source
if 'Year' not in df_out.columns:
    df_out['Year'] = df_out['Week'].dt.year

# =============================
# Alteryx-like Summarize (Week Mapping × Franchise)
# =============================
summ_wm_fr = (
    df_out.groupby(['Week Mapping', 'Franchise'], as_index=False)[['Sales', 'Units']].sum()
)

# =============================
# Cross Tabs (Franchise rows; LY/TTM columns)
# =============================
sales_ct = summ_wm_fr.pivot_table(
    index='Franchise', columns='Week Mapping', values='Sales',
    aggfunc='sum', fill_value=0
)
units_ct = summ_wm_fr.pivot_table(
    index='Franchise', columns='Week Mapping', values='Units',
    aggfunc='sum', fill_value=0
)

# Guarantee columns exist & order
for pvt in (sales_ct, units_ct):
    for col in ('LY', 'TTM'):
        if col not in pvt.columns:
            pvt[col] = 0
    pvt.sort_index(axis=1, inplace=True)

# Rename for final output
sales_ct = sales_ct[['LY', 'TTM']].rename(columns={'LY': 'LY_Sales', 'TTM': 'TTM_Sales'})
units_ct = units_ct[['LY', 'TTM']].rename(columns={'LY': 'LY_Units', 'TTM': 'TTM_Units'})

# =============================
# Analytical Table (merge Sales + Units)
# =============================
analytical_tbl = (
    sales_ct.join(units_ct, how='outer')
            .reset_index()  # Franchise
            .fillna(0)
)

# YoY / Growth (TTM / LY - 1)
analytical_tbl['Sales_Growth'] = (
    (analytical_tbl['TTM_Sales'] - analytical_tbl['LY_Sales']) /
    analytical_tbl['LY_Sales'].replace({0: pd.NA})
)
analytical_tbl['Units_Growth'] = (
    (analytical_tbl['TTM_Units'] - analytical_tbl['LY_Units']) /
    analytical_tbl['LY_Units'].replace({0: pd.NA})
)

# =============================
# CTG and Distribution
# =============================
total_LY_sales  = analytical_tbl['LY_Sales'].sum()
total_TTM_sales = analytical_tbl['TTM_Sales'].sum()

# CTG = (TTM Sales - LY Sales) / Sum_LY Sales
if total_LY_sales != 0:
    analytical_tbl['CTG'] = (analytical_tbl['TTM_Sales'] - analytical_tbl['LY_Sales']) / total_LY_sales
else:
    analytical_tbl['CTG'] = pd.NA

# Distribution = TTM Sales / Sum_TTM Sales
if total_TTM_sales != 0:
    analytical_tbl['Distribution'] = analytical_tbl['TTM_Sales'] / total_TTM_sales
else:
    analytical_tbl['Distribution'] = pd.NA

# Final column order
final_cols = [
    'Franchise',
    'LY_Sales', 'TTM_Sales',
    'LY_Units', 'TTM_Units',
    'Sales_Growth', 'Units_Growth',
    'CTG', 'Distribution'
]
analytical_tbl = analytical_tbl[[c for c in final_cols if c in analytical_tbl.columns]]
analytical_tbl = analytical_tbl.sort_values('TTM_Sales', ascending=False)

# =============================
# Prophet-style & Avg Price tables
# =============================
# Week x Franchise, Sum Units & Sum Sales
avg_price_tbl = (
    df_out.groupby(['Week', 'Franchise'], as_index=False)
          .agg(Sum_Units=('Units', 'sum'),
               Sum_Sales=('Sales', 'sum'))
)

avg_price_tbl['Average_Price'] = (
    avg_price_tbl['Sum_Sales'] /
    avg_price_tbl['Sum_Units'].replace({0: pd.NA})
)

# Sort by Week ascending, then Franchise
avg_price_tbl = avg_price_tbl.sort_values(['Week', 'Franchise'])

# Prophet input table (only if enabled)
if RUN_PROPHET_EXPORT:
    prophet_tbl = (
        avg_price_tbl[['Week', 'Franchise', 'Sum_Units']]
            .copy()
    )
else:
    prophet_tbl = None

# =============================
# Extra QA tabs
# =============================
overall_units = df_out['Units'].sum()
overall_sales = df_out['Sales'].sum()

franchise_summary = (
    df_out.groupby('Franchise', as_index=False)[['Units', 'Sales']].sum()
          .sort_values('Sales', ascending=False)
)

year_summary = (
    df_out.groupby('Year', as_index=False)[['Units', 'Sales']].sum()
          .sort_values('Year')
)

wm_summary = (
    df_out.groupby('Week Mapping', as_index=False)[['Units', 'Sales']].sum()
          .sort_values('Week Mapping', ascending=False)
)

weekly_summary = (
    df_out.groupby('Week', as_index=False)[['Units', 'Sales']].sum()
          .sort_values('Week', ascending=False)
)

# Data Quality
dq_cols = ['Week', 'Franchise', 'ST_Retail_$', 'ST_Units', 'Year', 'Week Mapping']
dq_cols = [c for c in dq_cols if c in df.columns]
nulls = df[dq_cols].isna().sum(min_count=1)
dq_nulls = nulls.to_frame(name='Null_Count').reset_index().rename(columns={'index': 'Column'})

neg_units = (df['ST_Units'] < 0).sum() if 'ST_Units' in df.columns else 0
neg_sales = (df['ST_Retail_$'] < 0).sum() if 'ST_Retail_$' in df.columns else 0

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

# =============================
# Write main Excel (full QA)
# =============================
cols_final = ['Week', 'Week Mapping', 'Franchise', 'Sales', 'Units', 'Year', 'Include']

with pd.ExcelWriter(OUTPUT_XLSX, engine='openpyxl') as writer:
    # Raw included rows (for traceability)
    df_out[cols_final].to_excel(writer, sheet_name='TTM_LY_Only', index=False)

    # Crosstabs like Alteryx
    sales_ct.reset_index().to_excel(writer, sheet_name='CrossTab_Sales', index=False)
    units_ct.reset_index().to_excel(writer, sheet_name='CrossTab_Units', index=False)

    # Final Analytical Table
    analytical_tbl.to_excel(writer, sheet_name='Analytical_Table', index=False)

    # QA tabs
    franchise_summary.to_excel(writer, sheet_name='Franchise_Summary', index=False)
    year_summary.to_excel(writer, sheet_name='Year_Summary', index=False)
    wm_summary.to_excel(writer, sheet_name='WeekMapping_Summary', index=False)
    weekly_summary.to_excel(writer, sheet_name='Weekly_Summary', index=False)

    # Avg price tab
    avg_price_tbl.to_excel(writer, sheet_name='Avg_Price', index=False)

    # Data Quality
    dq_summary.to_excel(writer, sheet_name='Data_Quality', index=False, startrow=0)
    dq_nulls.to_excel(writer, sheet_name='Data_Quality', index=False, startrow=len(dq_summary) + 3)

# =============================
# Write SECOND Excel: "Analytical Table.xlsx"
# Only: Raw_Data + Analytical_Table + Franchise_Summary + Avg_Price
# =============================
with pd.ExcelWriter(OUTPUT_ANALYTICAL, engine='openpyxl') as writer2:
    df_out[cols_final].to_excel(writer2, sheet_name='Raw_Data', index=False)
    analytical_tbl.to_excel(writer2, sheet_name='Analytical_Table', index=False)
    franchise_summary.to_excel(writer2, sheet_name='Franchise_Summary', index=False)
    avg_price_tbl.to_excel(writer2, sheet_name='Avg_Price', index=False)

# =============================
# Write THIRD Excel: Prophet INPUT (optional)
# =============================
if RUN_PROPHET_EXPORT and prophet_tbl is not None:
    with pd.ExcelWriter(OUTPUT_PROPHET_IN, engine='openpyxl') as writer3:
        prophet_tbl.to_excel(writer3, sheet_name='Prophet_Input', index=False)

# =============================
# Prophet MODELING (optional)
# =============================
if RUN_PROPHET_MODEL:
    if prophet_tbl is None:
        raise ValueError("prophet_tbl is None. Make sure RUN_PROPHET_EXPORT = True so the Prophet input table is created.")

    # 1) Prepare dataset: rename columns to ds / ID / y (as in your R code)
    dataset = (
        prophet_tbl
        .rename(columns={
            'Week': 'ds',
            'Franchise': 'ID',
            'Sum_Units': 'y'
        })
        .copy()
    )

    # Ensure ds is date-only
    dataset['ds'] = pd.to_datetime(dataset['ds'], errors='coerce').dt.normalize()

    # Optional: sort for safety
    dataset = dataset.sort_values(['ID', 'ds'])

    # 2) Split by ID (Franchise)
    franchise_groups = {fr_id: df for fr_id, df in dataset.groupby('ID')}

    results = []

    for fr_id, df_group in franchise_groups.items():
        # Keep only the columns Prophet needs
        prophet_df = df_group[['ds', 'y']].copy().sort_values('ds')

        # 3) Fit Prophet model
        m = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True
        )
        m.fit(prophet_df)

        # 4) Create future dataframe (365 days, like your R code)
        future = m.make_future_dataframe(periods=365)

        # 5) Predict
        forecast = m.predict(future)

        # Ensure weekly/yearly columns exist (to mirror R safety)
        if 'weekly' not in forecast.columns:
            forecast['weekly'] = pd.NA
        if 'yearly' not in forecast.columns:
            forecast['yearly'] = pd.NA

        # Add ID (Franchise) column
        forecast['ID'] = fr_id

        # Add an Index column similar to rownames in R
        forecast = forecast.reset_index(drop=True)
        forecast['Index'] = forecast.index

        results.append(forecast)

    # 6) Combine all forecasts
    all_forecasts = pd.concat(results, ignore_index=True)

    # Optional: filter dates if you want to mimic the commented R line
    # all_forecasts = all_forecasts[all_forecasts['ds'] < pd.to_datetime("2025-01-05")]

    # 7) Merge original dataset with forecast (on ID + ds) and keep same columns as R
    merged_dataset = (
        dataset
        .merge(
            all_forecasts[['ID', 'ds', 'trend', 'weekly', 'yearly', 'yhat']],
            on=['ID', 'ds'],
            how='left'
        )
        [['ID', 'ds', 'y', 'trend', 'weekly', 'yearly', 'yhat']]
        .copy()
    )

    # 8) Optional check: expected row count (1052 in your R script)
    expected_n = 1052
    if len(merged_dataset) != expected_n:
        print(f"⚠️ Warning: merged_dataset has {len(merged_dataset)} rows, expected {expected_n}.")

    # 9) Write to Excel
    with pd.ExcelWriter(OUTPUT_PROPHET_OUT, engine='openpyxl') as writer4:
        merged_dataset.to_excel(writer4, sheet_name='Prophet_Output', index=False)

    print(f"✅ Prophet modeling complete. Output file: {OUTPUT_PROPHET_OUT}")

# =============================
# Final prints
# =============================
print("✅ Files created:")
print(f"   Main QA file:         {OUTPUT_XLSX}")
print(f"   Analytical only file: {OUTPUT_ANALYTICAL}")
if RUN_PROPHET_EXPORT:
    print(f"   Prophet input file:   {OUTPUT_PROPHET_IN}")
if RUN_PROPHET_MODEL:
    print(f"   Prophet output file:  {OUTPUT_PROPHET_OUT}")
print(f"   Rows exported (included only): {len(df_out):,}")
print(f"   Unique weeks (included only):  {df_out['Week'].nunique()}")




