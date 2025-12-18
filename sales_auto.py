import pandas as pd
from pathlib import Path
from prophet import Prophet

# =============================
# Config
# =============================
INPUT_XLSX         = Path("Biolage Sales Data.xlsx")
INPUT_SHEET        = "Raw Data_Cleaned"

OUTPUT_XLSX        = Path("Biolage Sales Data_Filtered.xlsx")   # QA workbook (keep)
OUTPUT_ANALYTICAL  = Path("Analytical Table.xlsx")              # QA-lite workbook (keep)
OUTPUT_PROPHET_IN  = Path("Biolage Prophet Input.xlsx")         # keep
OUTPUT_PROPHET_OUT = Path("Biolage Prophet Output.xlsx")        # keep

RUN_PROPHET_EXPORT = True
RUN_PROPHET_MODEL  = True

# =============================
# Helpers
# =============================
def norm_week(s):
    return pd.to_datetime(s, errors="coerce").dt.normalize()

def norm_str(s):
    return s.astype(str).str.strip()

def assert_unique(df, keys, name):
    dup = df.duplicated(subset=keys).sum()
    if dup:
        raise ValueError(f"{name} has {dup} duplicate rows on {keys}")

# =============================
# Load Data
# =============================
df = pd.read_excel(INPUT_XLSX, sheet_name=INPUT_SHEET)
df.columns = [c.strip() for c in df.columns]

# =============================
# Date prep (fix: stable normalize)
# =============================
if "Week End" in df.columns:
    df["Week"] = norm_week(df["Week End"])
    df = df.drop(columns=["Week End"])
elif "Week" in df.columns:
    df["Week"] = norm_week(df["Week"])
else:
    raise KeyError("Expected 'Week End' or 'Week' column in sales file.")

# =============================
# Numeric hygiene
# =============================
df["ST_Units"]    = pd.to_numeric(df["ST_Units"], errors="coerce")
df["ST_Retail_$"] = pd.to_numeric(df["ST_Retail_$"], errors="coerce")

dup_count_before = df.duplicated().sum()
df = df.drop_duplicates()
df = df.sort_values("Week", ascending=False).reset_index(drop=True)

# =============================
# Week Mapping (TTM / LY / PY)
# =============================
unique_weeks = sorted(df["Week"].dropna().unique(), reverse=True)

week_map = {}
for i, wk in enumerate(unique_weeks):
    if i < 52:
        week_map[wk] = "TTM"
    elif i < 104:
        week_map[wk] = "LY"
    else:
        week_map[wk] = "PY"

df["Week Mapping"] = df["Week"].map(week_map)
df["Include"] = df["Week Mapping"].apply(lambda x: "Include" if x in {"TTM", "LY"} else "Exclude")

# =============================
# Subset + rename measures (keep only included)
# =============================
keep_cols = ["Week", "Week Mapping", "Franchise", "ST_Retail_$", "ST_Units", "Include"]
if "Year" in df.columns:
    keep_cols.append("Year")

df_out = (
    df.loc[df["Include"] == "Include", keep_cols]
      .rename(columns={"ST_Retail_$": "Sales", "ST_Units": "Units"})
      .copy()
)

df_out["Week"] = norm_week(df_out["Week"])
df_out["Franchise"] = norm_str(df_out["Franchise"])

if "Year" not in df_out.columns:
    df_out["Year"] = df_out["Week"].dt.year

# =============================
# Alteryx-like Summarize (Week Mapping × Franchise)
# =============================
summ_wm_fr = df_out.groupby(["Week Mapping", "Franchise"], as_index=False)[["Sales", "Units"]].sum()

# =============================
# Cross Tabs (Franchise rows; LY/TTM columns)
# =============================
sales_ct = summ_wm_fr.pivot_table(index="Franchise", columns="Week Mapping", values="Sales", aggfunc="sum", fill_value=0)
units_ct = summ_wm_fr.pivot_table(index="Franchise", columns="Week Mapping", values="Units", aggfunc="sum", fill_value=0)

for pvt in (sales_ct, units_ct):
    for col in ("LY", "TTM"):
        if col not in pvt.columns:
            pvt[col] = 0
    pvt.sort_index(axis=1, inplace=True)

sales_ct = sales_ct[["LY", "TTM"]].rename(columns={"LY": "LY_Sales", "TTM": "TTM_Sales"})
units_ct = units_ct[["LY", "TTM"]].rename(columns={"LY": "LY_Units", "TTM": "TTM_Units"})

# =============================
# Analytical Table (merge Sales + Units)
# =============================
analytical_tbl = sales_ct.join(units_ct, how="outer").reset_index().fillna(0)

analytical_tbl["Sales_Growth"] = (analytical_tbl["TTM_Sales"] - analytical_tbl["LY_Sales"]) / analytical_tbl["LY_Sales"].replace({0: pd.NA})
analytical_tbl["Units_Growth"] = (analytical_tbl["TTM_Units"] - analytical_tbl["LY_Units"]) / analytical_tbl["LY_Units"].replace({0: pd.NA})

total_LY_sales  = analytical_tbl["LY_Sales"].sum()
total_TTM_sales = analytical_tbl["TTM_Sales"].sum()

analytical_tbl["CTG"] = (analytical_tbl["TTM_Sales"] - analytical_tbl["LY_Sales"]) / total_LY_sales if total_LY_sales != 0 else pd.NA
analytical_tbl["Distribution"] = analytical_tbl["TTM_Sales"] / total_TTM_sales if total_TTM_sales != 0 else pd.NA

final_cols = [
    "Franchise",
    "LY_Sales", "TTM_Sales",
    "LY_Units", "TTM_Units",
    "Sales_Growth", "Units_Growth",
    "CTG", "Distribution"
]
analytical_tbl = analytical_tbl[[c for c in final_cols if c in analytical_tbl.columns]].sort_values("TTM_Sales", ascending=False)

# =============================
# Prophet-style & Avg Price tables (Week × Franchise)
# =============================
avg_price_tbl = (
    df_out.groupby(["Week", "Franchise"], as_index=False)
          .agg(Sum_Units=("Units", "sum"), Sum_Sales=("Sales", "sum"))
)

avg_price_tbl["Average_Price"] = avg_price_tbl["Sum_Sales"] / avg_price_tbl["Sum_Units"].replace({0: pd.NA})
avg_price_tbl = avg_price_tbl.sort_values(["Week", "Franchise"]).reset_index(drop=True)

assert_unique(avg_price_tbl, ["Week", "Franchise"], "avg_price_tbl")

# Prophet input table
prophet_tbl = avg_price_tbl[["Week", "Franchise", "Sum_Units"]].copy() if RUN_PROPHET_EXPORT else None

# =============================
# Extra QA tabs (keep full QA)
# =============================
overall_units = df_out["Units"].sum()
overall_sales = df_out["Sales"].sum()

franchise_summary = df_out.groupby("Franchise", as_index=False)[["Units", "Sales"]].sum().sort_values("Sales", ascending=False)
year_summary      = df_out.groupby("Year", as_index=False)[["Units", "Sales"]].sum().sort_values("Year")
wm_summary        = df_out.groupby("Week Mapping", as_index=False)[["Units", "Sales"]].sum().sort_values("Week Mapping", ascending=False)
weekly_summary    = df_out.groupby("Week", as_index=False)[["Units", "Sales"]].sum().sort_values("Week", ascending=False)

dq_cols = [c for c in ["Week", "Franchise", "ST_Retail_$", "ST_Units", "Year", "Week Mapping"] if c in df.columns]
nulls = df[dq_cols].isna().sum(min_count=1)
dq_nulls = nulls.to_frame(name="Null_Count").reset_index().rename(columns={"index": "Column"})

neg_units = (df["ST_Units"] < 0).sum() if "ST_Units" in df.columns else 0
neg_sales = (df["ST_Retail_$"] < 0).sum() if "ST_Retail_$" in df.columns else 0

dq_summary = pd.DataFrame({
    "Metric": [
        "Duplicate rows removed",
        "Negative ST_Units rows",
        "Negative ST_Retail_$ rows",
        "Distinct weeks (full data)",
        "Distinct weeks (included only)",
        "Latest week (full data)",
        "Earliest week (full data)",
        "Overall Units (included)",
        "Overall Sales (included)"
    ],
    "Value": [
        int(dup_count_before),
        int(neg_units),
        int(neg_sales),
        int(df["Week"].nunique()),
        int(df_out["Week"].nunique()),
        df["Week"].max(),
        df["Week"].min(),
        f"{overall_units:,.0f}",
        f"${overall_sales:,.2f}"
    ]
})

# =============================
# Prophet MODELING (optional)
# =============================
merged_dataset = None
if RUN_PROPHET_MODEL:
    if prophet_tbl is None:
        raise ValueError("prophet_tbl is None. Set RUN_PROPHET_EXPORT=True.")

    dataset = prophet_tbl.rename(columns={"Week": "ds", "Franchise": "ID", "Sum_Units": "y"}).copy()
    dataset["ds"] = norm_week(dataset["ds"])
    dataset["ID"] = norm_str(dataset["ID"])
    dataset = dataset.sort_values(["ID", "ds"])

    results = []
    for fr_id, df_group in dataset.groupby("ID"):
        prophet_df = df_group[["ds", "y"]].copy().sort_values("ds")

        # Guardrail: Prophet stability
        if prophet_df["y"].notna().sum() < 30:
            continue

        m = Prophet(yearly_seasonality=True, weekly_seasonality=True)
        m.fit(prophet_df)

        future = m.make_future_dataframe(periods=365)
        forecast = m.predict(future)

        for col in ["weekly", "yearly"]:
            if col not in forecast.columns:
                forecast[col] = pd.NA

        forecast["ID"] = fr_id
        results.append(forecast)

    all_forecasts = pd.concat(results, ignore_index=True) if results else pd.DataFrame(columns=["ID","ds","trend","weekly","yearly","yhat"])

    merged_dataset = (
        dataset.merge(
            all_forecasts[["ID", "ds", "trend", "weekly", "yearly", "yhat"]],
            on=["ID", "ds"],
            how="left"
        )
        [["ID", "ds", "y", "trend", "weekly", "yearly", "yhat"]]
        .copy()
    )

# =============================
# WRITE QA FILES (keep previous files)
# =============================
cols_final = ["Week", "Week Mapping", "Franchise", "Sales", "Units", "Year", "Include"]

with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    df_out[cols_final].assign(Week=df_out["Week"].dt.date).to_excel(writer, sheet_name="TTM_LY_Only", index=False)
    sales_ct.reset_index().to_excel(writer, sheet_name="CrossTab_Sales", index=False)
    units_ct.reset_index().to_excel(writer, sheet_name="CrossTab_Units", index=False)
    analytical_tbl.to_excel(writer, sheet_name="Analytical_Table", index=False)

    # QA tabs preserved
    franchise_summary.to_excel(writer, sheet_name="Franchise_Summary", index=False)
    year_summary.to_excel(writer, sheet_name="Year_Summary", index=False)
    wm_summary.to_excel(writer, sheet_name="WeekMapping_Summary", index=False)
    weekly_summary.assign(Week=weekly_summary["Week"].dt.date).to_excel(writer, sheet_name="Weekly_Summary", index=False)

    avg_price_tbl.assign(Week=avg_price_tbl["Week"].dt.date).to_excel(writer, sheet_name="Avg_Price", index=False)

    dq_summary.to_excel(writer, sheet_name="Data_Quality", index=False, startrow=0)
    dq_nulls.to_excel(writer, sheet_name="Data_Quality", index=False, startrow=len(dq_summary) + 3)

with pd.ExcelWriter(OUTPUT_ANALYTICAL, engine="openpyxl") as writer2:
    df_out[cols_final].assign(Week=df_out["Week"].dt.date).to_excel(writer2, sheet_name="Raw_Data", index=False)
    analytical_tbl.to_excel(writer2, sheet_name="Analytical_Table", index=False)
    franchise_summary.to_excel(writer2, sheet_name="Franchise_Summary", index=False)
    avg_price_tbl.assign(Week=avg_price_tbl["Week"].dt.date).to_excel(writer2, sheet_name="Avg_Price", index=False)

if RUN_PROPHET_EXPORT and prophet_tbl is not None:
    with pd.ExcelWriter(OUTPUT_PROPHET_IN, engine="openpyxl") as writer3:
        prophet_tbl.assign(Week=prophet_tbl["Week"].dt.date).to_excel(writer3, sheet_name="Prophet_Input", index=False)

if RUN_PROPHET_MODEL and merged_dataset is not None:
    out = merged_dataset.copy()
    out["ds"] = pd.to_datetime(out["ds"]).dt.date
    with pd.ExcelWriter(OUTPUT_PROPHET_OUT, engine="openpyxl") as writer4:
        out.to_excel(writer4, sheet_name="Prophet_Output", index=False)

print("✅ Sales pipeline complete (QA preserved)")
print(f"   Main QA file:         {OUTPUT_XLSX}")
print(f"   Analytical only file: {OUTPUT_ANALYTICAL}")
print(f"   Prophet input file:   {OUTPUT_PROPHET_IN if RUN_PROPHET_EXPORT else 'SKIPPED'}")
print(f"   Prophet output file:  {OUTPUT_PROPHET_OUT if RUN_PROPHET_MODEL else 'SKIPPED'}")





