import pandas as pd
from pathlib import Path
import numpy as np

# =========================
# 1) Load data
# =========================
sales_file = Path("Biolage Sales Data.xlsx")
df = pd.read_excel(sales_file, sheet_name="Raw Data_Cleaned")

# --- Ensure we have a Date column (rename if needed) ---
if "Date" not in df.columns:
    if "Week End" in df.columns:
        df = df.rename(columns={"Week End": "Date"})
    else:
        raise KeyError("No 'Date' or 'Week End' column found.")

# =========================
# 2) Basic cleaning
# =========================
# Parse Date and drop rows with invalid dates
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.dropna(subset=["Date"])

# Normalize to midnight (removes any time component)
df["Date"] = df["Date"].dt.normalize()

# Make numeric (coerce text/nulls), drop exact duplicate rows
for col in ["ST_Units", "ST_Retail_$"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
df = df.drop_duplicates()

# =========================
# 3) Build week ranking (latest → oldest)
# =========================
# Unique week-ending dates sorted newest first
unique_weeks_desc = (
    pd.Series(df["Date"].unique())
    .sort_values(ascending=False)
    .reset_index(drop=True)
)

# Map each unique week to a rank: 1 = most recent week
week_rank_map = {dt: i + 1 for i, dt in enumerate(unique_weeks_desc)}
df["Week_Rank"] = df["Date"].map(week_rank_map)

# =========================
# 4) Week mapping & inclusion flags
# =========================
# TTM = latest 52 weeks (ranks 1..52)
# LY  = next 52 weeks (ranks 53..104)
# PY  = everything older (rank >=105)
conditions = [
    df["Week_Rank"] <= 52,
    (df["Week_Rank"] >= 53) & (df["Week_Rank"] <= 104),
]
choices = ["TTM", "LY"]

df["Week_Mapping"] = np.select(conditions, choices, default="PY")
df["Inclusion"] = np.where(df["Week_Mapping"].isin(["TTM", "LY"]), "Include", "Exclude")

# =========================
# 5) Quick QA prints
# =========================
print("\n✅ Week mapping applied.")
print(f"- Total unique weeks: {len(unique_weeks_desc)}")
latest_week = unique_weeks_desc.iloc[0]
oldest_week = unique_weeks_desc.iloc[-1]
print(f"- Latest week: {latest_week.date()} | Oldest week: {oldest_week.date()}")

# Counts by mapping
print("\n🧮 Counts by Week_Mapping:")
print(df["Week_Mapping"].value_counts(dropna=False).to_string())

# Optional: check how many unique weeks per bucket (better QA)
unique_weeks_by_bucket = (
    df.groupby("Week_Mapping")["Date"].nunique().sort_index()
)
print("\n📅 Unique weeks by bucket:")
print(unique_weeks_by_bucket.to_string())

# Optional: totals by bucket (QA)
if {"ST_Units", "ST_Retail_$"}.issubset(df.columns):
    totals_by_bucket = (
        df.groupby("Week_Mapping")[["ST_Units", "ST_Retail_$"]]
        .sum()
        .sort_index()
    )
    print("\n💵 Totals by Week_Mapping (Units / Sales):")
    print(totals_by_bucket.to_string())
else:
    print("\n⚠️ Skipping totals by bucket (ST_Units / ST_Retail_$ not found).")

# =========================
# 6) (Optional) Save back out
# =========================
# Uncomment to write a new file with the mapping added.
# out_path = Path("Biolage Sales Data_with_WeekMapping.xlsx")
# with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
#     df.to_excel(writer, sheet_name="Raw Data_Cleaned", index=False)
# print(f"\n💾 Saved: {out_path}\n")

if __name__ == "__main__":
    print("\n✅ Done.\n")

