import pandas as pd
import numpy as np
from pathlib import Path

# =============================
# Config
# =============================
INPUT_MEDIA_XLSX   = Path("Biolage Media Data.xlsx")
INPUT_MEDIA_SHEET  = "Consolidated Media Data"
OUTPUT_MEDIA_FILE  = Path("Biolage Media Processed.xlsx")

BRAND_FRANCHISE_NAME = "Biolage - Brand"

# =============================
# Load media data
# =============================
df = pd.read_excel(INPUT_MEDIA_XLSX, sheet_name=INPUT_MEDIA_SHEET)

# Clean column names
df.columns = [c.strip() for c in df.columns]

# Normalize date column
if "Week End (Sat)" in df.columns:
    df = df.rename(columns={"Week End (Sat)": "Week"})

if "Week" in df.columns:
    df["Week"] = pd.to_datetime(df["Week"], errors="coerce").dt.normalize()

# Numeric hygiene
for col in ["Spend", "Impressions"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# =============================
# Franchise remapping
# =============================
if "Franchise" not in df.columns:
    raise KeyError("Expected a 'Franchise' column; check your headers.")

def map_franchise(v: str) -> str:
    v = str(v)
    if "Blowdry Cream" in v:
        return "Styling"
    elif "Prime Day" in v:
        return BRAND_FRANCHISE_NAME
    else:
        return v

df["Franchise"] = df["Franchise"].apply(map_franchise)

# =============================
# Partner_tag logic
# =============================
if "Partner" not in df.columns:
    raise KeyError("Expected a 'Partner' column; check your headers.")

df["Partner_tag"] = np.where(
    df["Franchise"].astype(str).str.contains("- Brand"),
    df["Partner"].astype(str) + "_Brand",
    df["Partner"].astype(str)
)

# =============================
# Skeleton for Partner / Partner_Brand (for summaries)
# =============================
unique_partners = (
    df["Partner"]
      .dropna()
      .drop_duplicates()
      .tolist()
)

skeleton_rows = []
for p in unique_partners:
    skeleton_rows.append({"Partner": p, "Partner_tag": p})
    skeleton_rows.append({"Partner": p, "Partner_tag": f"{p}_Brand"})

skeleton = pd.DataFrame(skeleton_rows).drop_duplicates().reset_index(drop=True)

num_cols = [c for c in ["Spend", "Impressions"] if c in df.columns]

# Summary by Franchise
if num_cols:
    franchise_summary = (
        df.groupby("Franchise", as_index=False)[num_cols]
          .sum()
          .sort_values(num_cols[0], ascending=False)
    )
else:
    franchise_summary = pd.DataFrame()

# Summary by Partner & Partner_tag
if num_cols:
    partner_tag_summary = (
        df.groupby(["Partner", "Partner_tag"], as_index=False)[num_cols]
          .sum()
    )
else:
    partner_tag_summary = pd.DataFrame()

if not partner_tag_summary.empty:
    partner_tag_summary = (
        skeleton
        .merge(partner_tag_summary, on=["Partner", "Partner_tag"], how="left")
    )
    for c in num_cols:
        partner_tag_summary[c] = partner_tag_summary[c].fillna(0)
    partner_tag_summary = partner_tag_summary.sort_values(["Partner", "Partner_tag"])
else:
    partner_tag_summary = skeleton.copy()
    for c in num_cols:
        partner_tag_summary[c] = 0

# Lists
franchise_list = (
    df["Franchise"]
      .dropna()
      .drop_duplicates()
      .sort_values()
      .to_frame(name="Franchise")
)

media_group_list = (
    partner_tag_summary["Partner_tag"]
      .dropna()
      .drop_duplicates()
      .sort_values()
      .to_frame(name="Partner_tag")
)

# ====================================================
# Spend branch (no halo; simple sums + weekly totals)
# ====================================================
spend_franchise = pd.DataFrame()
spend_weekly_totals = pd.DataFrame()

if "Spend" in df.columns:
    # Crosstab Week × Franchise × Partner_tag
    spend_franchise = (
        df.pivot_table(
            index=["Week", "Franchise"],
            columns="Partner_tag",
            values="Spend",
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )
    spend_partner_cols = [
        c for c in spend_franchise.columns if c not in ["Week", "Franchise"]
    ]

    # Weekly totals by Partner (sum across franchises)
    spend_weekly_totals = (
        spend_franchise.groupby("Week")[spend_partner_cols]
        .sum()
        .reset_index()
        .rename(columns={c: f"{c}_TotalSpend" for c in spend_partner_cols})
    )

    # Join totals back
    spend_franchise = spend_franchise.merge(
        spend_weekly_totals, on="Week", how="left"
    )
    spend_franchise.insert(2, "Metric", "Spend")

# ====================================================
# Impressions branch (Biolage-Brand halo + totals)
# ====================================================
imps_franchise = pd.DataFrame()
imps_weekly_totals = pd.DataFrame()

if "Impressions" in df.columns:
    # 1) Crosstab Week × Franchise × Partner_tag (no fill so we preserve NaNs)
    imps_pivot = (
        df.pivot_table(
            index=["Week", "Franchise"],
            columns="Partner_tag",
            values="Impressions",
            aggfunc="sum"
        )
        .reset_index()
    )
    partner_tag_cols = [
        c for c in imps_pivot.columns if c not in ["Week", "Franchise"]
    ]

    # 2) Split BRAND vs non-BRAND franchises
    is_brand = imps_pivot["Franchise"] == BRAND_FRANCHISE_NAME
    brand_rows = imps_pivot[is_brand].drop(columns=["Franchise"])  # right side
    non_brand_rows = imps_pivot[~is_brand].copy()                  # left side

    # 3) Join branding rows by Week and fill NULLs with brand values
    if not brand_rows.empty:
        brand_rows = brand_rows.set_index("Week")
        non_brand_rows = non_brand_rows.merge(
            brand_rows,
            left_on="Week",
            right_index=True,
            how="left",
            suffixes=("", "__brandrow")
        )

        for col in partner_tag_cols:
            bcol = col + "__brandrow"
            if bcol in non_brand_rows.columns:
                non_brand_rows[col] = np.where(
                    non_brand_rows[col].isna(),
                    non_brand_rows[bcol],
                    non_brand_rows[col]
                )
                non_brand_rows.drop(columns=[bcol], inplace=True)

    # Result: all non-brand franchises with halo applied
    imps_franchise = non_brand_rows.copy()
    imps_franchise.insert(2, "Metric", "Impressions")

    # 4) Weekly totals per partner: Partner + Partner_Brand
    imps_week_partner = (
        df.pivot_table(
            index="Week",
            columns="Partner_tag",
            values="Impressions",
            aggfunc="sum"
        )
    )

    imps_weekly_totals = pd.DataFrame(index=imps_week_partner.index)
    for p in unique_partners:
        base = imps_week_partner[p] if p in imps_week_partner.columns else 0
        brand = imps_week_partner[f"{p}_Brand"] if f"{p}_Brand" in imps_week_partner.columns else 0

        base = pd.Series(base).fillna(0)
        brand = pd.Series(brand).fillna(0)

        imps_weekly_totals[f"{p}TotalImps"] = base + brand

    imps_weekly_totals = imps_weekly_totals.reset_index()

    # Join totals back to per-franchise table
    imps_franchise = imps_franchise.merge(
        imps_weekly_totals, on="Week", how="left"
    )

# ====================================================
# Final union (Spend + Impressions)
# ====================================================
metric_union = pd.DataFrame()
to_union = []
if not spend_franchise.empty:
    to_union.append(spend_franchise)
if not imps_franchise.empty:
    to_union.append(imps_franchise)

if to_union:
    metric_union = pd.concat(to_union, ignore_index=True, sort=False)

# Optional: QA check for Impressions
qa_imps = pd.DataFrame()
if "Impressions" in df.columns and not imps_franchise.empty:
    raw_total = df["Impressions"].sum()
    out_total = imps_franchise[
        [c for c in imps_franchise.columns if c not in ["Week", "Franchise", "Metric"] 
         and not c.endswith("TotalImps")]
    ].sum().sum()
    qa_imps = pd.DataFrame({
        "Check": ["RawImps", "OutputImps"],
        "Value": [raw_total, out_total]
    })

# ====================================================
# Prepare dates for Excel
# ====================================================
for tbl in [df, spend_franchise, imps_franchise, metric_union]:
    if not tbl.empty and "Week" in tbl.columns:
        tbl["Week"] = pd.to_datetime(tbl["Week"]).dt.date

# ====================================================
# Write Excel
# ====================================================
with pd.ExcelWriter(OUTPUT_MEDIA_FILE, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="Media_Processed", index=False)

    if not franchise_summary.empty:
        franchise_summary.to_excel(writer, sheet_name="Summary_Franchise", index=False)
    partner_tag_summary.to_excel(writer, sheet_name="Summary_PartnerTag", index=False)

    if not franchise_list.empty:
        franchise_list.to_excel(writer, sheet_name="Franchise_List", index=False)
    if not media_group_list.empty:
        media_group_list.to_excel(writer, sheet_name="MediaGroup_List", index=False)

    if not spend_franchise.empty:
        spend_franchise.to_excel(writer, sheet_name="Spend_Franchise_Weekly", index=False)
    if not imps_franchise.empty:
        imps_franchise.to_excel(writer, sheet_name="Imps_Franchise_Weekly", index=False)

    if not spend_weekly_totals.empty:
        spend_weekly_totals.to_excel(writer, sheet_name="Spend_Weekly_Totals", index=False)
    if not imps_weekly_totals.empty:
        imps_weekly_totals.to_excel(writer, sheet_name="Imps_Weekly_Totals", index=False)

    if not metric_union.empty:
        metric_union.to_excel(writer, sheet_name="Metric_Union_Output", index=False)

    if not qa_imps.empty:
        qa_imps.to_excel(writer, sheet_name="QA_Imps", index=False)

print("✅ Media pipeline complete")
print(f"   Output file: {OUTPUT_MEDIA_FILE}")
print(f"   Unique partners: {len(unique_partners)}")
print(f"   Rows in Partner/Partner_Brand summary: {len(partner_tag_summary)}")
if not metric_union.empty:
    print(f"   Rows in final Spend/Impressions union: {len(metric_union)}")

# ====================================================
# Export Final Media Impressions (Impressions only)
# ====================================================

FINAL_IMPS_FILE = Path("Final Media Impressions.xlsx")

if not metric_union.empty:
    # Filter out Spend, keep only Impressions
    final_imps = metric_union[metric_union["Metric"] == "Impressions"].copy()

    # Write to separate file
    with pd.ExcelWriter(FINAL_IMPS_FILE, engine="openpyxl") as writer:
        final_imps.to_excel(writer, sheet_name="Impressions", index=False)

    print(f"✅ Final Media Impressions file created: {FINAL_IMPS_FILE}")
    print(f"   Rows: {len(final_imps)}")
else:
    print("⚠️ No Impression data available to export.")