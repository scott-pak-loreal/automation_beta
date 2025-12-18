import pandas as pd
import numpy as np
from pathlib import Path

# =============================
# Config
# =============================
INPUT_MEDIA_XLSX   = Path("Biolage Media Data.xlsx")
INPUT_MEDIA_SHEET  = "Consolidated Media Data"

OUTPUT_MEDIA_FILE  = Path("Biolage Media Processed.xlsx")   # QA workbook (keep)
FINAL_IMPS_FILE    = Path("Final Media Impressions.xlsx")   # modeling input (impressions only)

BRAND_FRANCHISE_NAME = "Biolage - Brand"

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
# Load media data
# =============================
df = pd.read_excel(INPUT_MEDIA_XLSX, sheet_name=INPUT_MEDIA_SHEET)
df.columns = [c.strip() for c in df.columns]

if "Week End (Sat)" in df.columns:
    df = df.rename(columns={"Week End (Sat)": "Week"})
if "Week" not in df.columns:
    raise KeyError("Expected 'Week' or 'Week End (Sat)' column in media file.")

df["Week"] = norm_week(df["Week"])

for col in ["Spend", "Impressions"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

if "Franchise" not in df.columns:
    raise KeyError("Expected 'Franchise' column in media file.")
if "Partner" not in df.columns:
    raise KeyError("Expected 'Partner' column in media file.")

def map_franchise(v: str) -> str:
    v = str(v)
    if "Blowdry Cream" in v:
        return "Styling"
    elif "Prime Day" in v:
        return BRAND_FRANCHISE_NAME
    return v

df["Franchise"] = norm_str(df["Franchise"].apply(map_franchise))
df["Partner"] = norm_str(df["Partner"])

df["Partner_tag"] = np.where(
    df["Franchise"].str.contains("- Brand", na=False),
    df["Partner"] + "_Brand",
    df["Partner"]
)

unique_partners = df["Partner"].dropna().drop_duplicates().tolist()

# =============================
# QA summaries (preserve)
# =============================
num_cols = [c for c in ["Spend", "Impressions"] if c in df.columns]

franchise_summary = (
    df.groupby("Franchise", as_index=False)[num_cols].sum().sort_values(num_cols[0], ascending=False)
    if num_cols else pd.DataFrame()
)

# Partner tag skeleton for QA
skeleton_rows = []
for p in unique_partners:
    skeleton_rows.append({"Partner": p, "Partner_tag": p})
    skeleton_rows.append({"Partner": p, "Partner_tag": f"{p}_Brand"})
skeleton = pd.DataFrame(skeleton_rows).drop_duplicates().reset_index(drop=True)

partner_tag_summary = (
    df.groupby(["Partner", "Partner_tag"], as_index=False)[num_cols].sum()
    if num_cols else pd.DataFrame()
)

if not partner_tag_summary.empty:
    partner_tag_summary = skeleton.merge(partner_tag_summary, on=["Partner", "Partner_tag"], how="left")
    for c in num_cols:
        partner_tag_summary[c] = partner_tag_summary[c].fillna(0)
    partner_tag_summary = partner_tag_summary.sort_values(["Partner", "Partner_tag"])
else:
    partner_tag_summary = skeleton.copy()
    for c in num_cols:
        partner_tag_summary[c] = 0

franchise_list = df["Franchise"].dropna().drop_duplicates().sort_values().to_frame(name="Franchise")
media_group_list = partner_tag_summary["Partner_tag"].dropna().drop_duplicates().sort_values().to_frame(name="Partner_tag")

# =============================
# Spend branch (keep QA tabs)
# =============================
spend_franchise = pd.DataFrame()
spend_weekly_totals = pd.DataFrame()

if "Spend" in df.columns:
    spend_franchise = (
        df.pivot_table(
            index=["Week", "Franchise"],
            columns="Partner_tag",
            values="Spend",
            aggfunc="sum",
            fill_value=0
        ).reset_index()
    )
    spend_partner_cols = [c for c in spend_franchise.columns if c not in ["Week", "Franchise"]]
    spend_weekly_totals = (
        spend_franchise.groupby("Week")[spend_partner_cols].sum().reset_index()
        .rename(columns={c: f"{c}_TotalSpend" for c in spend_partner_cols})
    )
    spend_franchise = spend_franchise.merge(spend_weekly_totals, on="Week", how="left")
    spend_franchise.insert(2, "Metric", "Spend")

# =============================
# Impressions branch (HALO + totals) (this is what model uses)
# =============================
imps_franchise = pd.DataFrame()
imps_weekly_totals = pd.DataFrame()

if "Impressions" in df.columns:
    imps_pivot = (
        df.pivot_table(
            index=["Week", "Franchise"],
            columns="Partner_tag",
            values="Impressions",
            aggfunc="sum"
        ).reset_index()
    )

    partner_tag_cols = [c for c in imps_pivot.columns if c not in ["Week", "Franchise"]]

    is_brand = imps_pivot["Franchise"] == BRAND_FRANCHISE_NAME
    brand_rows = imps_pivot[is_brand].drop(columns=["Franchise"])
    non_brand_rows = imps_pivot[~is_brand].copy()

    if not brand_rows.empty:
        brand_rows = brand_rows.set_index("Week")
        non_brand_rows = non_brand_rows.merge(
            brand_rows, left_on="Week", right_index=True, how="left", suffixes=("", "__brandrow")
        )
        for col in partner_tag_cols:
            bcol = f"{col}__brandrow"
            if bcol in non_brand_rows.columns:
                non_brand_rows[col] = np.where(non_brand_rows[col].isna(), non_brand_rows[bcol], non_brand_rows[col])
                non_brand_rows.drop(columns=[bcol], inplace=True)

    imps_franchise = non_brand_rows.copy()
    imps_franchise.insert(2, "Metric", "Impressions")

    # Weekly totals per partner: base + brand
    imps_week_partner = df.pivot_table(index="Week", columns="Partner_tag", values="Impressions", aggfunc="sum")

    imps_weekly_totals = pd.DataFrame(index=imps_week_partner.index)
    for p in unique_partners:
        base = imps_week_partner[p] if p in imps_week_partner.columns else 0
        brand = imps_week_partner[f"{p}_Brand"] if f"{p}_Brand" in imps_week_partner.columns else 0
        imps_weekly_totals[f"{p}TotalImps"] = pd.Series(base).fillna(0) + pd.Series(brand).fillna(0)

    imps_weekly_totals = imps_weekly_totals.reset_index()
    imps_franchise = imps_franchise.merge(imps_weekly_totals, on="Week", how="left")

# Union output for QA (keep)
metric_union = pd.DataFrame()
to_union = []
if not spend_franchise.empty:
    to_union.append(spend_franchise)
if not imps_franchise.empty:
    to_union.append(imps_franchise)
if to_union:
    metric_union = pd.concat(to_union, ignore_index=True, sort=False)

# Optional QA check for Impressions
qa_imps = pd.DataFrame()
if "Impressions" in df.columns and not imps_franchise.empty:
    raw_total = df["Impressions"].sum()
    out_total = imps_franchise[
        [c for c in imps_franchise.columns if c not in ["Week", "Franchise", "Metric"] and not c.endswith("TotalImps")]
    ].sum().sum()
    qa_imps = pd.DataFrame({"Check": ["RawImps", "OutputImps"], "Value": [raw_total, out_total]})

# Validate uniqueness of modeling panel (after halo)
if not imps_franchise.empty:
    assert_unique(imps_franchise, ["Week", "Franchise"], "imps_franchise")

# =============================
# Write QA workbook (unchanged intention)
# =============================
def date_for_excel(t):
    if t.empty:
        return t
    if "Week" in t.columns:
        t = t.copy()
        t["Week"] = pd.to_datetime(t["Week"]).dt.date
    return t

with pd.ExcelWriter(OUTPUT_MEDIA_FILE, engine="openpyxl") as writer:
    date_for_excel(df).to_excel(writer, sheet_name="Media_Processed", index=False)
    if not franchise_summary.empty:
        franchise_summary.to_excel(writer, sheet_name="Summary_Franchise", index=False)
    partner_tag_summary.to_excel(writer, sheet_name="Summary_PartnerTag", index=False)
    franchise_list.to_excel(writer, sheet_name="Franchise_List", index=False)
    media_group_list.to_excel(writer, sheet_name="MediaGroup_List", index=False)

    if not spend_franchise.empty:
        date_for_excel(spend_franchise).to_excel(writer, sheet_name="Spend_Franchise_Weekly", index=False)
    if not imps_franchise.empty:
        date_for_excel(imps_franchise).to_excel(writer, sheet_name="Imps_Franchise_Weekly", index=False)

    if not spend_weekly_totals.empty:
        date_for_excel(spend_weekly_totals).to_excel(writer, sheet_name="Spend_Weekly_Totals", index=False)
    if not imps_weekly_totals.empty:
        date_for_excel(imps_weekly_totals).to_excel(writer, sheet_name="Imps_Weekly_Totals", index=False)

    if not metric_union.empty:
        date_for_excel(metric_union).to_excel(writer, sheet_name="Metric_Union_Output", index=False)

    if not qa_imps.empty:
        qa_imps.to_excel(writer, sheet_name="QA_Imps", index=False)

# =============================
# Export Final Media Impressions (modeling input)
# =============================
if not imps_franchise.empty:
    final_imps = imps_franchise.drop(columns=["Metric"]).copy()

    # Numeric coercion + fill NA with 0 for modeling
    feat_cols = [c for c in final_imps.columns if c not in ["Week", "Franchise"]]
    for c in feat_cols:
        final_imps[c] = pd.to_numeric(final_imps[c], errors="coerce")
    final_imps[feat_cols] = final_imps[feat_cols].fillna(0)

    with pd.ExcelWriter(FINAL_IMPS_FILE, engine="openpyxl") as writer:
        final_imps.assign(Week=final_imps["Week"].dt.date).to_excel(writer, sheet_name="Impressions", index=False)

    print("✅ Media pipeline complete (QA preserved)")
    print(f"   QA workbook: {OUTPUT_MEDIA_FILE}")
    print(f"   Final imps:  {FINAL_IMPS_FILE}")
    print(f"   Rows: {len(final_imps)} | Cols: {final_imps.shape[1]}")
else:
    print("⚠️ No impressions data available to export.")

