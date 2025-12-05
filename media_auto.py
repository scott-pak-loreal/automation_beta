import pandas as pd
import numpy as np
from pathlib import Path

# =============================
# Config
# =============================
INPUT_MEDIA_XLSX   = Path("Biolage Media Data.xlsx")
INPUT_MEDIA_SHEET  = "Consolidated Media Data"  
OUTPUT_MEDIA_FILE  = Path("Biolage Media Processed.xlsx")

# =============================
# Load media data
# =============================
df = pd.read_excel(INPUT_MEDIA_XLSX, sheet_name=INPUT_MEDIA_SHEET)

# Strip whitespace from column names (safety)
df.columns = [c.strip() for c in df.columns]

# If your date column is "Week End (Sat)" rename it to "Week"
if "Week End (Sat)" in df.columns:
    df = df.rename(columns={"Week End (Sat)": "Week"})

# =============================
# Date prep (if Week column exists)
# =============================
if "Week" in df.columns:
    df["Week"] = pd.to_datetime(df["Week"], errors="coerce").dt.normalize()

# =============================
# Numeric hygiene
# =============================
for col in ["Spend", "Impressions"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# =============================
# 1) Franchise remapping
#    IF Contains("Blowdry Cream") -> "Styling"
#    ELSEIF Contains("Prime Day") -> "Biolage - Brand"
#    ELSE Franchise
# =============================
if "Franchise" not in df.columns:
    raise KeyError("Expected a 'Franchise' column; check your headers.")

def map_franchise(value: str) -> str:
    v = str(value)
    if "Blowdry Cream" in v:
        return "Styling"
    elif "Prime Day" in v:
        return "Biolage - Brand"
    else:
        return v

df["Franchise"] = df["Franchise"].apply(map_franchise)

# =============================
# 2) Partner_tag logic
#    IF Franchise contains "- Brand" -> Partner + "_Brand"
#    ELSE Partner
# =============================
if "Partner" not in df.columns:
    raise KeyError("Expected a 'Partner' column; check your headers.")

df["Partner_tag"] = np.where(
    df["Franchise"].astype(str).str.contains("- Brand"),
    df["Partner"].astype(str) + "_Brand",
    df["Partner"].astype(str)
)

# =============================
# 3) Build Partner / Partner_Brand skeleton
#    For every unique Partner, create:
#    (Partner, Partner) and (Partner, Partner_Brand)
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

# =============================
# 4) Summaries of Spend & Impressions
# =============================
num_cols = [c for c in ["Spend", "Impressions"] if c in df.columns]

# 4a) Summary by Franchise
if num_cols:
    franchise_summary = (
        df.groupby("Franchise", as_index=False)[num_cols]
          .sum()
          .sort_values(num_cols[0], ascending=False)
    )
else:
    franchise_summary = pd.DataFrame()

# 4b) Summary by Partner & Partner_tag
if num_cols:
    partner_tag_summary = (
        df.groupby(["Partner", "Partner_tag"], as_index=False)[num_cols]
          .sum()
    )
else:
    partner_tag_summary = pd.DataFrame()

# Ensure we have BOTH Partner and Partner_Brand for each Partner
if not partner_tag_summary.empty:
    partner_tag_summary = (
        skeleton
        .merge(partner_tag_summary, on=["Partner", "Partner_tag"], how="left")
    )
    for col in num_cols:
        partner_tag_summary[col] = partner_tag_summary[col].fillna(0)

    partner_tag_summary = partner_tag_summary.sort_values(["Partner", "Partner_tag"])
else:
    partner_tag_summary = skeleton.copy()
    for col in num_cols:
        partner_tag_summary[col] = 0

# =============================
# 5) Lists of Franchises & Media Groups
# =============================
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

# =============================
# 6) Write output Excel
# =============================
# Make Week pure date for Excel if present
if "Week" in df.columns:
    df["Week"] = pd.to_datetime(df["Week"]).dt.date

with pd.ExcelWriter(OUTPUT_MEDIA_FILE, engine="openpyxl") as writer:
    # Raw processed media rows
    df.to_excel(writer, sheet_name="Media_Processed", index=False)

    # Summaries
    if not franchise_summary.empty:
        franchise_summary.to_excel(writer, sheet_name="Summary_Franchise", index=False)
    partner_tag_summary.to_excel(writer, sheet_name="Summary_PartnerTag", index=False)

    # Lists
    if not franchise_list.empty:
        franchise_list.to_excel(writer, sheet_name="Franchise_List", index=False)
    if not media_group_list.empty:
        media_group_list.to_excel(writer, sheet_name="MediaGroup_List", index=False)

print("✅ Media pipeline complete:")
print(f"   Output file: {OUTPUT_MEDIA_FILE}")
print(f"   Unique partners: {len(unique_partners)}")
print(f"   Rows in Partner/Partner_Brand summary: {len(partner_tag_summary)}")
