import pandas as pd
from pathlib import Path

# =============================
# Config
# =============================
INPUT_MEDIA_XLSX   = Path("Biolage Media Data.xlsx")
INPUT_MEDIA_SHEET  = "Consolidated Media Data"   # adjust if needed
OUTPUT_MEDIA_FILE  = Path("Biolage Media Processed.xlsx")

# Which column is your "media group"?
MEDIA_GROUP_COL = "Partner_tag"   # we’ll create this below


# =============================
# Load media data
# =============================
df = pd.read_excel(INPUT_MEDIA_XLSX, sheet_name=INPUT_MEDIA_SHEET)

# Clean column names a bit
df.columns = [c.strip().replace(" ", "_") for c in df.columns]

# =============================
# Date prep (optional, if you have a week/date column)
# =============================
for possible_date in ["Week_End", "Week", "Date"]:
    if possible_date in df.columns:
        df[possible_date] = pd.to_datetime(df[possible_date], errors="coerce").dt.normalize()
        # standardize name to Week
        if possible_date != "Week":
            if "Week" in df.columns and possible_date != "Week":
                # avoid overwriting a different Week column accidentally
                pass
            else:
                df = df.rename(columns={possible_date: "Week"})
        break  # stop after first date column found

# =============================
# Numeric hygiene
# =============================
for col in ["Spend", "Impressions"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# =============================
# FORMULA TOOL #1 (Franchise remapping)
# -----------------------------
# IF Contains([Franchise], "Blowdry Cream") THEN "Styling"
# ELSEIF Contains([Franchise], "Prime Day") THEN "Biolage - Brand"
# ELSE [Franchise]
# ENDIF
# =============================

def map_franchise_jim(value: str) -> str:
    v = str(value)
    if "Blowdry Cream" in v:
        return "Styling"
    elif "Prime Day" in v:
        return "Biolage - Brand"
    else:
        return v

df["Franchise"] = df["Franchise"].apply(map_franchise_jim)

# =============================
# FORMULA TOOL #2 (Concatfranchise)
# -----------------------------
# Concatfranchise = [partner] + " - " + [Franchise]
# =============================
df["Concatfranchise"] = (
    df["partner"].astype(str) + " - " + df["Franchise"].astype(str)
)

# =============================
# FORMULA TOOL #3 (Partner_tag)
# -----------------------------
# IF Contains([concatfranchise], "- Brand") THEN
#     [partner] + "_Brand"
# ELSE
#     [partner]
# ENDIF
# =============================
def map_partner_tag(row):
    concat_val  = str(row["Concatfranchise"])
    partner_val = str(row["partner"])
    if "- Brand" in concat_val:
        return partner_val + "_Brand"
    else:
        return partner_val

df["Partner_tag"] = df.apply(map_partner_tag, axis=1)

# =============================
# Summaries of Spend & Impressions
# =============================
num_cols = [c for c in ["Spend", "Impressions"] if c in df.columns]

# 1) Summary by Franchise
if num_cols:
    franchise_summary = (
        df.groupby("Franchise", as_index=False)[num_cols]
          .sum()
          .sort_values(num_cols[0], ascending=False)  # sort by Spend if present
    )
else:
    franchise_summary = pd.DataFrame()

# 2) Summary by Media Group (Partner_tag)
if MEDIA_GROUP_COL in df.columns and num_cols:
    media_group_summary = (
        df.groupby(MEDIA_GROUP_COL, as_index=False)[num_cols]
          .sum()
          .sort_values(num_cols[0], ascending=False)
    )
else:
    media_group_summary = pd.DataFrame()

# =============================
# Lists of Franchises & Media Groups
# =============================
franchise_list = (
    df["Franchise"]
    .dropna()
    .drop_duplicates()
    .sort_values()
    .to_frame(name="Franchise")
)

if MEDIA_GROUP_COL in df.columns:
    media_group_list = (
        df[MEDIA_GROUP_COL]
        .dropna()
        .drop_duplicates()
        .sort_values()
        .to_frame(name=MEDIA_GROUP_COL)
    )
else:
    media_group_list = pd.DataFrame()

# =============================
# Write output Excel
# =============================
with pd.ExcelWriter(OUTPUT_MEDIA_FILE, engine="openpyxl") as writer:
    # Raw processed media rows
    df.to_excel(writer, sheet_name="Media_Processed", index=False)

    # Summaries
    if not franchise_summary.empty:
        franchise_summary.to_excel(writer, sheet_name="Summary_Franchise", index=False)
    if not media_group_summary.empty:
        media_group_summary.to_excel(writer, sheet_name="Summary_MediaGroup", index=False)

    # Lists
    if not franchise_list.empty:
        franchise_list.to_excel(writer, sheet_name="Franchise_List", index=False)
    if not media_group_list.empty:
        media_group_list.to_excel(writer, sheet_name="MediaGroup_List", index=False)

print("✅ Media pipeline complete:")
print(f"   Output file: {OUTPUT_MEDIA_FILE}")
if not franchise_summary.empty:
    print(f"   Franchises summarized: {len(franchise_summary)}")
if not media_group_summary.empty:
    print(f"   Media groups summarized: {len(media_group_summary)}")
