# 03_build_lasso_input.py
import pandas as pd
from pathlib import Path

# =============================
# Config
# =============================
SALES_ANALYTICAL_XLSX = Path("Analytical Table.xlsx")
SALES_AVGPRICE_SHEET  = "Avg_Price"

PROPHET_OUT_XLSX      = Path("Biolage Prophet Output.xlsx")
PROPHET_SHEET         = "Prophet_Output"

MEDIA_IMPS_XLSX       = Path("Final Media Impressions.xlsx")
MEDIA_SHEET           = "Impressions"

# ---- National Promo (optional, future-proof) ----
PROMO_XLSX            = Path("National_Promo_Calendar.xlsx")
PROMO_SHEET           = "Promos"

OUTPUT_LASSO_XLSX     = Path("Biolage_Lasso_Input.xlsx")

FILL_MEDIA_NA_WITH_ZERO = True

# =============================
# Helpers
# =============================
def norm_week(s):
    return pd.to_datetime(s, errors="coerce").dt.normalize()

def norm_franchise(s):
    return s.astype(str).str.strip()

def assert_unique(df, keys, name):
    dup = df.duplicated(subset=keys).sum()
    if dup:
        raise ValueError(f"{name} has {dup} duplicate rows on {keys}")

# =============================
# Load Sales Avg Price (Week × Franchise)
# =============================
avg_price = pd.read_excel(SALES_ANALYTICAL_XLSX, sheet_name=SALES_AVGPRICE_SHEET)

req_cols = {"Week", "Franchise", "Sum_Units", "Average_Price"}
missing = req_cols - set(avg_price.columns)
if missing:
    raise KeyError(f"Avg_Price missing columns: {missing}")

sales_base = (
    avg_price[["Week", "Franchise", "Sum_Units", "Average_Price"]]
    .rename(columns={"Sum_Units": "Units"})
    .copy()
)

sales_base["Week"] = norm_week(sales_base["Week"])
sales_base["Franchise"] = norm_franchise(sales_base["Franchise"])
assert_unique(sales_base, ["Week", "Franchise"], "sales_base")

# =============================
# Load Prophet yhat (Week × Franchise)
# =============================
prophet = pd.read_excel(PROPHET_OUT_XLSX, sheet_name=PROPHET_SHEET)

req_cols = {"ID", "ds", "yhat"}
missing = req_cols - set(prophet.columns)
if missing:
    raise KeyError(f"Prophet_Output missing columns: {missing}")

prophet_keep = (
    prophet.rename(columns={"ID": "Franchise", "ds": "Week"})[["Week", "Franchise", "yhat"]]
    .copy()
)

prophet_keep["Week"] = norm_week(prophet_keep["Week"])
prophet_keep["Franchise"] = norm_franchise(prophet_keep["Franchise"])
assert_unique(prophet_keep, ["Week", "Franchise"], "prophet_keep")

sales_base = sales_base.merge(
    prophet_keep,
    on=["Week", "Franchise"],
    how="left",
    validate="one_to_one"
)

# =============================
# Load Media Impressions (Week × Franchise)
# =============================
media = pd.read_excel(MEDIA_IMPS_XLSX, sheet_name=MEDIA_SHEET)

if not {"Week", "Franchise"}.issubset(media.columns):
    raise KeyError("Final Media Impressions must include 'Week' and 'Franchise'.")

media["Week"] = norm_week(media["Week"])
media["Franchise"] = norm_franchise(media["Franchise"])
assert_unique(media, ["Week", "Franchise"], "media_imps")

media_feat_cols = [c for c in media.columns if c not in ["Week", "Franchise"]]
for c in media_feat_cols:
    media[c] = pd.to_numeric(media[c], errors="coerce")

# =============================
# Merge Sales + Media
# =============================
model_input = sales_base.merge(
    media,
    on=["Week", "Franchise"],
    how="left",
    validate="one_to_one"
)

if FILL_MEDIA_NA_WITH_ZERO and media_feat_cols:
    model_input[media_feat_cols] = model_input[media_feat_cols].fillna(0)

# =============================
# HALO FEATURES (Alteryx-equivalent)
# Halo = TotalImps - (Base + Brand)
# =============================
total_cols = [c for c in model_input.columns if c.endswith("TotalImps")]
halo_cols = []

for total_col in total_cols:
    partner = total_col.replace("TotalImps", "")
    base_col = partner
    brand_col = f"{partner}_Brand"
    halo_col = f"{partner}_Halo"

    if base_col not in model_input.columns:
        model_input[base_col] = 0
    if brand_col not in model_input.columns:
        model_input[brand_col] = 0

    for c in [total_col, base_col, brand_col]:
        model_input[c] = pd.to_numeric(model_input[c], errors="coerce").fillna(0)

    model_input[halo_col] = model_input[total_col] - (model_input[base_col] + model_input[brand_col])
    halo_cols.append(halo_col)

if halo_cols:
    model_input["Total_Halo"] = model_input[halo_cols].sum(axis=1)

# =============================
# NATIONAL PROMO (optional, future-ready)
# =============================
if PROMO_XLSX.exists():
    promo = pd.read_excel(PROMO_XLSX, sheet_name=PROMO_SHEET)

    if "Week" not in promo.columns:
        raise KeyError("Promo file must include a 'Week' column.")

    promo["Week"] = norm_week(promo["Week"])

    # Require Promo_Flag, default to 1/0
    if "Promo_Flag" not in promo.columns:
        raise KeyError("Promo file must include 'Promo_Flag' (1 = promo, 0 = no promo).")

    promo["Promo_Flag"] = pd.to_numeric(promo["Promo_Flag"], errors="coerce").fillna(0)

    # Keep only numeric promo features
    promo_feat_cols = ["Promo_Flag"] + [
        c for c in promo.columns
        if c not in ["Week", "Promo_Flag"] and pd.api.types.is_numeric_dtype(promo[c])
    ]

    promo = promo[["Week"] + promo_feat_cols].drop_duplicates(subset=["Week"])

    model_input = model_input.merge(
        promo,
        on="Week",
        how="left",
        validate="many_to_one"
    )

    model_input[promo_feat_cols] = model_input[promo_feat_cols].fillna(0)

else:
    # Promo file not present → create neutral control
    model_input["Promo_Flag"] = 0

# =============================
# Column order
# =============================
base_cols = ["Week", "Franchise", "Units", "yhat", "Average_Price", "Promo_Flag"]
other_cols = [c for c in model_input.columns if c not in base_cols]
model_input = model_input[base_cols + sorted(other_cols)]

# =============================
# Write output (QA files untouched)
# =============================
with pd.ExcelWriter(OUTPUT_LASSO_XLSX, engine="openpyxl") as writer:
    model_input.assign(Week=model_input["Week"].dt.date).to_excel(
        writer,
        sheet_name="Lasso_Input",
        index=False
    )

print("✅ Lasso input created (QA files untouched):", OUTPUT_LASSO_XLSX)
print("Rows:", len(model_input), "| Cols:", model_input.shape[1])
print("Promo file used:" , PROMO_XLSX.exists())
print("Halo columns:", len(halo_cols))

