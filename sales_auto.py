
import pandas as pd
# Load the Excel file
df = pd.read_excel("Biolage Sales Data.xlsx", sheet_name="Raw Data")
# Display first 5 rows
print(df.head())