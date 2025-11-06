import pandas as pd

# Load CSV/Excel file
df = pd.read_csv(r"F:\CryptoData\Output\BlockWorks\BlockWorks_output_coins.csv")   # or pd.read_excel("your_input_file.xlsx")

# 1. Drop rows with empty Date
df = df.dropna(subset=["Time (datetime)"])

# 2. Drop rows with empty Coin Type
df = df.dropna(subset=["Coin_Type"])

# 2. Drop rows with empty Description
df = df.dropna(subset=["Full Description"])

# 3. Split multiple Coin Types into separate rows
df = df.assign(**{
    "Coin_Type": df["Coin_Type"].str.split(",")
}).explode("Coin_Type")

# 4. Clean up whitespace and drop blanks again
df["Coin_Type"] = df["Coin_Type"].str.strip()
df = df[df["Coin_Type"] != ""]

# Save output
df.to_csv(r"F:\CryptoData\Output\BlockWorks\BlockWorks_output_separated_coins.csv", index=False)
