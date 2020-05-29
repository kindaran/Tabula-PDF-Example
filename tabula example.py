import tabula as tab
import pandas as pd
from datetime import datetime

def generateOutputFilename(p_filename):
        try:
                # strips the raw filename out of file string
                filename = p_filename.split(".")[0].split("\\")[-1]
                current_datetime = datetime.strftime(
                datetime.now(), "%Y%m%d%H%M%S")
                output_filename = filename + "_" + current_datetime + ".csv"
                print("Output filename: %s" % (output_filename))
                return output_filename
        except Exception as e:
                msg = str(e)
                print(
                "*****Error in generateOutputFilename. Error: %s" % (msg))
                return None
# END DEF

# Read pdf into list of DataFrame
print("Reading PDF")
df = tab.read_pdf(r"~/Downloads/questrade_bonds_list.pdf", stream=True,pages="15-31")

df_combined = pd.DataFrame()

print("Concat all PDF tables")
df_combined = pd.concat(df).drop_duplicates()

print("Set index")
df_combined.index = df_combined["CUSIP"]

print("Filter columns")
df_combined = df_combined.loc[:,["ISSUER","COUPON","MATURITY","PRICE","YIELD","DBRS","FEATURE"]]

print("Create new columns")

df_combined = df_combined.assign(
        TOTAL_COST=(df_combined["PRICE"]/100)*5000,
        YEARS=(pd.to_datetime(df_combined["MATURITY"],format="%Y-%m-%d") 
                - pd.to_datetime(datetime.now(),format="%Y-%m-%d")).dt.total_seconds() 
                / (24*60*60*365.25),
        INTEREST_DOLLARS=df_combined["COUPON"] / 100 * 5000,
        AY=lambda x: (( (5000 - x["TOTAL_COST"]) / x["YEARS"] ) + x["INTEREST_DOLLARS"]) / x["TOTAL_COST"] * 100,
        MATURITY_YEAR=pd.to_datetime(df_combined["MATURITY"],format="%Y-%m-%d").dt.year
)

print("Cleanup columns")
df_combined.drop(df_combined[ df_combined["YEARS"] > 6.0 ].index,inplace=True)

print("Sort")
df_combined.sort_values(by=["MATURITY_YEAR","AY"],ascending=False,inplace=True)

print(df_combined)

filename = generateOutputFilename("bond_candidates")

print("Write to CSV")
df_combined.to_csv(filename, index=True, quoting=1)