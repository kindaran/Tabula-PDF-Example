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
df = tab.read_pdf(r"D:\Downloads\questrade_bonds_list.pdf", stream=True,pages="14-29")

df_combined = pd.DataFrame()

df_combined = pd.concat(df).drop_duplicates()

df_combined.index = df_combined["CUSIP"]

df_combined = df_combined.loc[:,["ISSUER","COUPON","MATURITY","PRICE","YIELD","DBRS","FEATURE"]]

df_combined = df_combined.assign(
        TOTAL_COST=(df_combined["PRICE"]/100)*5000,
        YEARS=(pd.to_datetime(df_combined["MATURITY"],format="%Y-%m-%d") 
                - pd.to_datetime(datetime.now(),format="%Y-%m-%d")).dt.total_seconds() 
                / (24*60*60*365.25),
        INTEREST_DOLLARS=df_combined["COUPON"] / 100 * 5000,
        AY=lambda x: (( (5000 - x["TOTAL_COST"]) / x["YEARS"] ) + x["INTEREST_DOLLARS"]) / x["TOTAL_COST"] * 100,
        MATURITY_YEAR=pd.to_datetime(df_combined["MATURITY"],format="%Y-%m-%d").dt.year
)

df_combined.drop(df_combined[ df_combined["YEARS"] > 6.0 ].index,inplace=True)

df_combined.sort_values(by=["MATURITY_YEAR","AY"],ascending=False,inplace=True)

print(df_combined)

filename = generateOutputFilename("bond_candidates")

df_combined.to_csv(filename, index=False, quoting=1)