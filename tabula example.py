import tabula as tab
import pandas as pd
import logging
import sys
from datetime import datetime

from requests import get  # to make GET request


##########
## Functions
##########

def getArgs():
    '''
        Parameters:
        * n/a
        
        Return:
        list object: containing values retrieved from sys.argv
        None: on error
        
        Details:        
        * test length of sys.argv
        * retrieve commandline args
        * for source file path, will test if arg has an ending slash. If not, adds one
        * if argv count is "incorrect", will print out a message to remind user of args required
    '''
    args = []

    try:
        logging.info("***RETRIEVING COMMAND LINE ARGS")
        if len(sys.argv) == 4:
            sourceFilePath = sys.argv[1]
            if sourceFilePath[-1] != "/": 
                sourceFilePath += "/"            
            #END IF
            args.append(sourceFilePath)
            sourceFilename = sys.argv[2]
            args.append(sourceFilename)
            pages = sys.argv[3]
            args.append(pages)
            logging.debug("Commandline args used:\r\n {} \r\n {} \r\n {}".format(sourceFilePath,sourceFilename,pages))
            return args
        else:
            logging.error("Not enough arguments provided.")
            print(
                "Incorrect arguments provided\r\nPlease include path to source file, filename, and page range")
            return None
    except Exception as e:
        msg = str(e)
        logging.error("*****Error in getArgs. Args: {}  Error: {}".format(sys.argv, msg))
        return None
# END DEF

def generateOutputFilename(p_filename,p_extension):
    '''
        Parameters:
        * p_filename: a filename, possibly including pathing and extension, to start with as a base
        * p_extension: an extension string value for the target filename. This makes this proc a bit more generic
            to use across different apps. 
        
        Return:
        * string: the input filename modified
        * None: on error
        
        Details:
        * get current datetime
        * parse the filename param to extract only the filename without pathing and without extension
        * recreate filename as: <base filename> + "_" + <datetime as format YYYYMMDDHHMISS> + ".csv"
        
    '''
    try:
        logging.info("*****GENERATE FILE NAME")
        # strips the raw filename out of file string
        filename = p_filename.split(".")[0].split("/")[-1]
        current_datetime = datetime.strftime(
        datetime.now(), "%Y%m%d%H%M%S")
        output_filename = filename + "_" + current_datetime + "." + p_extension
        print("Output filename: %s" % (output_filename))
        return output_filename
    except Exception as e:
        msg = str(e)
        logging.error("*****Error in generateOutputFilename. Error: %s" % (msg))
        return None
# END DEF

def downloadFile(p_url, p_output_filename):
    
    try:
        logging.info("***RETRIEVING FILE FROM: {}".format(p_url))
        # open in binary mode
        with open(p_output_filename, "wb") as file:
            # get request
            response = get(p_url)
            response.raise_for_status()
            
            # write to file
            logging.debug("Writing file to {}".format(p_output_filename))
            file.write(response.content)
            return True
    except Exception as e:
        msg = str(e)
        logging.error("*****Error in generateOutputFilename. Error: %s" % (msg))
        return False
 
#END DEF
        
##########
## Main
##########
def main():

    try:
        args = getArgs()
        if args == None:
            logging.error("Unable to retrieve command line args - EXITING")
            return
        #END IF

        sourceFilePath = args[0]
        sourceFilename = args[1]
        pageRange = args[2]
        
        # get file from web site
        if not downloadFile(
                "https://www.questrade.com/docs/librariesprovider7/default-document-library/questrade_bonds_list.pdf",
                "questrade_bonds_list.pdf"
        ):
            logging.error("Unable to retrieve file from web - EXITING")
            return
        #END IF
                
        # Read pdf into list of DataFrame
        logging.info("Reading PDF")
        df = tab.read_pdf(sourceFilePath + sourceFilename, stream=True,pages=pageRange)

        df_combined = pd.DataFrame()

        logging.info("Concat all PDF tables")
        df_combined = pd.concat(df).drop_duplicates()

        logging.info("Set index")
        df_combined.index = df_combined["CUSIP"]

        logging.info("Filter columns")
        df_combined = df_combined.loc[:,["ISSUER","COUPON","MATURITY","PRICE","YIELD","DBRS","FEATURE"]]

        logging.info("Create new columns")

        df_combined = df_combined.assign(
                TOTAL_COST=(df_combined["PRICE"]/100)*5000,
                YEARS=(pd.to_datetime(df_combined["MATURITY"],format="%Y-%m-%d")
                        - pd.to_datetime(datetime.now(),format="%Y-%m-%d")).dt.total_seconds()
                        / (24*60*60*365.25),
                INTEREST_DOLLARS=df_combined["COUPON"] / 100 * 5000,
                AY=lambda x: (( (5000 - x["TOTAL_COST"]) / x["YEARS"] ) + x["INTEREST_DOLLARS"]) / x["TOTAL_COST"] * 100,
                MATURITY_YEAR=pd.to_datetime(df_combined["MATURITY"],format="%Y-%m-%d").dt.year
        )

        logging.info("Cleanup columns")
        df_combined.drop(df_combined[ df_combined["YEARS"] > 6.0 ].index,inplace=True)

        logging.info("Sort")
        df_combined.sort_values(by=["MATURITY_YEAR","AY"],ascending=False,inplace=True)

        logging.debug(df_combined)

        filename = generateOutputFilename("bond_candidates","csv")
        if filename == None:
            logging.error("Unable to a filename for output - EXITING")
            return
        #END IF
        
        logging.info("Write to CSV")
        df_combined.to_csv(filename, index=True, quoting=1)

    except Exception as e:
        msg = str(e)
        logging.error("*****Error in main(). Error: %s" % (msg))
        return


#END DEF

##########
## Globals
##########
g_LoggingLevel = logging.DEBUG

logging.basicConfig(level=g_LoggingLevel, format="%(levelname)s: %(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")

logging.info('*****PROGRAM START')

if __name__ == '__main__':

    main()

# end if main()

logging.info('*****PROGRAM END')