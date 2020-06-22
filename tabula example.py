import logging
import sys
from datetime import datetime

import pandas as pd
import tabula as tab
import json 

##########
## Functions
##########

def getArgs():
    """
        * current args: config filepath, config filename
        * test length of sys.argv
        * if argv count is "incorrect", will print out a message to remind user of args required    
        * retrieve commandline args
        * for filepath, will test if arg has an ending slash. If not, adds one

    Returns:
        list object: containing values retrieved from sys.argv
        None: on handled error
    """

    args = []

    try:
        logging.info("***RETRIEVING COMMAND LINE ARGS")
        if len(sys.argv) == 3:
            configFilePath = sys.argv[1]
            if configFilePath[-1] != "/": 
                configFilePath += "/"            
            #END IF
            args.append(configFilePath)
            
            configFilename = sys.argv[2]
            args.append(configFilename)
            
            logging.debug("Commandline args used:\r\n {} \r\n {}".format(configFilePath,configFilename))
            return args
        else:
            logging.error("Not enough arguments provided.")
            print(
                "Incorrect arguments provided\r\nPlease include path to config file and filename")
            return None
    except Exception as e:
        msg = str(e)
        logging.error("*****Error in getArgs. Error: {}".format(msg))
        return None
# END DEF

def generateOutputFilename(p_filename,p_extension):
    """
        * get current datetime
        * parse the filename param to extract only the filename without pathing and without extension
        * recreate filename as: <base filename> + "_" + <datetime as format YYYYMMDDHHMISS> + ".csv"    

    Args:
        p_filename (string): a filename, possibly including pathing and extension, to start with as a base
        p_extension (string): an extension string value for the target filename. This makes this proc a bit more generic
            to use across different apps. 

    Returns:
        string: the input filename modified
        None: on handled error
    """

    try:
        logging.info("*****GENERATE FILE NAME")
        # strips the raw filename out of file string
        filename = p_filename.split(".")[0].split("/")[-1]
        current_datetime = datetime.strftime(
        datetime.now(), "%Y%m%d%H%M%S")
        output_filename = filename + "_" + current_datetime + "." + p_extension
        logging.debug("Output filename: %s" % (output_filename))
        return output_filename
    except Exception as e:
        msg = str(e)
        logging.error("*****Error in generateOutputFilename. Error: {}".format(msg))
        return None
# END DEF
        
def loadConfigFile(p_path):
    """
    Given a path/filename, loads a JSON format file and returns file content

    Args:
        p_path (string): path/filename to a json file

    Returns:
        dictionary: returns json file content in dictionary structure
        None: on handled error
    """
    data = None

    try:
        logging.info("*****LOAD CONFIG FILE")
        with open(p_path, 'r') as read_file:
            data = json.load(read_file)
        # END WITH

        logging.debug("Config file content is:\r\n %s" %(json.dumps(data)))
        return data
    except Exception as e:
        msg = str(e)
        logging.error("*****Error in loadConfigFile. Error: {}".format(msg))
        return None

# END DEF
        
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

        configFilePath = args[0] + args[1]
        
        # load JSON config file
        configFile = loadConfigFile(configFilePath)
        if configFile == None:
            logging.info("Unable to retrieve config file - ending")
            return
        # END IF

        # Read pdf into list of DataFrame
        logging.info("***READING PDF")
        df = tab.read_pdf(configFile["url"], stream=True,pages=configFile["page_range"])

        df_combined = pd.DataFrame()

        logging.info("***CONCAT ALL PDF TABLES")
        df_combined = pd.concat(df).drop_duplicates()

        logging.info("***SET INDEX")
        df_combined.index = df_combined[configFile["index_column"]]

        logging.info("***FILTER COLUMNS")
        df_combined = df_combined.loc[:,configFile["keep_columns"]]

        logging.info("***CREATE NEW COLUMNS")

        df_combined = df_combined.assign(
                TOTAL_COST=(df_combined["PRICE"]/100)*5000,
                YEARS=(pd.to_datetime(df_combined["MATURITY"],format="%Y-%m-%d")
                        - pd.to_datetime(datetime.now(),format="%Y-%m-%d")).dt.total_seconds()
                        / (24*60*60*365.25),
                INTEREST_DOLLARS=df_combined["COUPON"] / 100 * 5000,
                AY=lambda x: (( (5000 - x["TOTAL_COST"]) / x["YEARS"] ) + x["INTEREST_DOLLARS"]) / x["TOTAL_COST"] * 100,
                MATURITY_YEAR=pd.to_datetime(df_combined["MATURITY"],format="%Y-%m-%d").dt.year
        )

        logging.info("***CLEANUP COLUMNS")
        df_combined.drop(df_combined[ df_combined["YEARS"] > 6.0 ].index,inplace=True)

        logging.info("***SORT")
        df_combined.sort_values(by=["MATURITY_YEAR","AY"],ascending=False,inplace=True)

        logging.debug(df_combined)

        filename = generateOutputFilename(configFile["csv_filename"],"csv")
        if filename == None:
            logging.error("Unable to a filename for output - EXITING")
            return
        #END IF
        
        logging.info("WRITE TO CSV")
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
