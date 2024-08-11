"""
- Update the following datasets:
"""



# LIB
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
from typing import Optional


# EXCEPTION CLASS
class CouldNotFindEnvVar(ValueError):
    def __init__(self, message:Optional[str]=None):
        if message is None:
            message = "Could not find environment variable"


# VAR
# Load environment variables file
load_dotenv(dotenv_path='./url.env')

try:
    # url = os.getenv("GTFS_RT_URL")
    # gtfs_storage_path = os.getenv("GTFS_STORAGE_PATH")
    # clean_data_path = os.getenv("CLEAN_DATA_PATH")
except:
    error =  CouldNotFindEnvVar("Some environment variables could not be found")
    error.add_note = "Please make sure that the following environment variables are set: "", "", ""
    raise error




# FUNCTIONS
download_raw_datas(url:list[str], raw_storage_path:str):





if __name__ == "__main__":
    pass