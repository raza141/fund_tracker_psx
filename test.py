from scrapper import execution as ex 
import os
from pathlib import Path 
import pandas as pd, numpy as np, datetime 
import matplotlib.pyplot as plt, seaborn as sns 
import plotly.graph_objects as go
from utils.fipi_lipi_processor import FIPIDataProcessor
from config.config import (FIPI_DIR, LIPI_DIR, PROCESSED_FIPI_LIPI_DIR, STOCKS_DIR, RAW_STOCKS_DIR, QUOTATION_DATA_DIR)
from utils.fipi_lipi_preprocessor_v2 import FIPIDataProcessor
from utils.db_connection_v2 import DatabaseConnection
from config.db_config import db_config
from datetime import date, datetime
from datetime import timedelta



from scrapper.quotationextraction import PDFExtraction as qe
from scrapper.scrapper import Scrapper as sp
from scrapper.datareader import DataReader as dr


import pymysql


sns.set_style('darkgrid')
plt.rcParams['figure.figsize'] = (18, 6)
plt.rcParams['font.size'] = 14
plt.rcParams['figure.dpi'] = 300
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)


# %matplotlib inline
# %reload_ext autoreload
# %autoreload 2

previous_date = datetime.today() - timedelta(days=1)

class_df = ex.Execution(previous_date).execution_by_extension()

if __name__ == '__main__':
    print(class_df)