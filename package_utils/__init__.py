from package_utils.spark_session import spark
from package_utils.DataHandler import DataHandler
from package_utils.utils import get_incremental_list_of_dates

# only requires the data_handler
__all__ = ['data_handler', 'get_incremental_list_of_dates']

data_handler = DataHandler()