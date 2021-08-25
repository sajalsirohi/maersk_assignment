from package_utils.spark_session import spark
from pyspark.sql.functions import monotonically_increasing_id, row_number, udf
from pyspark.sql import Window

import datetime


def get_incremental_list_of_dates(start_date, interval, total_size, convert_to_df = True):
    """
    Return the list incremental value, after interval of input
    :return:
    """
    result = []
    print(f"interval : {interval}, total_size : {total_size}")
    current_date_temp = datetime.datetime.strptime(start_date, "%d/%m/%Y")

    for itr in range(int(total_size / interval)):
        print(f"Assigning value : {itr}")
        newdate = current_date_temp + datetime.timedelta(days=itr)
        result += [newdate] * interval
    if convert_to_df:
        result = spark.createDataFrame([(val,) for val in result], ['vaccination_slot_day'])
        result = result.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
    return result


def convert_to_date(val):
    """
    convert to the date
    :param val:
    :return:
    """
    val = val - 1
    current_date_temp = datetime.datetime.strptime("25/08/2021", "%d/%m/%Y")
    newdate = current_date_temp + datetime.timedelta(days = val)
    return newdate.strftime("%d/%m/%Y")


convert_to_date_udf = udf(lambda x: convert_to_date(x))