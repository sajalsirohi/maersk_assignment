# Main driver script
from package_utils import \
    data_handler,\
    get_incremental_list_of_dates,\
    convert_to_date_udf

from pyspark.sql.functions import\
    monotonically_increasing_id,\
    row_number,\
    col,\
    lit, \
    unix_timestamp,\
    from_unixtime

from pyspark.sql import Window
from pyspark.sql.types import StringType

import datetime


def main():
    """
    Driver function
    :return:
    """
    data_handler.read_dataframe_from_path().show()
    # this is the emoployee df, which contains 50000 rows
    employee_df = data_handler.multiply_data_by(100, using = 'pandas')
    # just for the sake of code readabality
    data_handler.sampleEmployee = employee_df
    # create the CityEmployeeDensity using spark - sql,
    # with column population, city_name (to distinguish from the column city, and sequence
    data_handler.CityEmployeeDensity = \
        data_handler.execute_query("select *, RANK() OVER(ORDER BY population DESC) sequence from "
                                   "(select city as city_name, count(*) as population "
                                   "from sampleEmployee "
                                   "group by city order by population desc)")
    # Eg Output
    """
    +-------------+----------+--------+
    |    city_name|population|sequence|
    +-------------+----------+--------+
    |     New York|      1400|       1|
    | Philadelphia|       800|       2|
    |      Chicago|       700|       3|
    |        Miami|       600|       4|
    |      Phoenix|       500|       5|
    +-------------+----------+--------+
    """
    data_handler.CityEmployeeDensity.show()
    # join the two dataframes, and "VaccinationDrivePlan"  will be complete
    data_handler.VaccinationDrivePlan = data_handler.sampleEmployee.join(
        data_handler.CityEmployeeDensity,
        data_handler.sampleEmployee.city == data_handler.CityEmployeeDensity.city_name,
        "inner"
    ).orderBy("sequence")
    # Example Out put :
    """
    +----------+---------+--------------------+--------------------+--------+--------+-----+-----+------------+------------+--------------------+--------------------+---------+----------+--------+
    |first_name|last_name|        company_name|             address|    city|  county|state|  zip|      phone1|      phone2|               email|                 web|city_name|population|sequence|
    +----------+---------+--------------------+--------------------+--------+--------+-----+-----+------------+------------+--------------------+--------------------+---------+----------+--------+
    |     Cyril| Daufeldt|Galaxy Internatio...|         3 Lawton St|New York|New York|   NY|10013|212-745-8484|212-422-5427|cyril_daufeldt@da...|http://www.galaxy...| New York|      1400|       1|
    |      Jess| Chaffins|New York Public L...|          18 3rd Ave|New York|New York|   NY|10016|212-510-4633|212-428-9538|jess.chaffins@cha...|http://www.newyor...| New York|      1400|       1|
    |      Jess| Chaffins|New York Public L...|          18 3rd Ave|New York|New York|   NY|10016|212-510-4633|212-428-9538|jess.chaffins@cha...|http://www.newyor...| New York|      1400|       1|
    |     Cyril| Daufeldt|Galaxy Internatio...|         3 Lawton St|New York|New York|   NY|10013|212-745-8484|212-422-5427|cyril_daufeldt@da...|http://www.galaxy...| New York|      1400|       1|
    |    Derick|   Dhamer|Studer, Eugene A Esq|    87163 N Main Ave|New York|New York|   NY|10013|212-304-4515|212-225-9676|     ddhamer@cox.net|http://www.studer...| New York|      1400|       1|
    +----------+---------+--------------------+--------------------+--------+--------+-----+-----+------------+------------+--------------------+--------------------+---------+----------+--------+
    """
    # maintain the original df, to also process the parallel thing
    parallel_vaccination_plan = data_handler.VaccinationDrivePlan
    # Now we need to merge the incremental sequence with the data that we have
    emp_planned_per_day = 100
    vaccination_start_date = "25/08/2021"
    incremental_df = get_incremental_list_of_dates(
        vaccination_start_date,
        emp_planned_per_day,
        data_handler.VaccinationDrivePlan.count())
    # now we need to add this incremental_df to our main df
    data_handler.VaccinationDrivePlan = data_handler.VaccinationDrivePlan\
        .withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))

    # now this will add column ' vaccination_slot_day ', which will contain the slot for the employeed
    data_handler.VaccinationDrivePlan = data_handler.VaccinationDrivePlan\
        .join(incremental_df, data_handler.VaccinationDrivePlan.row_idx == incremental_df.row_idx). \
        drop("row_idx")
    # output
    """
    +----------+---------+--------------------+--------------------+--------+--------+-----+-----+------------+------------+--------------------+--------------------+---------+----------+--------+--------------------+
    |first_name|last_name|        company_name|             address|    city|  county|state|  zip|      phone1|      phone2|               email|                 web|city_name|population|sequence|vaccination_slot_day|
    +----------+---------+--------------------+--------------------+--------+--------+-----+-----+------------+------------+--------------------+--------------------+---------+----------+--------+--------------------+
    |     Tawna|   Buvens|H H H Enterprises...|3305 Nabell Ave #679|New York|New York|   NY|10009|212-674-9610|212-462-9157|     tawna@gmail.com|http://www.hhhent...| New York|      1400|       1| 2021-08-25 00:00:00|
    |      Jess| Chaffins|New York Public L...|          18 3rd Ave|New York|New York|   NY|10016|212-510-4633|212-428-9538|jess.chaffins@cha...|http://www.newyor...| New York|      1400|       1| 2021-08-25 00:00:00|
    |    Haydee| Denooyer|Cleaning Station Inc|        25346 New Rd|New York|New York|   NY|10016|212-792-8658|212-782-3493|hdenooyer@denooye...|http://www.cleani...| New York|      1400|       1| 2021-08-25 00:00:00|
    |   Alishia|    Sergi|Milford Enterpris...|2742 Distribution...|New York|New York|   NY|10025|212-860-1579|212-753-2740|    asergi@gmail.com|http://www.milfor...| New York|      1400|       1| 2021-08-25 00:00:00|
    +----------+---------+--------------------+--------------------+--------+--------+-----+-----+------------+------------+--------------------+--------------------+---------+----------+--------+--------------------+
    """
    data_handler.VaccinationDrivePlan.createOrReplaceTempView("VaccinationDrivePlan")
    # get the date for max date for city, and the min, and then join on city name
    city_date_data = data_handler.execute_query("select city, MIN(vaccination_slot_day),"
                                                " MAX(vaccination_slot_day),"
                                                " MAX(vaccination_slot_day) - MIN(vaccination_slot_day) as time_taken"
                                                " from VaccinationDrivePlan group by city")
    # report, when we serially execute the vaccination program.
    city_date_data.show()
    """
    +-------------+-------------------------+-------------------------+----------+
    |         city|min(vaccination_slot_day)|max(vaccination_slot_day)|time_taken|
    +-------------+-------------------------+-------------------------+----------+
    |     New York|      2021-08-25 00:00:00|      2021-09-07 00:00:00| 312 hours|
    | Philadelphia|      2021-09-08 00:00:00|      2021-09-15 00:00:00| 168 hours|
    |      Chicago|      2021-09-16 00:00:00|      2021-09-22 00:00:00| 144 hours|
    |        Miami|      2021-09-23 00:00:00|      2021-09-28 00:00:00| 120 hours|
    |      Phoenix|      2021-09-29 00:00:00|      2021-10-03 00:00:00|  96 hours|
    +-------------+-------------------------+-------------------------+---------+
    """
    # convert the interval schema to string
    data_df = city_date_data.withColumn("time_taken", city_date_data["time_taken"].cast(StringType()))
    data_df.toPandas().to_csv("C:\\Users\\sanjeev\\PycharmProjects\\maersk_assign\\city_data.csv")

    ##############################################################
    ## Parallel vaccination plan
    ##############################################################

    print("creating the view")
    parallel_vaccination_plan.createOrReplaceTempView("parallel_vaccination_plan")
    # modify the data. on grouping by city, and incrementing data on gap of 100 rows.
    parallel_vaccination_plan = data_handler.execute_query(f"""
                select *, CAST(((row_num - ((row_num - 1) % {emp_planned_per_day})) / {emp_planned_per_day}) as Int) + 1 as itr from 
                (select *, row_number() over(partition by city order by population desc) as row_num from parallel_vaccination_plan) 
                """).drop("row_num")
    print("addding the date")
    # create the vaccination_slot_day column
    parallel_vaccination_plan = parallel_vaccination_plan.withColumn(
        'vaccination_slot_day', convert_to_date_udf(col("itr"))
    ).drop("itr")
    parallel_vaccination_plan.show(10, truncate=False)
    """
    +----------+---------+-----------------------+---------------+---------+--------------------+-----+-----+------------+------------+---------------------+------------------------------------+---------+----------+--------+--------------------+
    |first_name|last_name|company_name           |address        |city     |county              |state|zip  |phone1      |phone2      |email                |web                                 |city_name|population|sequence|vaccination_slot_day|
    +----------+---------+-----------------------+---------------+---------+--------------------+-----+-----+------------+------------+---------------------+------------------------------------+---------+----------+--------+--------------------+
    |Erick     |Ferencz  |Cindy Turner Associates|20 S Babcock St|Fairbanks|Fairbanks North Star|AK   |99712|907-741-1044|907-227-6777|erick.ferencz@aol.com|http://www.cindyturnerassociates.com|Fairbanks|200       |35      |25/08/2021          |
    |Erick     |Ferencz  |Cindy Turner Associates|20 S Babcock St|Fairbanks|Fairbanks North Star|AK   |99712|907-741-1044|907-227-6777|erick.ferencz@aol.com|http://www.cindyturnerassociates.com|Fairbanks|200       |35      |25/08/2021          |
    |Roxane    |Campain  |Rapid Trading Intl     |1048 Main St   |Fairbanks|Fairbanks North Star|AK   |99708|907-231-4722|907-335-6568|roxane@hotmail.com   |http://www.rapidtradingintl.com     |Fairbanks|200       |35      |25/08/2021          |
    |Roxane    |Campain  |Rapid Trading Intl     |1048 Main St   |Fairbanks|Fairbanks North Star|AK   |99708|907-231-4722|907-335-6568|roxane@hotmail.com   |http://www.rapidtradingintl.com     |Fairbanks|200       |35      |25/08/2021          |
    |Roxane    |Campain  |Rapid Trading Intl     |1048 Main St   |Fairbanks|Fairbanks North Star|AK   |99708|907-231-4722|907-335-6568|roxane@hotmail.com   |http://www.rapidtradingintl.com     |Fairbanks|200       |35      |25/08/2021          |
    +----------+---------+-----------------------+---------------+---------+--------------------+-----+-----+------------+------------+---------------------+------------------------------------+---------+----------+--------+--------------------+
    
    """
    # converting the string column to timestamp column in the query
    parallel_vaccination_plan = parallel_vaccination_plan.withColumn("vaccination_slot_day",
                                                                     from_unixtime(
                                                                         unix_timestamp('vaccination_slot_day', 'dd/MM/yyyy')).alias(
                                                                         'vaccination_slot_day'
                                                                     )
                                                                     )
    parallel_vaccination_plan.show(30)
    # refresh the view
    parallel_vaccination_plan.createOrReplaceTempView("parallel_vaccination_plan")

    # calculate the max and minimum date for the city
    parallel_vaccination_plan_rpt_df = data_handler.execute_query("select city,"
                                                                  " MIN(vaccination_slot_day),"
                                                                  " MAX(vaccination_slot_day)"
                                                                  " from parallel_vaccination_plan group by city")
    parallel_vaccination_plan_rpt_df.show()
    data_df = parallel_vaccination_plan_rpt_df.withColumn("time_taken",
                                                          parallel_vaccination_plan_rpt_df["time_taken"].cast(StringType()))
    data_df.toPandas().to_csv("C:\\Users\\sanjeev\\PycharmProjects\\maersk_assign\\parallel_city_data.csv")


if __name__ == '__main__':
    main()
