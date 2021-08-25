import pyspark

from package_utils.spark_session import spark
from pyspark.sql.functions import rand

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None


class DataHandler:
    """
    Handle the data. Reading the file as dataframe, create more data out of it, etc, all
    services are defined here
    """
    def __init__(self):
        # create an empty dataframe.
        self.sampleEmployee = None
        self.CityEmployeeDensity = None
        self.VaccinationDrivePlan = None

    def read_dataframe_from_path(self, path = None) -> pyspark.sql.dataframe.DataFrame:
        """
        Datafrrame from the file path defined
        :param path: Only allowed one file as of now
        :return: spark dataframe
        """
        # Determine the path to the data
        path = path or "C:\\Users\\sanjeev\\Downloads\\us-500.csv"
        print(f"Reading from the path : {path}")

        # if there are no ".csv" added to the path, add it
        if not path.endswith(".csv"):
            path = path + ".csv"

        # read the data into the dataframe and return the data
        self.sampleEmployee = spark.read\
            .option("header", True)\
            .csv(path)

        return self.sampleEmployee

    def multiply_data_by(self, multiplier = None, **options) -> pyspark.sql.dataframe.DataFrame:
        """
        How many times you want to duplicate the data for?
        This is extremely slow when using spark unionAll method, so adding pandas as data size is small
        :param multiplier:
        :return:
        """
        multiplier = multiplier or 100
        if options.get('using', 'spark') == 'spark':
            # if there is a dataframe mentioned, assign it to self.df, or keep it the same
            self.sampleEmployee = options.get('df') or self.sampleEmployee
            original_df = self.sampleEmployee
            # start the multiplying of the data
            for itr in range(multiplier):
                print(f"Running the iteration :: {itr}")
                # perform the union function to generate the data
                self.sampleEmployee = self.sampleEmployee.unionAll(original_df)
                # persist the data, so we can just persist the data as soon as it is done,
                # to tackle the lazu=y evaluation
                print(f"Total count :: ", self.sampleEmployee.count())
                # shuffle the data to make the data random
                self.sampleEmployee = self.sampleEmployee.orderBy(rand())
        elif options.get('using', 'pandas') == 'pandas':
            # convert it into pandas df
            pandas_df = self.sampleEmployee.toPandas()
            # create the list, and every element in the list is the pandas df
            df_list = [pandas_df] * multiplier
            # concat all the df_list
            result_df = pd.concat(df_list, ignore_index = True).sample(frac = 1)
            # convert it to spark df_
            print("Result df is ready \n", result_df)
            self.sampleEmployee = spark.createDataFrame(result_df)
            print(f"Total count :: ", self.sampleEmployee.count())
        # create the view to execute the queries if we have any
        self.sampleEmployee.createOrReplaceTempView("sampleEmployee")
        print("Created the view")
        return self.sampleEmployee

    def execute_query(self, query):
        """
        Execute Spark sql queries on the df
        :param query: SQL query to execute
        :return:
        """
        return spark.sql(query)