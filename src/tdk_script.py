from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from application_logger import *
from config import *

logger=define_logger(log_file_location)
class tdk_script:
        
    def __init__(self):
        # Constructor code here
        pass

    def create_spark_session(self,appName):
        spark = SparkSession.builder.master("local")\
                .config("spark.jars",jar_path )\
                .config("spark.sql.parquet.enableVectorizedReader", "false")\
                .appName(appName)\
                .getOrCreate()
        return spark

    def extract_data(self, spark,basepath,path):
        df_input = spark.read.option("basepath", basepath).parquet(path)
        return df_input

    def process_data(self, df_input):
        df_input = df_input.withColumn("execution_time_stamp", col("execution_time_stamp").cast(TimestampType()))
        logger.info('Converted execution_time_stamp column to timestamp type from String')
        df_input = df_input.na.fill(value='BLANK_ovs_cust_name',subset=["ovs_cust_name"])
        df_input = df_input.na.fill(value='BLANK_sap_customer_no',subset=["sap_customer_no"])
        df_input = df_input.na.fill(value='BLANK_item_name',subset=["item_name"])
        df_input = df_input.na.fill(value='BLANK_item_code',subset=["item_code"])
        logger.info('Null values replaced from ovs_cust_name,sap_customer_no,item_name,item_code')
        w = Window.partitionBy('ovs_cust_name', 'sap_customer_no', 'item_name', 'item_code').orderBy(desc('execution_time_stamp'))
        df_input = df_input.withColumn("row", row_number().over(w))
        df_input = df_input.filter(col("row") == 1).drop("row")

        return df_input
    def typecast_data(self, df_input):
        coltype_map = {
            'base_month': 'integer',
            'term': 'integer',
            'budget_amt': 'integer',
            'budget_qty': 'integer',
            'budget_price': 'integer',
            'result_amt': 'integer',
            'result_qty': 'integer',
            'result_price': 'integer',
            'co_amt': 'integer',
            'co_qty': 'integer',
            'co_price': 'integer',
            'fi_amt': 'integer',
            'fi_qty': 'integer',
            'fi_price': 'integer',
            'bok_amt': 'integer',
            'bok_qty': 'integer',
            'bok_price': 'integer',
        }
        logger.info('Mapping Dictionary defined')
        rest_cols = [F.col(cl) for cl in df_input.columns if cl not in coltype_map]
        conv_cols = [F.col(cl_name).cast(cl_type).alias(cl_name)
                     for cl_name, cl_type in coltype_map.items() if cl_name in df_input.columns]
        df_input1 = df_input.select(*rest_cols, *conv_cols)
        logger.info('data typecasted into destination datatype')
        return df_input1

    def load_data_to_destination(self, df_input1,url,driver,dbtable,user,password):
        df_input1.write.format('jdbc').options(
            url=url,
            driver=driver,
            dbtable=dbtable,
            user=user,
            password=password).mode('append').save()
        logger.info('Data Written to destination')

    def main(self):
        # configuration
        

        spark = self.create_spark_session("TDK_USE_CASE")
        logger.info('Spark Session Created')
        
    
        df_input = self.extract_data(spark,Input_file_directory,Input_file_path)
        logger.info('Input File Read')
        

        logger.info('fetch {} records from source files \n'.format(df_input.count()))

        df_input = self.process_data(df_input)
        df_input1=self.typecast_data(df_input)
        
        self.load_data_to_destination(df_input1,url,driver,dbtable,user,password)

        spark.stop()
        logger.info('Spark Session Stopped')
