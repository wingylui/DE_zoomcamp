# %%
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


from google.cloud import bigquery

# %%
# parse arguments
parser = argparse.ArgumentParser(description='GCP setup')
parser.add_argument('--projectid', required=True, help='GCP Project ID')
parser.add_argument('--BIGQuerydataset', required=True, help='BigQuery Dataset Name')
parser.add_argument('--bucket', required=True, help='Bucket Name')
parser.add_argument('--year', required=True, help='year')
args = parser.parse_args()

# jar for spark run in GCP
bucket_jar = f"gs://{args.bucket}/jars"
bigquery_jar = "spark-3.5-bigquery-0.42.1.jar"
gcs_connector_jar = "gcs-connector-hadoop3-latest.jar"

# variables
GCP_projectID = args.projectid
BigQuery_dataset = args.BIGQuerydataset
bucket = args.bucket
year = args.year

# buckets link
birthRate_gs = f"gs://{args.bucket}/birth-rate_{year}.csv"
life_gs = f"gs://{args.bucket}/life-expectancy_{year}.csv"
refugee_gs = f"gs://{args.bucket}/refugee-population_{year}.csv"
migrant_gs = f"gs://{args.bucket}/migrant-total_{year}.csv"

# %%
# create spark session
spark = SparkSession.builder \
    .appName("DE_Zoomcamp_Population") \
    .config("spark.jars", f"{bucket_jar}/{bigquery_jar},{bucket_jar}/{gcs_connector_jar}") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.executor.heartbeatInterval", "3660s") \
    .config("spark.network.timeout", "3600s") \
    .getOrCreate()

# read data from bucket
birth_rate_df = spark.read.csv(birthRate_gs) \
                        .option(delimiter=",", header=True)
life_df = spark.read.csv(life_gs) \
                        .option(delimiter=",", header=True)
refugee_df = spark.read.csv(refugee_gs) \
                        .option(delimiter=",", header=True)
migrant_df = spark.read.csv(migrant_gs) \
                        .option(delimiter=",", header=True)


# create dim table
country_df = life_df.groupBy(["Code", "Entity"]).count()\
              .drop("count")

# dropping country name columns for all the tables
birth_rate_clean  =   birth_rate_df.drop("Entity")
life_clean        =   life_df.drop("Entity")
refugee_clean     =   refugee_df.drop("Entity")
migrant_clean     =   migrant_df.drop("Entity")


# merging four tables together
combined_df = birth_rate_clean \
              .join(life_clean,     on=["Code", "Year"], how= "left") \
              .join(refugee_clean,  on=["Code", "Year"], how= "left") \
              .join(migrant_clean,  on=["Code", "Year"], how= "left" )

# filter out no country code and then replace null to 0
combined_df = combined_df.filter(combined_df.Code.isNotNull()) 
                         
              
# rename column
combined_df = combined_df \
              .withColumnRenamed("Birth rate (historical)", "birth_rate") \
              .withColumnRenamed("Life expectancy - Sex: total - Age: 0 - Type: period", "life_expectancy") \
              .withColumnRenamed("Total number of international immigrants", "international_immigrants") \
              .withColumnRenamed("Refugees by country of origin", "refugees")

# changing column type
combined_df = combined_df \
              .withColumn("Year",                     F.col("Year").cast("int")) \
              .withColumn("birth_rate",               F.col("birth_rate").cast("float")) \
              .withColumn("life_expectancy",          F.col("life_expectancy").cast("float")) \
              .withColumn("international_immigrants", F.col("international_immigrants").cast("float")) \
              .withColumn("refugees",                 F.col("refugees").cast("float")) 

# replace 0 for all the null values
combined_df = combined_df.fillna(0, subset=["birth_rate", "life_expectancy", "international_immigrants", "refugees"])



# BigQuery tables
master_table = "fact_population"
country = "dim_country"

# writing tables with partition and clustering the table
combined_df.write.format("bigquery").mode("append") \
            .option("writeMethod", "direct") \
            .option("table", f"{GCP_projectID}:{BigQuery_dataset}.{master_table}") \
            .save()

country_df.write.format("bigquery").mode("append") \
            .option("writeMethod", "direct") \
            .option("table", f"{GCP_projectID}:{BigQuery_dataset}.{master_table}") \
            .save()


# stop the session
spark.stop()



