import datetime
import os

from pyspark.sql.functions import *
from pyspark.sql.types import *

from resources.dev import config
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimensions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.spark_session import spark_session


###################### Get S3 client ######################
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(aws_access_key, aws_secret_key)
s3_client = s3_client_provider.get_client()

# Now you can use s3_client for your S3 operations
# List all CSV files in your S3 source folder
bucket      = config.bucket_name
prefix      = config.s3_source_directory.rstrip("/") + "/"
response        = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
s3_csv_files     = [
    obj["Key"]
    for obj in response.get("Contents", [])
    if obj["Key"].lower().endswith(".csv")
]

if not s3_csv_files:
    logger.info(f"No CSV files available at s3://{bucket}/{prefix}")
    raise Exception("No Data available to process")

# Check staging table for any in‑flight runs
file_names = [os.path.basename(k) for k in s3_csv_files]
in_list    = ",".join(f"'{fn}'" for fn in file_names)

statement = f"""
SELECT DISTINCT file_name
  FROM {config.database_name}.product_staging_table
 WHERE file_name IN ({in_list})
   AND status = 'A'
"""
logger.info("Staging‑table re‑run check SQL: %s", statement)

conn   = get_mysql_connection()
cursor = conn.cursor()
cursor.execute(statement)
existing = cursor.fetchall()
cursor.close()
conn.close()

if existing:
    logger.info("Previous run left these files Active: %s", existing)
    active_files = [row[0] for row in existing]  # list of filenames
    logger.info("Auto‑resetting these files to Inactive: %s", active_files)

    conn = get_mysql_connection()
    cursor = conn.cursor()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for fn in active_files:
        cursor.execute(f"""
                UPDATE {config.database_name}.{config.product_staging_table}
                   SET status = 'I', updated_date = '{now}'
                 WHERE file_name = '{fn}'
            """)
    conn.commit()
    cursor.close()
    conn.close()

    logger.info("Staging table cleaned; proceeding with processing.")
else:
    logger.info("No in-flight files found—proceeding")

# Now s3_files holds every CSV you’ll process in Spark via s3a://
#    (no local download needed)
# Build S3A URIs for Spark
spark_paths = [f"s3a://{config.bucket_name}/{key}" for key in s3_csv_files]
logger.info("Spark will read from: %s", spark_paths)



logger.info("********** Listing the File **********")
logger.info("List of csv files that needs to be processed %s", s3_csv_files)

logger.info("********** Creating spark session **********")
spark = spark_session()
logger.info("********** Spark session created **********")


# Check the required column in the schema of CSV files
# If not required columns, keep it in a list or error_files
# Else union all the data into one dataframe

logger.info("********** Checking Schema for data loaded in S3 **********")

correct_files = []
error_files = []
for file in s3_csv_files:
    s3_path = f"s3a://{bucket}/{file}"
    data_schema = spark.read.format("csv")\
                            .option("header", "true")\
                            .load(s3_path).columns

    logger.info(f"Schema for the {file} is {data_schema}")
    logger.info(f"Mandatory columns schema is {config.mandatory_columns}")

    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(file)
    else:
        logger.info(f"No missing column for the {file}")
        correct_files.append(file)

logger.info(f"********** List of correct files ********** {correct_files}")
logger.info(f"********** List of error files ********** {error_files}")

if error_files:
    logger.info("********** Moving error files to S3 error directory **********")
    source_prefix      = config.s3_source_directory.rstrip("/") + "/"
    destination_prefix = config.s3_error_directory.rstrip("/") + "/"

    for file in error_files:
        file_name = os.path.basename(file)
        message = move_s3_to_s3(
            s3_client,
            config.bucket_name,
            source_prefix,
            destination_prefix,
            file_name
        )
        logger.info(f"Moved {file} to {destination_prefix}: {message}")
else:
    logger.info("********** No error files to move **********")

#Additonal columns needs to be taken care of
#Determine extra columns

# Before running the process
# Stage table needs to be updated with status as Active (A) or Inactive (I)

logger.info("********** Updating the product_staging_table that we have started the process **********")


db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    insert_statements = []
    for file in correct_files:
        filename = os.path.basename(file)
        file_location = file
        statement = f"""
        INSERT INTO {db_name}.{config.product_staging_table} 
        (file_name, file_location, created_date, status)
        VALUES ('{filename}', '{file_location}', '{formatted_date}', 'A')
        """
        insert_statements.append(statement)

    logger.info(f"Insert statement created for staging table --- {insert_statements}")

    logger.info("********** Connecting with MySQL server **********")
    connection = get_mysql_connection()
    cursor = connection.cursor()

    logger.info("********** MySQL server connected successfully **********")

    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()

    cursor.close()
    connection.close()

    logger.info("********** Staging table updated successfully **********")
    logger.info("********** Fixing extra column coming from source **********")

else:
    logger.error("********** There are no files to process **********")
    raise Exception("********** No data available with correct files **********")


# Define the schema for the CSV files
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

# Connecting with DatabaseReader
database_client = DatabaseReader(config.url, config.mysql_properties)
logger.info("********** Creating empty dataframe **********")
final_df_to_process = database_client.create_dataframe(spark, table_name="empty_df_create_table")

# Processing each correct file
for file in correct_files:
    s3_path = f"s3a://{config.bucket_name}/{file}"
    data_df = spark.read.format("csv")\
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .load(s3_path)

    data_schema = data_df.columns

    extra_columns = list(set(data_schema) - set(config.mandatory_columns))

    logger.info(f"Extra columns present in {file}: {extra_columns}")

    # Collapse extras into one column, or add a null column if none
    if extra_columns:
        data_df = data_df.withColumn(
            "additional_column",
            concat_ws(",", *extra_columns)
        ).select(
            "customer_id", "store_id", "product_name", "sales_date",
            "sales_person_id", "price", "quantity", "total_cost", "additional_column"
        )

        logger.info(f"Processed {file} and added 'additional_column'")

    else:
        data_df = data_df.withColumn("additional_column", lit(None)) \
            .select(
                "customer_id", "store_id", "product_name", "sales_date",
                "sales_person_id", "price", "quantity", "total_cost", "additional_column"
            )

    # Union each data frame to final frame
    final_df_to_process = final_df_to_process.union(data_df)

# Show the final DataFrame
logger.info("********** Final Dataframe from source which will be going to processing **********")
final_df_to_process.show()

#Enrich the data from all dimension table
#also create datamart for sales_team and their incentive, address and all another datamart for customer who
#bought how much each days of month for every month there should be a file and inside that
#there should be a store_id segregation
#Read the data from parquet and generate a csv file
#in which there will be a sales_person_name,sales_person_store_id
#sales_person_total_billing_done_for_each_month, total_incentive

# Connecting with DatabaseReader
database_client = DatabaseReader(config.url, config.mysql_properties)

# Creating dataframe for all tables
# Customer table
logger.info("********** Loading customer table into customer_table_df **********")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)

# Product table
logger.info("********** Loading product table into product_table_df **********")
product_table_df = database_client.create_dataframe(spark, config.product_table)

# Product staging table
logger.info("********** Loading staging table into product_staging_table_df **********")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)

# Sales team table
logger.info("********** Loading sales team table into sales_team_table_df **********")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

# Store table
logger.info("********** Loading store table into store_table_df **********")
store_table_df = database_client.create_dataframe(spark, config.store_table)

# Final enriched data by joining
s3_customer_store_sales_df_join = dimensions_table_join(
    final_df_to_process,
    customer_table_df,
    store_table_df,
    sales_team_table_df
)

# Final Enriched Data
logger.info("********** Final Enriched Data **********")
s3_customer_store_sales_df_join.show()

# Write the customer data into customer data mart in parquet format
# File will be written to local first
# Move the RAW data to s3 bucket for reporting tool
# Write reporting data into MySQL table also

logger.info("************** Write the data into Customer Data Mart **************")
final_customer_data_mart_df = s3_customer_store_sales_df_join \
    .select(
        "ct.customer_id",
        "ct.first_name", "ct.last_name", "ct.address",
        "ct.pincode", "phone_number",
        "sales_date", "total_cost"
    )

logger.info("************** Final Data for customer Data Mart **************")
final_customer_data_mart_df.show()

customer_s3_path = f"s3a://{config.bucket_name}/{config.s3_customer_datamart_directory}"
logger.info("Writing Customer Data Mart to %s", customer_s3_path)
final_customer_data_mart_df.write \
    .mode("overwrite") \
    .parquet(customer_s3_path)

logger.info(f"************** Customer data written to Customer Data Mart at {config.s3_customer_datamart_directory} **************")

# -------------------- SALES TEAM DATA MART --------------------

# sales_team Data Mart
logger.info("************** write the data into sales team Data Mart **************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join \
    .select(
        "store_id",
        "sales_person_id", "sales_person_first_name", "sales_person_last_name",
        "store_manager_name", "manager_id", "is_manager",
        "sales_person_address", "sales_person_pincode",
        "sales_date", "total_cost",
        expr("SUBSTRING(sales_date,1,7) as sales_month")
    )

logger.info("************** Final Data for sales team Data Mart **************")
final_sales_team_data_mart_df.show()

#In these lines you’re persisting the “Sales Team” Data Mart out to S3 in two different forms
# once as a simple “flat” Parquet dataset
# once as a partitioned Parquet dataset that Hive‑style directory partition pruning can leverage.
sales_flat_s3_path = f"s3a://{config.bucket_name}/{config.s3_sales_datamart_directory}"
logger.info("Writing flat Sales Team Data Mart to %s", sales_flat_s3_path)
final_sales_team_data_mart_df.write \
    .mode("overwrite") \
    .parquet(sales_flat_s3_path)

sales_partitioned_s3_path = f"s3a://{config.bucket_name}/{config.s3_sales_datamart_directory}/partitioned"
logger.info("Writing partitioned Sales Team Data Mart to %s", sales_partitioned_s3_path)
final_sales_team_data_mart_df.write \
    .mode("overwrite") \
    .partitionBy("sales_month", "store_id") \
    .parquet(sales_partitioned_s3_path)

logger.info(f"************** Sales team data written to Sales Data Mart at {config.s3_sales_datamart_directory} **************")


logger.info("******Calculating customer every month purchased amount *******")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("******Calculation of customer mart done and written into the table*******")

# Calculation for Sales Team Mart
# Find out the total sales done by each sales person every month
# Give the top performer 1% incentive of total sales of the month
# Rest sales persons will get nothing
# Write the data into MySQL table

logger.info("******Calculating sales every month billed amount *******")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("******Calculation of sales mart done and written into the table*******")

###########################  Last Step ############################
# Move the files on S3 into processed folder
source_prefix = config.s3_source_directory.rstrip("/") + "/"
destination_prefix = config.s3_processed_directory.rstrip("/") + "/"
message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

# Update the status of staging table
if correct_files:
    update_statements = []
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"UPDATE {db_name}.{config.product_staging_table} " \
                     f" SET status = 'I', updated_date='{formatted_date}' " \
                     f"WHERE file_name = '{filename}'"
        update_statements.append(statements)

    logger.info(f"Updated statement created for staging table --- {update_statements}")
    logger.info("********** Connecting with MySQL server **********")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("********** MySQL server connected successfully **********")

    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()

    cursor.close()
    connection.close()
    logger.info("Staging table updated to Inactive for all processed files")
else:
    logger.error("********** There is some error in process in between **********")
    sys.exit()

input("Press enter to terminate ")
