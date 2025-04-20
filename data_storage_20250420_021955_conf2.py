from pyspark.sql import SparkSession, functions as F
from datetime import datetime, date
import calendar

# Initialize SparkSession
spark = SparkSession.builder.appName("SAS_Conversion").getOrCreate()

# Get reporting date and extract components
reporting_date = date.today()
reporting_year = reporting_date.year
reporting_month = reporting_date.month

# Create analysis_start_date as first day of current year
analysis_start_date = date(reporting_year, 1, 1)

# Define variables to keep (equivalent to SAS keep_vars)
keep_vars = ["customer_id", "account_id", "transaction_date", "amount", "balance", "status"]

# These variables are now available for use in subsequent PySpark code
# Example usage:
# df = df.select(keep_vars)  # Select only the columns defined in keep_vars

from pyspark.sql import SparkSession, functions as F
from datetime import datetime, date
import calendar

# Initialize SparkSession
spark = SparkSession.builder.appName("SAS_Conversion").getOrCreate()

# Get reporting date and extract components
reporting_date = date.today()
reporting_year = reporting_date.year
reporting_month = reporting_date.month

# Create analysis_start_date as first day of current year
analysis_start_date = date(reporting_year, 1, 1)

# Define variables to keep (equivalent to SAS keep_vars)
keep_vars = ["customer_id", "account_id", "transaction_date", "amount", "balance", "status"]

# These variables are now available for use in subsequent PySpark code
# Example usage:
# df = df.select(keep_vars)  # Select only the columns defined in keep_vars

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Define a UDF to format dates similar to SAS format_date macro
def format_date(date, date_format='yyyy-MM-dd'):
    """Format a date or return null if the date is missing.
    
    Args:
        date: The date to format
        date_format: The format string to use (default: 'yyyy-MM-dd')
        
    Returns:
        Formatted date string or None if date is None
    """
    if date is None:
        return None
    else:
        return date.strftime(date_format)

# Register the UDF
format_date_udf = F.udf(format_date, StringType())

# Example usage:
# df = df.withColumn("formatted_date", format_date_udf(F.col("date_column"), F.lit("yyyy-MM-dd")))

# Alternative using built-in Spark functions (preferred for performance):
# df = df.withColumn("formatted_date", 
#                    F.when(F.col("date_column").isNull(), None)
#                     .otherwise(F.date_format(F.col("date_column"), "yyyy-MM-dd")))


from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Define a UDF to format dates similar to SAS format_date macro
def format_date(date, date_format='yyyy-MM-dd'):
    """Format a date or return null if the date is missing.
    
    Args:
        date: The date to format
        date_format: The format string to use (default: 'yyyy-MM-dd')
        
    Returns:
        Formatted date string or None if date is None
    """
    if date is None:
        return None
    else:
        return date.strftime(date_format)

# Register the UDF
format_date_udf = F.udf(format_date, StringType())

# Example usage:
# df = df.withColumn("formatted_date", format_date_udf(F.col("date_column"), F.lit("yyyy-MM-dd")))

# Alternative using built-in Spark functions (preferred for performance):
# df = df.withColumn("formatted_date", 
#                    F.when(F.col("date_column").isNull(), None)
#                     .otherwise(F.date_format(F.col("date_column"), "yyyy-MM-dd")))


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assuming 'transaction_date' is a column in your DataFrame 'df'
# and reporting_date is a variable passed to the script

def process_transaction_dates(df, reporting_date):
    
    # Add derived date columns only for non-null transaction_date values
    return df.withColumn(
        "transaction_year", 
        F.when(F.col("transaction_date").isNotNull(), F.year(F.col("transaction_date")))
    ).withColumn(
        "transaction_month", 
        F.when(F.col("transaction_date").isNotNull(), F.month(F.col("transaction_date")))
    ).withColumn(
        "transaction_day", 
        F.when(F.col("transaction_date").isNotNull(), F.dayofmonth(F.col("transaction_date")))
    ).withColumn(
        "days_since_transaction", 
        F.when(F.col("transaction_date").isNotNull(), 
               F.datediff(F.lit(reporting_date), F.col("transaction_date")))
    )

# Example usage:
# df_processed = process_transaction_dates(df, reporting_date)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assuming 'transaction_date' is a column in your DataFrame 'df'
# and reporting_date is a variable passed to the script

def process_transaction_dates(df, reporting_date):
    
    # Add derived date columns only for non-null transaction_date values
    return df.withColumn(
        "transaction_year", 
        F.when(F.col("transaction_date").isNotNull(), F.year(F.col("transaction_date")))
    ).withColumn(
        "transaction_month", 
        F.when(F.col("transaction_date").isNotNull(), F.month(F.col("transaction_date")))
    ).withColumn(
        "transaction_day", 
        F.when(F.col("transaction_date").isNotNull(), F.dayofmonth(F.col("transaction_date")))
    ).withColumn(
        "days_since_transaction", 
        F.when(F.col("transaction_date").isNotNull(), 
               F.datediff(F.lit(reporting_date), F.col("transaction_date")))
    )

# Example usage:
# df_processed = process_transaction_dates(df, reporting_date)

from pyspark.sql import functions as F

# Create new columns for positive and negative amounts
df = df.withColumn(
    "positive_amount",
    F.when(F.col("amount") > 0, F.col("amount")).otherwise(0)
).withColumn(
    "negative_amount",
    F.when(F.col("amount") < 0, F.abs(F.col("amount"))).otherwise(0)
)

from pyspark.sql import functions as F

# Create new columns for positive and negative amounts
df = df.withColumn(
    "positive_amount",
    F.when(F.col("amount") > 0, F.col("amount")).otherwise(0)
).withColumn(
    "negative_amount",
    F.when(F.col("amount") < 0, F.abs(F.col("amount"))).otherwise(0)
)

from pyspark.sql import functions as F

# Format customer_id and account_id with prefixes and padding
df = df.withColumn(
    "customer_id_formatted", 
    F.concat(F.lit("CUST_"), F.lpad(F.col("customer_id").cast("string"), 8, " "))
)

df = df.withColumn(
    "account_id_formatted", 
    F.concat(F.lit("ACCT_"), F.lpad(F.col("account_id").cast("string"), 10, " "))
)

from pyspark.sql import functions as F

# Format customer_id and account_id with prefixes and padding
df = df.withColumn(
    "customer_id_formatted", 
    F.concat(F.lit("CUST_"), F.lpad(F.col("customer_id").cast("string"), 8, " "))
)

df = df.withColumn(
    "account_id_formatted", 
    F.concat(F.lit("ACCT_"), F.lpad(F.col("account_id").cast("string"), 10, " "))
)

from pyspark.sql import functions as F

# Define a UDF to replicate the IFN nested logic
def status_code_udf(status):
    if status is None:
        return 99
    status_upper = status.upper()
    if status_upper == 'ACTIVE':
        return 1
    elif status_upper == 'PENDING':
        return 2
    elif status_upper == 'CLOSED':
        return 3
    else:
        return 99

# Register the UDF
status_code_udf_registered = F.udf(status_code_udf, returnType=F.IntegerType())

# Apply the transformation to the DataFrame
df = df.withColumn("status_code", status_code_udf_registered(F.col("status")))

# Alternative approach using when/otherwise
df = df.withColumn("status_code", 
                   F.when(F.upper(F.col("status")) == "ACTIVE", 1)
                    .when(F.upper(F.col("status")) == "PENDING", 2)
                    .when(F.upper(F.col("status")) == "CLOSED", 3)
                    .otherwise(99))

from pyspark.sql import functions as F

# Define a UDF to replicate the IFN nested logic
def status_code_udf(status):
    if status is None:
        return 99
    status_upper = status.upper()
    if status_upper == 'ACTIVE':
        return 1
    elif status_upper == 'PENDING':
        return 2
    elif status_upper == 'CLOSED':
        return 3
    else:
        return 99

# Register the UDF
status_code_udf_registered = F.udf(status_code_udf, returnType=F.IntegerType())

# Apply the transformation to the DataFrame
df = df.withColumn("status_code", status_code_udf_registered(F.col("status")))

# Alternative approach using when/otherwise
df = df.withColumn("status_code", 
                   F.when(F.upper(F.col("status")) == "ACTIVE", 1)
                    .when(F.upper(F.col("status")) == "PENDING", 2)
                    .when(F.upper(F.col("status")) == "CLOSED", 3)
                    .otherwise(99))

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("Format Conversion").getOrCreate()

# Create a dictionary to store the format mappings
status_fmt = {
    1: 'Active',
    2: 'Pending',
    3: 'Closed',
    99: 'Unknown'
}

flag_fmt = {
    0: 'No',
    1: 'Yes'
}

# Create UDFs for these format mappings
status_fmt_udf = F.udf(lambda x: status_fmt.get(x) if x in status_fmt else None, StringType())
flag_fmt_udf = F.udf(lambda x: flag_fmt.get(x) if x in flag_fmt else None, StringType())

# Example of how to use these UDFs with a DataFrame
# df = df.withColumn("status_formatted", status_fmt_udf(F.col("status")))
# df = df.withColumn("flag_formatted", flag_fmt_udf(F.col("flag")))

# Alternative approach using when/otherwise for better performance
def apply_status_fmt(df, col_name):
    status_expr = F.when(F.col(col_name) == 1, F.lit("Active"))\
                   .when(F.col(col_name) == 2, F.lit("Pending"))\
                   .when(F.col(col_name) == 3, F.lit("Closed"))\
                   .when(F.col(col_name) == 99, F.lit("Unknown"))\
                   .otherwise(None)
    return df.withColumn(f"{col_name}_formatted", status_expr)

def apply_flag_fmt(df, col_name):
    flag_expr = F.when(F.col(col_name) == 0, F.lit("No"))\
                .when(F.col(col_name) == 1, F.lit("Yes"))\
                .otherwise(None)
    return df.withColumn(f"{col_name}_formatted", flag_expr)

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("Format Conversion").getOrCreate()

# Create a dictionary to store the format mappings
status_fmt = {
    1: 'Active',
    2: 'Pending',
    3: 'Closed',
    99: 'Unknown'
}

flag_fmt = {
    0: 'No',
    1: 'Yes'
}

# Create UDFs for these format mappings
status_fmt_udf = F.udf(lambda x: status_fmt.get(x) if x in status_fmt else None, StringType())
flag_fmt_udf = F.udf(lambda x: flag_fmt.get(x) if x in flag_fmt else None, StringType())

# Example of how to use these UDFs with a DataFrame
# df = df.withColumn("status_formatted", status_fmt_udf(F.col("status")))
# df = df.withColumn("flag_formatted", flag_fmt_udf(F.col("flag")))

# Alternative approach using when/otherwise for better performance
def apply_status_fmt(df, col_name):
    status_expr = F.when(F.col(col_name) == 1, F.lit("Active"))\
                   .when(F.col(col_name) == 2, F.lit("Pending"))\
                   .when(F.col(col_name) == 3, F.lit("Closed"))\
                   .when(F.col(col_name) == 99, F.lit("Unknown"))\
                   .otherwise(None)
    return df.withColumn(f"{col_name}_formatted", status_expr)

def apply_flag_fmt(df, col_name):
    flag_expr = F.when(F.col(col_name) == 0, F.lit("No"))\
                .when(F.col(col_name) == 1, F.lit("Yes"))\
                .otherwise(None)
    return df.withColumn(f"{col_name}_formatted", flag_expr)

# Assuming output_ds is a DataFrame variable that already exists

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Define UDF functions to replicate SAS formats
def status_fmt(code):
    # This is a placeholder - you need to implement the actual format logic
    # that matches your SAS status_fmt format
    return str(code)

def flag_fmt(flag):
    # This is a placeholder - you need to implement the actual format logic
    # that matches your SAS flag_fmt format
    return str(flag)

# Register the UDFs
status_fmt_udf = F.udf(status_fmt, StringType())
flag_fmt_udf = F.udf(flag_fmt, StringType())

# Apply formats to the DataFrame
output_ds = output_ds.withColumn("status_code", status_fmt_udf(F.col("status_code")))
output_ds = output_ds.withColumn("large_transaction_flag", flag_fmt_udf(F.col("large_transaction_flag")))
output_ds = output_ds.withColumn("high_balance_flag", flag_fmt_udf(F.col("high_balance_flag")))


# Assuming output_ds is a DataFrame variable that already exists

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Define UDF functions to replicate SAS formats
def status_fmt(code):
    # This is a placeholder - you need to implement the actual format logic
    # that matches your SAS status_fmt format
    return str(code)

def flag_fmt(flag):
    # This is a placeholder - you need to implement the actual format logic
    # that matches your SAS flag_fmt format
    return str(flag)

# Register the UDFs
status_fmt_udf = F.udf(status_fmt, StringType())
flag_fmt_udf = F.udf(flag_fmt, StringType())

# Apply formats to the DataFrame
output_ds = output_ds.withColumn("status_code", status_fmt_udf(F.col("status_code")))
output_ds = output_ds.withColumn("large_transaction_flag", flag_fmt_udf(F.col("large_transaction_flag")))
output_ds = output_ds.withColumn("high_balance_flag", flag_fmt_udf(F.col("high_balance_flag")))


from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("TransactionsProcessing").getOrCreate()

# Define schema for the CSV file
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("balance", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("description", StringType(), True)
])

# Read CSV file into DataFrame
raw_transactions = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .schema(schema) \
    .load("/path/to/transactions.csv")

# Save DataFrame as a table or parquet file
raw_transactions.write.mode("overwrite").saveAsTable("raw_transactions")


from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("TransactionsProcessing").getOrCreate()

# Define schema for the CSV file
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("balance", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("description", StringType(), True)
])

# Read CSV file into DataFrame
raw_transactions = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .schema(schema) \
    .load("/path/to/transactions.csv")

# Save DataFrame as a table or parquet file
raw_transactions.write.mode("overwrite").saveAsTable("raw_transactions")


from pyspark.sql import functions as F

# Apply strip function to remove leading and trailing spaces
df = df.withColumn("transaction_type", F.trim(F.col("transaction_type")))\
      .withColumn("status", F.trim(F.col("status")))\
      .withColumn("description", F.trim(F.col("description")))

from pyspark.sql import functions as F

# Apply strip function to remove leading and trailing spaces
df = df.withColumn("transaction_type", F.trim(F.col("transaction_type")))\
      .withColumn("status", F.trim(F.col("status")))\
      .withColumn("description", F.trim(F.col("description")))

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerData").getOrCreate()

# Define schema for the DataFrame
schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("join_date", DateType(), True),
    StructField("customer_type", StringType(), True),
    StructField("credit_score", IntegerType(), True)
])

# Create data for the DataFrame
data = [
    (1, "John", "Smith", datetime.date(2015, 1, 1), "REGULAR", 720),
    (2, "Jane", "Doe", datetime.date(2016, 2, 15), "PREMIUM", 780),
    (3, "Robert", "Johnson", datetime.date(2017, 3, 10), "REGULAR", 690),
    (4, "Sarah", "Williams", datetime.date(2018, 4, 20), "PREMIUM", 810),
    (5, "Michael", "Brown", datetime.date(2019, 5, 5), "REGULAR", 700),
    (6, "Jennifer", "Davis", datetime.date(2020, 6, 12), "REGULAR", 720),
    (7, "David", "Miller", datetime.date(2021, 7, 25), "PREMIUM", 750),
    (8, "Lisa", "Wilson", datetime.date(2018, 8, 30), "REGULAR", 680),
    (9, "Richard", "Moore", datetime.date(2019, 9, 15), "PREMIUM", 790),
    (10, "Patricia", "Taylor", datetime.date(2020, 10, 28), "REGULAR", 710)
]

# Create DataFrame
customers_df = spark.createDataFrame(data, schema)

# Save DataFrame (equivalent to sasdata.customers)
customers_df.write.mode("overwrite").format("parquet").saveAsTable("customers")

# Show the data
customers_df.show()

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerData").getOrCreate()

# Define schema for the DataFrame
schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("join_date", DateType(), True),
    StructField("customer_type", StringType(), True),
    StructField("credit_score", IntegerType(), True)
])

# Create data for the DataFrame
data = [
    (1, "John", "Smith", datetime.date(2015, 1, 1), "REGULAR", 720),
    (2, "Jane", "Doe", datetime.date(2016, 2, 15), "PREMIUM", 780),
    (3, "Robert", "Johnson", datetime.date(2017, 3, 10), "REGULAR", 690),
    (4, "Sarah", "Williams", datetime.date(2018, 4, 20), "PREMIUM", 810),
    (5, "Michael", "Brown", datetime.date(2019, 5, 5), "REGULAR", 700),
    (6, "Jennifer", "Davis", datetime.date(2020, 6, 12), "REGULAR", 720),
    (7, "David", "Miller", datetime.date(2021, 7, 25), "PREMIUM", 750),
    (8, "Lisa", "Wilson", datetime.date(2018, 8, 30), "REGULAR", 680),
    (9, "Richard", "Moore", datetime.date(2019, 9, 15), "PREMIUM", 790),
    (10, "Patricia", "Taylor", datetime.date(2020, 10, 28), "REGULAR", 710)
]

# Create DataFrame
customers_df = spark.createDataFrame(data, schema)

# Save DataFrame (equivalent to sasdata.customers)
customers_df.write.mode("overwrite").format("parquet").saveAsTable("customers")

# Show the data
customers_df.show()

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("SAS to PySpark Conversion").getOrCreate()

# Define the schema for the accounts DataFrame
accounts_schema = StructType([
    StructField("account_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("account_type", StringType(), False),
    StructField("open_date", DateType(), True),
    StructField("close_date", DateType(), True),
    StructField("initial_balance", DoubleType(), True)
])

# Create the data as a list of tuples
accounts_data = [
    (101, 1, "CHECKING", "2015-02-01", None, 1500.0),
    (102, 1, "SAVINGS", "2015-02-01", None, 5000.0),
    (103, 2, "CHECKING", "2016-02-20", None, 2500.0),
    (104, 2, "SAVINGS", "2016-02-20", None, 10000.0),
    (105, 3, "CHECKING", "2017-03-15", None, 1000.0),
    (106, 4, "CHECKING", "2018-04-25", None, 2000.0),
    (107, 4, "SAVINGS", "2018-04-25", None, 7500.0),
    (108, 5, "CHECKING", "2019-05-10", None, 1200.0),
    (109, 6, "CHECKING", "2020-06-15", None, 1800.0),
    (110, 7, "SAVINGS", "2021-07-30", None, 15000.0),
    (111, 8, "CHECKING", "2018-09-05", None, 900.0),
    (112, 9, "CHECKING", "2019-09-20", None, 3000.0),
    (113, 9, "SAVINGS", "2019-09-20", None, 12000.0),
    (114, 10, "CHECKING", "2020-11-05", None, 1700.0),
    (115, 3, "SAVINGS", "2017-03-20", "2019-06-15", 4000.0)
]

# Create DataFrame
accounts_df = spark.createDataFrame(accounts_data, schema=accounts_schema)

# Convert string dates to actual date type
accounts_df = accounts_df.withColumn("open_date", F.to_date(F.col("open_date")))
accounts_df = accounts_df.withColumn("close_date", F.to_date(F.col("close_date")))

# Save the DataFrame to the desired location
accounts_df.write.mode("overwrite").parquet("sasdata/accounts")

# Show the DataFrame
accounts_df.show()

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("SAS to PySpark Conversion").getOrCreate()

# Define the schema for the accounts DataFrame
accounts_schema = StructType([
    StructField("account_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("account_type", StringType(), False),
    StructField("open_date", DateType(), True),
    StructField("close_date", DateType(), True),
    StructField("initial_balance", DoubleType(), True)
])

# Create the data as a list of tuples
accounts_data = [
    (101, 1, "CHECKING", "2015-02-01", None, 1500.0),
    (102, 1, "SAVINGS", "2015-02-01", None, 5000.0),
    (103, 2, "CHECKING", "2016-02-20", None, 2500.0),
    (104, 2, "SAVINGS", "2016-02-20", None, 10000.0),
    (105, 3, "CHECKING", "2017-03-15", None, 1000.0),
    (106, 4, "CHECKING", "2018-04-25", None, 2000.0),
    (107, 4, "SAVINGS", "2018-04-25", None, 7500.0),
    (108, 5, "CHECKING", "2019-05-10", None, 1200.0),
    (109, 6, "CHECKING", "2020-06-15", None, 1800.0),
    (110, 7, "SAVINGS", "2021-07-30", None, 15000.0),
    (111, 8, "CHECKING", "2018-09-05", None, 900.0),
    (112, 9, "CHECKING", "2019-09-20", None, 3000.0),
    (113, 9, "SAVINGS", "2019-09-20", None, 12000.0),
    (114, 10, "CHECKING", "2020-11-05", None, 1700.0),
    (115, 3, "SAVINGS", "2017-03-20", "2019-06-15", 4000.0)
]

# Create DataFrame
accounts_df = spark.createDataFrame(accounts_data, schema=accounts_schema)

# Convert string dates to actual date type
accounts_df = accounts_df.withColumn("open_date", F.to_date(F.col("open_date")))
accounts_df = accounts_df.withColumn("close_date", F.to_date(F.col("close_date")))

# Save the DataFrame to the desired location
accounts_df.write.mode("overwrite").parquet("sasdata/accounts")

# Show the DataFrame
accounts_df.show()

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Initialize SparkSession
spark = SparkSession.builder.appName("SAS_Array_Conversion").getOrCreate()

# Create lists equivalent to SAS arrays
cust_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
acct_ids = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115]
txn_types = ['DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'PAYMENT']
status_vals = ['ACTIVE', 'PENDING', 'CLOSED']

# If you need these as DataFrames for operations:
cust_ids_df = spark.createDataFrame([(x,) for x in cust_ids], ["customer_id"])
acct_ids_df = spark.createDataFrame([(x,) for x in acct_ids], ["account_id"])
txn_types_df = spark.createDataFrame([(x,) for x in txn_types], ["transaction_type"])
status_vals_df = spark.createDataFrame([(x,) for x in status_vals], ["status"])

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Initialize SparkSession
spark = SparkSession.builder.appName("SAS_Array_Conversion").getOrCreate()

# Create lists equivalent to SAS arrays
cust_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
acct_ids = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115]
txn_types = ['DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'PAYMENT']
status_vals = ['ACTIVE', 'PENDING', 'CLOSED']

# If you need these as DataFrames for operations:
cust_ids_df = spark.createDataFrame([(x,) for x in cust_ids], ["customer_id"])
acct_ids_df = spark.createDataFrame([(x,) for x in acct_ids], ["account_id"])
txn_types_df = spark.createDataFrame([(x,) for x in txn_types], ["transaction_type"])
status_vals_df = spark.createDataFrame([(x,) for x in status_vals], ["status"])

from pyspark.sql import SparkSession, functions as F
import random

# Initialize SparkSession
spark = SparkSession.builder.appName("SAS_Random_Conversion").getOrCreate()

# Set random seeds to match SAS behavior
random.seed(123)
cust_seed = random.random()
random.seed(456)
acct_seed = random.random()
random.seed(789)
txn_seed = random.random()
random.seed(12)  # SAS uses 012, but Python treats it as octal 10, so using 12
status_seed = random.random()

# Create a DataFrame with a single row to apply the transformations
df = spark.createDataFrame([(1,)], ["dummy"])

# Apply the random number generation with the seeds
df = df.withColumn("cust_idx", F.ceil(F.rand(seed=123) * 10))
df = df.withColumn("acct_idx", F.ceil(F.rand(seed=456) * 15))
df = df.withColumn("txn_idx", F.ceil(F.rand(seed=789) * 4))
df = df.withColumn("status_idx", F.ceil(F.rand(seed=12) * 3))

# Show the result
df.select("cust_idx", "acct_idx", "txn_idx", "status_idx").show()

from pyspark.sql import SparkSession, functions as F
import random

# Initialize SparkSession
spark = SparkSession.builder.appName("SAS_Random_Conversion").getOrCreate()

# Set random seeds to match SAS behavior
random.seed(123)
cust_seed = random.random()
random.seed(456)
acct_seed = random.random()
random.seed(789)
txn_seed = random.random()
random.seed(12)  # SAS uses 012, but Python treats it as octal 10, so using 12
status_seed = random.random()

# Create a DataFrame with a single row to apply the transformations
df = spark.createDataFrame([(1,)], ["dummy"])

# Apply the random number generation with the seeds
df = df.withColumn("cust_idx", F.ceil(F.rand(seed=123) * 10))
df = df.withColumn("acct_idx", F.ceil(F.rand(seed=456) * 15))
df = df.withColumn("txn_idx", F.ceil(F.rand(seed=789) * 4))
df = df.withColumn("status_idx", F.ceil(F.rand(seed=12) * 3))

# Show the result
df.select("cust_idx", "acct_idx", "txn_idx", "status_idx").show()

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# Assuming cust_ids, acct_ids, txn_types are lists or arrays available in the driver
# and cust_idx, acct_idx, txn_idx are indices

def generate_transaction_data(cust_ids, acct_ids, txn_types, cust_idx, acct_idx, txn_idx):
    # Get values based on indices
    customer_id = cust_ids[cust_idx]
    account_id = acct_ids[acct_idx]
    
    # Equivalent to TODAY() - CEIL(RANUNI(345) * 365)
    # RANUNI generates random uniform value between 0 and 1
    # Setting seed for reproducibility similar to SAS
    random.seed(345)
    days_ago = int(random.random() * 365) + 1  # CEIL ensures at least 1 day
    transaction_date = datetime.now().date() - timedelta(days=days_ago)
    
    transaction_type = txn_types[txn_idx]
    
    return {"customer_id": customer_id, 
            "account_id": account_id, 
            "transaction_date": transaction_date, 
            "transaction_type": transaction_type}

# To use this in a DataFrame context:
# Example usage (assuming the indices and arrays are defined)
# transaction = generate_transaction_data(cust_ids, acct_ids, txn_types, cust_idx, acct_idx, txn_idx)
# transaction_df = spark.createDataFrame([transaction])

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# Assuming cust_ids, acct_ids, txn_types are lists or arrays available in the driver
# and cust_idx, acct_idx, txn_idx are indices

def generate_transaction_data(cust_ids, acct_ids, txn_types, cust_idx, acct_idx, txn_idx):
    # Get values based on indices
    customer_id = cust_ids[cust_idx]
    account_id = acct_ids[acct_idx]
    
    # Equivalent to TODAY() - CEIL(RANUNI(345) * 365)
    # RANUNI generates random uniform value between 0 and 1
    # Setting seed for reproducibility similar to SAS
    random.seed(345)
    days_ago = int(random.random() * 365) + 1  # CEIL ensures at least 1 day
    transaction_date = datetime.now().date() - timedelta(days=days_ago)
    
    transaction_type = txn_types[txn_idx]
    
    return {"customer_id": customer_id, 
            "account_id": account_id, 
            "transaction_date": transaction_date, 
            "transaction_type": transaction_type}

# To use this in a DataFrame context:
# Example usage (assuming the indices and arrays are defined)
# transaction = generate_transaction_data(cust_ids, acct_ids, txn_types, cust_idx, acct_idx, txn_idx)
# transaction_df = spark.createDataFrame([transaction])

from pyspark.sql import functions as F
import random

# Set seeds for reproducibility similar to SAS RANUNI seeds
random.seed(567)
seed_567 = random.random()
random.seed(678)
seed_678 = random.random()
random.seed(789)
seed_789 = random.random()
random.seed(890)
seed_890 = random.random()

# Create UDFs for the random number generation with different seeds
def ranuni_567():
    random.seed(567)
    return random.random()

def ranuni_678():
    random.seed(678)
    return random.random()

def ranuni_789():
    random.seed(789)
    return random.random()

def ranuni_890():
    random.seed(890)
    return random.random()

# Register UDFs
ranuni_567_udf = F.udf(lambda: ranuni_567(), F.DoubleType())
ranuni_678_udf = F.udf(lambda: ranuni_678(), F.DoubleType())
ranuni_789_udf = F.udf(lambda: ranuni_789(), F.DoubleType())
ranuni_890_udf = F.udf(lambda: ranuni_890(), F.DoubleType())

# Apply the CASE logic using when/otherwise
df = df.withColumn(
    "amount",
    F.when(F.col("transaction_type") == "DEPOSIT", 
           F.round(ranuni_567_udf() * F.lit(5000), 2))
     .when(F.col("transaction_type") == "WITHDRAWAL", 
           F.round(ranuni_678_udf() * F.lit(-2000), 2))
     .when(F.col("transaction_type") == "TRANSFER", 
           F.round((ranuni_789_udf() * F.lit(2000)) - F.lit(1000), 2))
     .when(F.col("transaction_type") == "PAYMENT", 
           F.round(ranuni_890_udf() * F.lit(-1000), 2))
     .otherwise(F.lit(0))
)

from pyspark.sql import functions as F
import random

# Set seeds for reproducibility similar to SAS RANUNI seeds
random.seed(567)
seed_567 = random.random()
random.seed(678)
seed_678 = random.random()
random.seed(789)
seed_789 = random.random()
random.seed(890)
seed_890 = random.random()

# Create UDFs for the random number generation with different seeds
def ranuni_567():
    random.seed(567)
    return random.random()

def ranuni_678():
    random.seed(678)
    return random.random()

def ranuni_789():
    random.seed(789)
    return random.random()

def ranuni_890():
    random.seed(890)
    return random.random()

# Register UDFs
ranuni_567_udf = F.udf(lambda: ranuni_567(), F.DoubleType())
ranuni_678_udf = F.udf(lambda: ranuni_678(), F.DoubleType())
ranuni_789_udf = F.udf(lambda: ranuni_789(), F.DoubleType())
ranuni_890_udf = F.udf(lambda: ranuni_890(), F.DoubleType())

# Apply the CASE logic using when/otherwise
df = df.withColumn(
    "amount",
    F.when(F.col("transaction_type") == "DEPOSIT", 
           F.round(ranuni_567_udf() * F.lit(5000), 2))
     .when(F.col("transaction_type") == "WITHDRAWAL", 
           F.round(ranuni_678_udf() * F.lit(-2000), 2))
     .when(F.col("transaction_type") == "TRANSFER", 
           F.round((ranuni_789_udf() * F.lit(2000)) - F.lit(1000), 2))
     .when(F.col("transaction_type") == "PAYMENT", 
           F.round(ranuni_890_udf() * F.lit(-1000), 2))
     .otherwise(F.lit(0))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assuming we have a DataFrame 'df' with columns 'i', 'amount', 'status_idx'
# and an array 'status_vals' available

# Define window spec for accessing previous row's balance
window_spec = Window.orderBy('row_id').rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Create UDF for status lookup
def get_status(status_vals_broadcast):
    def lookup_status(idx):
        if idx is not None:
            return status_vals_broadcast.value[idx]
        return None
    return F.udf(lookup_status)

# Broadcast the status_vals array to all executors
status_vals_broadcast = spark.sparkContext.broadcast(status_vals)

# Apply the conditional logic and maintain running balance
df_with_balance = df.withColumn(
    'balance',
    F.when(F.col('i') == 1, 1000 + F.col('amount'))
     .otherwise(F.last('balance', ignorenulls=True).over(window_spec) + F.col('amount'))
).withColumn(
    'status',
    get_status(status_vals_broadcast)(F.col('status_idx'))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assuming we have a DataFrame 'df' with columns 'i', 'amount', 'status_idx'
# and an array 'status_vals' available

# Define window spec for accessing previous row's balance
window_spec = Window.orderBy('row_id').rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Create UDF for status lookup
def get_status(status_vals_broadcast):
    def lookup_status(idx):
        if idx is not None:
            return status_vals_broadcast.value[idx]
        return None
    return F.udf(lookup_status)

# Broadcast the status_vals array to all executors
status_vals_broadcast = spark.sparkContext.broadcast(status_vals)

# Apply the conditional logic and maintain running balance
df_with_balance = df.withColumn(
    'balance',
    F.when(F.col('i') == 1, 1000 + F.col('amount'))
     .otherwise(F.last('balance', ignorenulls=True).over(window_spec) + F.col('amount'))
).withColumn(
    'status',
    get_status(status_vals_broadcast)(F.col('status_idx'))
)

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import random

# Since we only have a fragment of SAS code, we need to create a UDF that mimics the CASE logic
# for generating descriptions based on transaction_type

# Create UDFs to simulate the RANUNI functions with seeds
def ranuni_901(seed=901):
    random.seed(seed)
    return random.random()

def ranuni_112(seed=112):
    random.seed(seed)
    return random.random()

def ranuni_223(seed=223):
    random.seed(seed)
    return random.random()

def ranuni_334(seed=334):
    random.seed(seed)
    return random.random()

# Register UDF to generate description based on transaction_type
@F.udf(StringType())
def generate_description(transaction_type):
    if transaction_type == 'DEPOSIT':
        return f"Deposit at branch #{int(ranuni_901() * 50) + 1}"
    elif transaction_type == 'WITHDRAWAL':
        return f"ATM withdrawal #{int(ranuni_112() * 100) + 1}"
    elif transaction_type == 'TRANSFER':
        return f"Transfer with account #{100 + int(ranuni_223() * 100) + 1}"
    elif transaction_type == 'PAYMENT':
        return f"Payment to vendor #{int(ranuni_334() * 200) + 1}"
    else:
        return "Unknown transaction"

# Apply the UDF to the DataFrame
# Assuming the DataFrame is called df and has a column named 'transaction_type'
df = df.withColumn("description", generate_description(F.col("transaction_type")))

# Drop columns that were mentioned in the DROP statement
# Assuming these columns exist in the DataFrame
columns_to_drop = ["i", "cust_idx", "acct_idx", "txn_idx", "status_idx"]
df = df.drop(*columns_to_drop)

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import random

# Since we only have a fragment of SAS code, we need to create a UDF that mimics the CASE logic
# for generating descriptions based on transaction_type

# Create UDFs to simulate the RANUNI functions with seeds
def ranuni_901(seed=901):
    random.seed(seed)
    return random.random()

def ranuni_112(seed=112):
    random.seed(seed)
    return random.random()

def ranuni_223(seed=223):
    random.seed(seed)
    return random.random()

def ranuni_334(seed=334):
    random.seed(seed)
    return random.random()

# Register UDF to generate description based on transaction_type
@F.udf(StringType())
def generate_description(transaction_type):
    if transaction_type == 'DEPOSIT':
        return f"Deposit at branch #{int(ranuni_901() * 50) + 1}"
    elif transaction_type == 'WITHDRAWAL':
        return f"ATM withdrawal #{int(ranuni_112() * 100) + 1}"
    elif transaction_type == 'TRANSFER':
        return f"Transfer with account #{100 + int(ranuni_223() * 100) + 1}"
    elif transaction_type == 'PAYMENT':
        return f"Payment to vendor #{int(ranuni_334() * 200) + 1}"
    else:
        return "Unknown transaction"

# Apply the UDF to the DataFrame
# Assuming the DataFrame is called df and has a column named 'transaction_type'
df = df.withColumn("description", generate_description(F.col("transaction_type")))

# Drop columns that were mentioned in the DROP statement
# Assuming these columns exist in the DataFrame
columns_to_drop = ["i", "cust_idx", "acct_idx", "txn_idx", "status_idx"]
df = df.drop(*columns_to_drop)

from pyspark.sql import SparkSession, functions as F

# Initialize SparkSession
spark = SparkSession.builder.appName("Transaction Processing").getOrCreate()

# Read the input datasets
raw_transactions_df = spark.read.format("your_format").load("path_to_raw_transactions")
generated_transactions_df = spark.read.format("your_format").load("path_to_generated_transactions")

# Union the datasets
transactions_df = raw_transactions_df.union(generated_transactions_df)

# Sort by customer_id to match the SAS BY statement behavior
transactions_df = transactions_df.orderBy("customer_id")

# Write the result
transactions_df.write.format("your_format").mode("overwrite").save("path_to_transactions")

from pyspark.sql import SparkSession, functions as F

# Initialize SparkSession
spark = SparkSession.builder.appName("Transaction Processing").getOrCreate()

# Read the input datasets
raw_transactions_df = spark.read.format("your_format").load("path_to_raw_transactions")
generated_transactions_df = spark.read.format("your_format").load("path_to_generated_transactions")

# Union the datasets
transactions_df = raw_transactions_df.union(generated_transactions_df)

# Sort by customer_id to match the SAS BY statement behavior
transactions_df = transactions_df.orderBy("customer_id")

# Write the result
transactions_df.write.format("your_format").mode("overwrite").save("path_to_transactions")

# Assuming standard_transformations is a SAS macro that needs to be replicated
# We need to create a PySpark function that mimics the behavior of that macro

def standard_transformations(input_df, output_location):
    """
    Performs standard transformations on the input DataFrame and saves the result
    to the specified output location.
    
    Args:
        input_df: Input PySpark DataFrame
        output_location: Path where the transformed data should be saved
    
    Returns:
        Transformed PySpark DataFrame
    """
    # This is a placeholder for the actual transformations that would be in the SAS macro
    # You would need to replace this with the specific transformations from the SAS macro
    
    # Example transformations that might be in a standard pipeline:
    from pyspark.sql import functions as F
    
    transformed_df = input_df
    
    # Common transformations might include:
    # 1. Converting date strings to date type
    # 2. Standardizing case of string columns
    # 3. Handling missing values
    # 4. Deriving new columns
    
    # Save the transformed DataFrame
    transformed_df.write.mode("overwrite").save(output_location)
    
    return transformed_df

# Usage example
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StandardTransformations").getOrCreate()

# Load the input data
transactions_df = spark.read.format("your_input_format").load("path/to/sasdata/transactions")

# Apply transformations and save the result
transformed_df = standard_transformations(
    input_df=transactions_df,
    output_location="path/to/sasdata/transactions_transformed"
)

# Assuming standard_transformations is a SAS macro that needs to be replicated
# We need to create a PySpark function that mimics the behavior of that macro

def standard_transformations(input_df, output_location):
    """
    Performs standard transformations on the input DataFrame and saves the result
    to the specified output location.
    
    Args:
        input_df: Input PySpark DataFrame
        output_location: Path where the transformed data should be saved
    
    Returns:
        Transformed PySpark DataFrame
    """
    # This is a placeholder for the actual transformations that would be in the SAS macro
    # You would need to replace this with the specific transformations from the SAS macro
    
    # Example transformations that might be in a standard pipeline:
    from pyspark.sql import functions as F
    
    transformed_df = input_df
    
    # Common transformations might include:
    # 1. Converting date strings to date type
    # 2. Standardizing case of string columns
    # 3. Handling missing values
    # 4. Deriving new columns
    
    # Save the transformed DataFrame
    transformed_df.write.mode("overwrite").save(output_location)
    
    return transformed_df

# Usage example
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StandardTransformations").getOrCreate()

# Load the input data
transactions_df = spark.read.format("your_input_format").load("path/to/sasdata/transactions")

# Apply transformations and save the result
transformed_df = standard_transformations(
    input_df=transactions_df,
    output_location="path/to/sasdata/transactions_transformed"
)

from pyspark.sql import SparkSession, functions as F

# Initialize Spark session if not already initialized
# spark = SparkSession.builder.appName("Account Processing").getOrCreate()

def create_account_table(spark, account_type):
    """
    Creates a filtered account table based on account type
    """
    # Read data from source tables
    accounts_df = spark.table("sasdata.accounts")
    customers_df = spark.table("sasdata.customers")
    
    # Perform the join and filtering operation
    result_df = (accounts_df
        .join(
            customers_df,
            accounts_df.customer_id == customers_df.customer_id,
            "inner"
        )
        .withColumn(
            "account_status",
            F.when(F.col("close_date").isNull(), "Active").otherwise("Closed")
        )
        .filter(F.upper(F.col("account_type")) == F.upper(F.lit(account_type)))
        .select(
            accounts_df.account_id,
            accounts_df.customer_id,
            accounts_df.account_type,
            accounts_df.open_date,
            accounts_df.close_date,
            accounts_df.initial_balance,
            customers_df.first_name,
            customers_df.last_name,
            customers_df.customer_type,
            customers_df.credit_score,
            F.col("account_status")
        )
        .orderBy("account_id")
    )
    
    # Write the result to a table
    result_df.write.mode("overwrite").saveAsTable(f"work.accounts_{account_type}")
    
    return result_df

# Example usage:
# account_type = "savings"  # This would be passed as a parameter
# result = create_account_table(spark, account_type)

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit

# Unexpected error: Execution approaching Lambda timeout limit