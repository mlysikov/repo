from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType
import constant
import db_config
import time
#import hashlib


# At first, I decided to define a UDF to calculate a hash value, but after testing it turned out that this function
# works slowly compared to Spark SQL built-in functions.
# Using a UDF function: Reconciliation completed in 39.3246641159 seconds
# Using Spark SQL built-in functions: Reconciliation completed in 7.98040294647 seconds (almost five times faster!)
# UDF's are a black box to Spark hence it can't apply optimization, and we will lose all the optimization Spark does on
# Dataframe/Dataset. It's better to use Spark SQL built-in functions as these functions provide optimization.
# def calc_hash_value(p_id,
#                     p_client_id,
#                     p_account_number,
#                     p_transaction_date,
#                     p_amount):
#     att_list = [str(p_id), str(p_client_id), p_account_number, p_transaction_date.strftime('%Y-%m-%d'), str(p_amount)]
#     separator = '|'
#
#     return hashlib.sha1(separator.join(att_list).encode('utf-8')).hexdigest()

# Register the function as a UDF.
# calc_hash_value_udf = func.udf(calc_hash_value)

# Returns a Dataframe from the database table.
def adapter_db():
    spark = SparkSession.builder.appName("Reconciliation").getOrCreate()

    df_db1 = spark.read.format("jdbc") \
        .option("url", db_config.url) \
        .option("driver", db_config.driver) \
        .option("dbtable", db_config.dbtable) \
        .option("user", db_config.user) \
        .option("password", db_config.pw) \
        .load()
    # df_db2 = df_db1.withColumn("hash_val", calc_hash_value_udf(func.col("client_id"),
    #                                                            func.col("account_number"),
    #                                                            func.col("transaction_date"),
    #                                                            func.col("amount")))
    df_db2 = df_db1.withColumn("hash_val", func.sha1(func.concat_ws("|", func.col("client_id"),
                                                                         func.col("account_number"),
                                                                         func.date_format(func.col("transaction_date"),"yyyy-MM-dd"),
                                                                         func.col("amount"))))

    return df_db2

# Returns a Dataframe from the CSV file.
def adapter_file():
    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("account_number", StringType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("amount", DecimalType(10, 4), True)
    ])
    df_file1 = spark.read.csv(constant.path_to_csv_file,
                              header="true",
                              sep=",",
                              inferSchema="false",
                              schema=schema,
                              dateFormat="yyyy-MM-dd")
    # df_file2 = df_file1.withColumn("hash_val", calc_hash_value_udf(func.col("client_id"),
    #                                                                func.col("account_number"),
    #                                                                func.col("transaction_date"),
    #                                                                func.col("amount")))
    df_file2 = df_file1.withColumn("hash_val", func.sha1(func.concat_ws("|", func.col("client_id"),
                                                                             func.col("account_number"),
                                                                             func.date_format(func.col("transaction_date"),"yyyy-MM-dd"),
                                                                             func.col("amount"))))

    return df_file2

# A key part of the reconciliation process.
# Data is considered reconciled if:
#   1. A match on ID + HASH_VAL.
#   2. A match on ID + CLIENT_ID + ACCOUNT_NUMBER + TRANSACTION_DATE + Tolerance.
# Tolerance is computed by the formula ( ( N2 - N1 ) / N2 ) * 100, where:
#   N2 - is the higher value of N2, N1.
#   N1 - is the smaller value of N2, N1.
def reconciliation(p_tolerance=None):
    df_db = adapter_db()
    df_file = adapter_file()

    df_join1 = df_db.join(df_file, (df_file.id == df_file.id) &
                                   (df_db.hash_val == df_file.hash_val), how="inner").select(df_db.id,
                                                                                             df_db.client_id,
                                                                                             df_db.account_number,
                                                                                             df_db.transaction_date,
                                                                                             df_db.amount)

    df_join2 = df_db.join(df_file, (df_file.id == df_file.id) &
                                   (df_db.client_id == df_file.client_id) &
                                   (df_db.account_number == df_file.account_number) &
                                   (df_db.transaction_date == df_file.transaction_date) &
                                   (df_db.hash_val != df_file.hash_val), how="inner"). \
                     filter(((func.greatest(df_db.amount, df_file.amount) - func.least(df_db.amount, df_file.amount)) /
                              func.greatest(df_db.amount, df_file.amount)) * 100 <= p_tolerance).select(df_db.id,
                                                                                                        df_db.client_id,
                                                                                                        df_db.account_number,
                                                                                                        df_db.transaction_date,
                                                                                                        df_db.amount)

    df_union = df_join1.union(df_join2).distinct()

    # An alternative option using a SQL query.
    # df_db.createOrReplaceTempView("trans_db")
    # df_file.createOrReplaceTempView("trans_csv")
    # df_union = spark.sql("""select x.*
    #                         from   (select t1.id
    #                                       ,t1.client_id
    #                                       ,t1.account_number
    #                                       ,t1.transaction_date
    #                                       ,t1.amount
    #                                 from   trans_db t1
    #                                 join   trans_csv t2
    #                                 on     t1.id = t2.id
    #                                        and t1.hash_val = t2.hash_val
    #                                 union
    #                                 select t1.id
    #                                       ,t1.client_id
    #                                       ,t1.account_number
    #                                       ,t1.transaction_date
    #                                       ,t1.amount
    #                                 from   trans_db t1
    #                                 join   trans_csv t2
    #                                 on     t1.id = t2.id
    #                                        and t1.hash_val != t2.hash_val
    #                                        and t1.client_id = t2.client_id
    #                                        and t1.account_number = t2.account_number
    #                                        and t1.transaction_date = t2.transaction_date
    #                                        and ( ( greatest(t1.amount, t2.amount) - least(t1.amount, t2.amount)) / greatest(t1.amount, t2.amount ) ) * 100 <= {}) x""".format(p_tolerance))

    return df_union


# Start the reconciliation process.
def start_reconciliation():
    start = time.time()
    df_recon = reconciliation(constant.tolerance_value)
    elapsed = (time.time() - start)
    print('Reconciliation completed in ' + str(elapsed) + " seconds")

    # Write the dataframe containing reconciled data to a Parquet file (overwrite if it already exists).
    df_recon.write.mode("overwrite").parquet(constant.path_to_parquet_file)


if __name__ == '__main__':
    start_reconciliation()
