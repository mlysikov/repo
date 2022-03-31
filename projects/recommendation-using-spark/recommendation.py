from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.ml.recommendation import ALS
import sys


def start_recommendation():
    spark = SparkSession.builder.appName("Recommendation").getOrCreate()

    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("film_id", IntegerType(), True),
        StructField("rating", IntegerType(), True),
    ])
    ratings = spark.read.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/input/films.csv",
                             header="true",
                             sep=",",
                             inferSchema="false",
                             schema=schema)

    print("Training recommendation model...")

    als = ALS().setMaxIter(5) \
               .setRegParam(0.01) \
               .setUserCol("user_id")\
               .setItemCol("film_id") \
               .setRatingCol("rating")

    model = als.fit(ratings)

    # Manually construct a dataframe of the user ID's we want records for.
    user_id = int(sys.argv[1])
    user_schema = StructType([StructField("user_id", IntegerType(), True)])
    users = spark.createDataFrame([[user_id,]], user_schema)

    recommendations = model.recommendForUserSubset(users, 5).collect()

    print("Top 5 recommendations for user_id =  " + str(user_id))

    for user_rec in recommendations:
        cur_rec = user_rec[1]
        for rec in cur_rec:
            film = rec[0]
            rating = rec[1]
            print("film_id = " + str(film) + " rating = " + str(rating))


if __name__ == '__main__':
    start_recommendation()
