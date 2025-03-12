"""
@file    : featuring_movie.py
@function: 将处理过的用户数据和电影数据连接生成训练数据
@version : V1.0
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import when

if __name__ == '__main__':
    # 初始化SparkSession
    spark = SparkSession.builder.appName("Link Ratings").getOrCreate()

    original_dir = "../../public/data/original/"
    processed_dir = "../../public/data/processed/"
    
    movies_df = spark.read.csv(processed_dir + "movies.csv", header=True)
    users_df = spark.read.csv(processed_dir + "users.csv", header=True)
    ratings_df = spark.read.csv(original_dir + "ratings.csv", header=True)
    
    # 将 ratings 与 users 和 movies连接
    ratings_df = ratings_df.join(users_df, "userId")
    ratings_df = ratings_df.join(movies_df, "movieId")
    
    # 评定标签
    ratings_df = ratings_df.withColumn("label", when(ratings_df["rating"] > 3, 1).otherwise(0))

    # 显示结果
    ratings_df.show()
    ratings_df.write.mode('overwrite').csv(processed_dir + "ratings.csv", header=True)