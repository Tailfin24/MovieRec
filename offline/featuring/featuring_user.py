"""
@file    : featuring_user.py
@function: 用户数据处理
@version : V1.0
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, stddev, count, when, rank, max, coalesce, lit

if __name__ == '__main__':
    # 初始化SparkSession
    spark = SparkSession.builder.appName("Featuring User Metadata").getOrCreate()

    original_dir = "../../public/data/original/"
    processed_dir = "../../public/data/processed/"
    
    # 加载 CSV 文件
    users_df = spark.read.csv(original_dir + "users.dat", sep='::', inferSchema=True)\
        .toDF("userId", "Gender", "Age", "Occupation", "Zip-code")
    movies_df = spark.read.csv(original_dir + "movies.csv", header=True)
    ratings_df = spark.read.csv(original_dir + "ratings.csv", header=True)
    
    
    # 计算每个用户的评分信息
    agg_ratings_df = ratings_df.groupBy('userId').agg(
        avg('rating').alias('user_avg_rating'),
        stddev("rating").alias("user_rating_stddev"),
        count("rating").alias("user_rating_count"))
    
    # 修正标准差无法计算的情况  
    agg_ratings_df = agg_ratings_df.withColumn(
        "user_rating_stddev", 
        when(col("user_rating_count") < 2, 0).otherwise(col("user_rating_stddev"))
    )
    result_df = users_df.join(agg_ratings_df, 'userId')
    
    ## 统计用户最喜欢且最近打分的三部电影
    window_spec = Window.partitionBy("userId").orderBy(col("rating").desc(), col("timestamp").desc())
    ranked_movies = ratings_df.withColumn("rank", rank().over(window_spec))
    fav_movies = ranked_movies.filter(col("rank") <= 3).select(
        "userId",
        when(col("rank") == 1, col("movieId")).alias("fav_movie_1"),
        when(col("rank") == 2, col("movieId")).alias("fav_movie_2"),
        when(col("rank") == 3, col("movieId")).alias("fav_movie_3"),
    )
    fav_movies = fav_movies.groupBy("userId").agg(
        coalesce(max("fav_movie_1"), lit("0")).alias("fav_movie_1"),
        coalesce(max("fav_movie_2"), lit("0")).alias("fav_movie_2"),
        coalesce(max("fav_movie_3"), lit("0")).alias("fav_movie_3"),
    )
    fav_movies.show()
    
    result_df = result_df.join(fav_movies, "userId")
    result_df.show()
    result_df.write.mode("overwrite").csv(processed_dir + "users.csv", header=True)
    
    