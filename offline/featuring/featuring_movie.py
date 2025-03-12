"""
@file    : featuring_movie.py
@function: 电影数据处理
@version : V1.0
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, split, stddev, count, when, year, month, dayofmonth

if __name__ == '__main__':
    # 初始化SparkSession
    spark = SparkSession.builder.appName("Link Movie Metadata").getOrCreate()

    original_dir = "../../public/data/original/"
    processed_dir = "../../public/data/processed/"

    ## 加载 CSV 文件
    movies_df = spark.read.csv(original_dir + "movies.csv", header=True)
    links_df = spark.read.csv(original_dir + "links.csv", header=True)
    tmdb_df = spark.read.option("multiLine","true").option('escape',"\"").option('header',True).csv(original_dir + 'top10K-TMDB-movies.csv')
    
    ## movies 处理
    
    # tmdb 筛掉标题
    tmdb_df = tmdb_df.drop("title", "overview")
    
    # genres 处理
    movies_df = movies_df.drop("genres")
    tmdb_df = tmdb_df.withColumn("genre_split", split(tmdb_df["genre"], ","))
    # 生成动态列，如果 genre 数量不够，用 None 填充
    for i in range(5):
        tmdb_df = tmdb_df.withColumn(
            f"genre_{i+1}", 
            when(col("genre_split").getItem(i).isNotNull(), col("genre_split").getItem(i)).otherwise("None")
        )    
    tmdb_df = tmdb_df.drop("genre_split")
    
    # 日期处理
    # 提取年份、月份、日期
    tmdb_df = tmdb_df.withColumn('release_year', year(col('release_date'))) \
        .withColumn('release_month', month(col('release_date'))) \
        .withColumn('release_day', dayofmonth(col('release_date')))
    
    # 将 movies_df 和 tmdb_df 根据 movieId 和 tmdbId 连接
    joined_df = movies_df.join(links_df, "movieId")
    joined_df = joined_df.join(tmdb_df, joined_df.tmdbId == tmdb_df.id)
    joined_df = joined_df.drop("id")
    
    # 添加平均评分列
    ratings_df = spark.read.csv(original_dir + "ratings.csv", header=True)
    ratings_df = ratings_df.withColumn("rating", col("rating").cast("float"))
    agg_ratings_df = ratings_df.groupBy("movieId").agg(
        avg("rating").alias("movie_avg_rating"),
        stddev("rating").alias("movie_rating_stddev"),
        count("rating").alias("movie_rating_count"))
    # 修正标准差无法计算的情况
    agg_ratings_df = agg_ratings_df.withColumn(
        "movie_rating_stddev", 
        when(col("movie_rating_count") < 2, 0).otherwise(col("movie_rating_stddev"))
    )
    result_df = joined_df.join(agg_ratings_df, "movieId")
    
    
    result_df.show()
    
    
    # 输出并保存至csv文件中
    result_df.write.mode("overwrite").csv(processed_dir + "movies.csv", header=True)
    