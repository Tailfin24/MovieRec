"""
@file    : dict_generate.py
@function: 生成可用于模型的词典
@version : V1.0
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col
if __name__ == '__main__':
    spark = SparkSession.builder.appName("GenreWordsProcessing").getOrCreate()
    original_dir = "../../public/data/original/"
    processed_dir = "../../public/data/processed/"
    
    movies_df = spark.read.option("multiLine","true").option('escape',"\"").option('header',True).csv(original_dir + "top10K-TMDB-movies.csv", header=True)
    
    ## 类别词典
    genres_split = movies_df.select(explode(split(movies_df['genre'], ',')).alias('genre'))
    unique_genres = genres_split.distinct()
    unique_genres.show(truncate=False)
    genres_list = [row['genre'] for row in unique_genres.collect()]
    print(genres_list)
    # 将词汇表写入文本文件，以便用 Python 读取
    with open(processed_dir + "genre_vocab.txt", "w") as f:
        f.write(",".join(genres_list))
        f.write(",None")
        
    ## 语言词典
    lang_split = movies_df.select("original_language")
    unique_langs = lang_split.distinct()
    unique_langs.show(truncate=False)
    langs_list = [row['original_language'] for row in unique_langs.collect()]
    print(langs_list)
    # 将词汇表写入文本文件，以便用 Python 读取
    with open(processed_dir + "language_vocab.txt", "w") as f:
        f.write(",".join(langs_list))
