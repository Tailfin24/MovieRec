"""
@file    : WidenDeep.py
@function: 创建基于 Wide & Deep的推荐模型
@version : V1.0
"""

import tensorflow as tf
from util import load_dataset, split_dataset, save_model

# 定义路径
original_dir = "../../public/data/original/"
processed_dir = "../../public/data/processed/"
model_dir = "../../public/model/"  # 保存的模型路径

def input_layer():
    inputs = {
        'user_avg_rating': tf.keras.layers.Input(name='user_avg_rating', shape=(), dtype='float32'),
        'user_rating_stddev': tf.keras.layers.Input(name='user_rating_stddev', shape=(), dtype='float32'),
        'vote_average': tf.keras.layers.Input(name='vote_average', shape=(), dtype='float32'),
        'popularity': tf.keras.layers.Input(name='popularity', shape=(), dtype='float32'),
        'movie_avg_rating': tf.keras.layers.Input(name='movie_avg_rating', shape=(), dtype='float32'),
        'movie_rating_stddev': tf.keras.layers.Input(name='movie_rating_stddev', shape=(), dtype='float32'),
        
        'user_rating_count': tf.keras.layers.Input(name='user_rating_count', shape=(), dtype='int32'),
        'vote_count': tf.keras.layers.Input(name='vote_count', shape=(), dtype='int32'),
        'release_year': tf.keras.layers.Input(name='release_year', shape=(), dtype='int32'),
        'release_month': tf.keras.layers.Input(name='release_month', shape=(), dtype='int32'),
        'release_day': tf.keras.layers.Input(name='release_day', shape=(), dtype='int32'),
        'Age': tf.keras.layers.Input(name='Age', shape=(), dtype='int32'),
        'movie_rating_count': tf.keras.layers.Input(name='movie_rating_count', shape=(), dtype='int32'),
        'fav_movie_1': tf.keras.layers.Input(name='fav_movie_1', shape=(), dtype='int32'),
        'fav_movie_2': tf.keras.layers.Input(name='fav_movie_2', shape=(), dtype='int32'),
        'fav_movie_3': tf.keras.layers.Input(name='fav_movie_3', shape=(), dtype='int32'),
        
        'movieId': tf.keras.layers.Input(name='movieId', shape=(), dtype='int32'),
        'userId': tf.keras.layers.Input(name='userId', shape=(), dtype='int32'),
        'Occupation': tf.keras.layers.Input(name='Occupation', shape=(), dtype='int32'),

        'Gender': tf.keras.layers.Input(name='Gender', shape=(), dtype='string'),
        'original_language': tf.keras.layers.Input(name='original_language', shape=(), dtype='string'),
        'genre_1': tf.keras.layers.Input(name='genre_1', shape=(), dtype='string'),
        'genre_2': tf.keras.layers.Input(name='genre_2', shape=(), dtype='string'),
        'genre_3': tf.keras.layers.Input(name='genre_3', shape=(), dtype='string'),
        'genre_4': tf.keras.layers.Input(name='genre_4', shape=(), dtype='string'),
        'genre_5': tf.keras.layers.Input(name='genre_5', shape=(), dtype='string'),
        
    }
    return inputs

def embedding():
    ### 定义嵌入列
    ## 类别特征处理
    categorical_columns = []
    
    # movieId 特征处理
    movie_col = tf.feature_column.categorical_column_with_identity(key='movieId', num_buckets=200000)
    movie_emb_col = tf.feature_column.embedding_column(movie_col, 10)
    categorical_columns.append(movie_emb_col)
    
    # userId 特征处理
    user_col = tf.feature_column.categorical_column_with_identity(key='userId', num_buckets=30001)
    user_emb_col = tf.feature_column.embedding_column(user_col, 10)
    categorical_columns.append(user_emb_col)
    
    # 字符串类别特征处理
    lang_vocab = []
    with open(processed_dir + "language_vocab.txt", "r") as f:
        lang_vocab = f.read().split(",")
    genre_vocab = []
    with open(processed_dir + "genre_vocab.txt", "r") as f:
        genre_vocab = f.read().split(",")
    
    keys = ['Gender', 'original_language', 'genre_1', 'genre_2', 'genre_3', 'genre_4', 'genre_5']
    vocabs = {
        'Gender': ["M", "F"],
        'original_language': lang_vocab,
        'genre_1': genre_vocab,
        'genre_2': genre_vocab,
        'genre_3': genre_vocab,
        'genre_4': genre_vocab, 
        'genre_5': genre_vocab
    }
    
    for key in keys:
        input_col = tf.feature_column.categorical_column_with_vocabulary_list(
            key=key, vocabulary_list=vocabs[key]
        )
        emb_col = tf.feature_column.embedding_column(input_col, 1)
        categorical_columns.append(emb_col)
            
    # Occupation 特征处理
    occupation_col = tf.feature_column.categorical_column_with_identity(key='Occupation', num_buckets=21)
    occupation_emb_col = tf.feature_column.embedding_column(occupation_col, 2)
    categorical_columns.append(occupation_emb_col)
       
    ## 数值特征处理
    numeral_columns = [
                       tf.feature_column.numeric_column('Age'),
                       tf.feature_column.numeric_column('user_avg_rating'),
                       tf.feature_column.numeric_column('user_rating_stddev'),
                       tf.feature_column.numeric_column('user_rating_count'),
                       tf.feature_column.numeric_column('movie_avg_rating'),
                       tf.feature_column.numeric_column('movie_rating_stddev'),
                       tf.feature_column.numeric_column('movie_rating_count'),
                       tf.feature_column.numeric_column('popularity'),
                       tf.feature_column.numeric_column('vote_average'),
                       tf.feature_column.numeric_column('vote_count'),
                       tf.feature_column.numeric_column('release_year'),
                       tf.feature_column.numeric_column('release_month'),
                       tf.feature_column.numeric_column('release_day'),
                       ]
    
    embedding_columns = categorical_columns + numeral_columns
    return embedding_columns

def create_model():
    """创建预处理模型
    """
    inputs = input_layer()
    embedding_columns = embedding()
    
    ## 定义Deep层
    deep = tf.keras.layers.DenseFeatures(embedding_columns)(inputs)
    deep = tf.keras.layers.Dense(256, activation='relu')(deep)
    deep = tf.keras.layers.Dense(128, activation='relu')(deep)
    deep = tf.keras.layers.Dense(64, activation='relu')(deep)
    
    ## 定义Wide层
    movie_col = tf.feature_column.categorical_column_with_identity(key='movieId', num_buckets=200000)
    fav_movie_1 = tf.feature_column.categorical_column_with_identity(key='fav_movie_1', num_buckets=200000)
    fav_movie_2 = tf.feature_column.categorical_column_with_identity(key='fav_movie_2', num_buckets=200000)
    fav_movie_3 = tf.feature_column.categorical_column_with_identity(key='fav_movie_3', num_buckets=200000)
    crossed1 = tf.feature_column.indicator_column(tf.feature_column.crossed_column([movie_col, fav_movie_1], 10000))
    wide1 = tf.keras.layers.DenseFeatures(crossed1)(inputs)
    crossed2 = tf.feature_column.indicator_column(tf.feature_column.crossed_column([movie_col, fav_movie_2], 10000))
    wide2 = tf.keras.layers.DenseFeatures(crossed2)(inputs)
    crossed3 = tf.feature_column.indicator_column(tf.feature_column.crossed_column([movie_col, fav_movie_3], 10000))
    wide3 = tf.keras.layers.DenseFeatures(crossed3)(inputs)
    both = tf.keras.layers.concatenate([deep, wide1, wide2, wide3])
    output_layer = tf.keras.layers.Dense(1, activation='sigmoid')(both)
    model = tf.keras.Model(inputs, output_layer)
    return model
    


if __name__ == "__main__":
    # 确认 GPU 是否可用
    print("Num GPUs Available: ", len(tf.config.experimental.list_physical_devices('GPU')))
    # 加载数据集
    dataset = load_dataset(processed_dir + "ratings.csv")
    
    for batch, label in dataset.take(1):
        print(batch)
        print(label)
        
    # 划分数据集
    train_dataset, test_dataset = split_dataset(dataset)
    
    # 模型构建
    model = create_model()
    
    # 编译模型
    model.compile(
        loss='binary_crossentropy',
        optimizer='adam',
        metrics=['accuracy', tf.keras.metrics.AUC(curve='ROC'), tf.keras.metrics.AUC(curve='PR')])

    # 训练模型
    model.fit(train_dataset, epochs=20)
    
    # 测试模型
    test_loss, test_accuracy, test_roc_auc, test_pr_auc = model.evaluate(test_dataset)
    print('\n\nTest Loss {}, Test Accuracy {}, Test ROC AUC {}, Test PR AUC {}'.format(test_loss, test_accuracy,
                                                                                    test_roc_auc, test_pr_auc))
    # 预测模型
    predictions = model.predict(test_dataset)
    for prediction, goodRating in zip(predictions[:12], list(test_dataset)[0][1][:12]):
        print("Predicted good rating: {:.2%}".format(prediction[0]),
            " | Actual rating label: ",
            ("Good Rating" if bool(goodRating) else "Bad Rating"))
    
    # 保存模型
    save_model(model, model_dir, "WidenDeep")