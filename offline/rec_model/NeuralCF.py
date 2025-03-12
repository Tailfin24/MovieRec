"""
@file    : NeuralCF.py
@function: 创建基于 NeuralCF 的模型
@version : V1.0
"""
import tensorflow as tf
from util import load_dataset, split_dataset, save_model

# 定义路径
original_dir = "../../public/data/original/"
processed_dir = "../../public/data/processed/"
model_dir = "../../public/model/"  # 保存的模型路径

def create_model():
    # movieId 特征处理
    movie_col = tf.feature_column.categorical_column_with_identity(key='movieId', num_buckets=200000)
    movie_emb_col = tf.feature_column.embedding_column(movie_col, 10)
    
    # userId 特征处理
    user_col = tf.feature_column.categorical_column_with_identity(key='userId', num_buckets=30001)
    user_emb_col = tf.feature_column.embedding_column(user_col, 10)
    
    inputs = {
        'movieId': tf.keras.layers.Input(name='movieId', shape=(), dtype='int32'),
        'userId': tf.keras.layers.Input(name='userId', shape=(), dtype='int32'),
    }

    item_tower = tf.keras.layers.DenseFeatures([movie_emb_col])(inputs)
    user_tower = tf.keras.layers.DenseFeatures([user_emb_col])(inputs)
    hidden_units = [32, 64, 128]  # 较小的隐藏层数，适合有限硬件
    for num_nodes in hidden_units:
        item_tower = tf.keras.layers.Dense(num_nodes, activation='relu')(item_tower)
        user_tower = tf.keras.layers.Dense(num_nodes, activation='relu')(user_tower)
    
    output = tf.keras.layers.Dot(axes=1)([item_tower, user_tower])    
    output = tf.keras.layers.Dense(1, activation='sigmoid')(output)
    
    model = tf.keras.Model(inputs, output)
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
    save_model(model, model_dir, "NeuralCF")