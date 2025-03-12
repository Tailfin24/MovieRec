"""
@file    : util.py
@author  : caudalfin24
@function: 训练模型中的常用函数
@version : V1.0
"""
import tensorflow as tf
import os

def load_dataset(filepath):
    """从文件路径中读取数据集.
    
    Args:
        filepath : 文件路径.
        label    : 将该列作为标签列.
    Returns:
        读入的数据集.
    """
    dataset = tf.data.experimental.make_csv_dataset(
        filepath,
        batch_size=32,
        label_name="label",
        num_epochs=1,
        na_value="0",
        shuffle=True,
        ignore_errors=True
    )
    return dataset

def split_dataset(dataset, train_size=0.8):
    """将数据集划分为训练集与测试集.
    
    Args:
        dataset       : 输入数据集.
        train_size    : 训练集占总比例的大小.
    Returns:
        [train_dataset, test_dataset]
    """
    total_samples = sum(1 for _ in dataset)
    train_samples = int(train_size * total_samples)
    
    train_dataset = dataset.take(train_samples)
    test_dataset = dataset.skip(train_samples)
    
    return train_dataset, test_dataset

def save_model(model, model_dir, model_name="model"):
    """保存模型，若存在则覆盖"""
    model_path = os.path.join(model_dir, model_name)
    
    # 如果目录不存在，创建目录
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)
    
    # 保存模型，若模型文件已存在则覆盖
    model.save(model_path, overwrite=True)
    print(f"模型已保存到：{model_path}")