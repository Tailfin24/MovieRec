"""
@file    : tmdb_img.py
@function: 获取 tmdb 图片链接
@version : V1.0
"""

import requests
import csv
import os

TMDB_BASE_URL = "https://api.themoviedb.org/3"
INPUT_CSV = "../../public/data/original/links.csv"

OUTPUT_CSV = "../../public/data/processed/posters.csv"  # 输出文件路径
TMDB_API_KEY = "c2eb85c0979041d7587dcd2630a9a935"

# 定义一个 Python 函数来获取 poster_path
def get_movie_image(tmdb_id):
    """根据 TMDB ID 获取电影的图片链接"""
    url = f"{TMDB_BASE_URL}/movie/{tmdb_id}"
    params = {"api_key": TMDB_API_KEY}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        poster_path = data.get("poster_path")
        return poster_path
    return None

def process_csv(input_csv, output_csv):
    """
    读取 CSV 文件中的 TMDB ID 列，根据 TMDB API 获取封面图片链接，并保存为新的 CSV。
    支持中断后继续处理未完成的部分。
    :param input_csv: 输入的 CSV 文件路径
    :param output_csv: 输出的 CSV 文件路径
    :param api_key: TMDB API 密钥
    """
    # 加载已处理数据
    processed_tmdb_ids = set()
    if os.path.exists(output_csv):
        with open(output_csv, mode='r', encoding='utf-8') as outfile:
            reader = csv.DictReader(outfile)
            processed_tmdb_ids = {row['tmdbId'] for row in reader if row.get('tmdbId')}

    with open(input_csv, mode='r', encoding='utf-8') as infile, \
         open(output_csv, mode='a', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['PosterURL']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        # 如果是新文件，写入表头
        if not os.path.exists(output_csv) or os.stat(output_csv).st_size == 0:
            writer.writeheader()

        for row in reader:
            tmdb_id = row.get('tmdbId')
            if tmdb_id in processed_tmdb_ids:
                # print(f"Skipping already processed TMDB ID: {tmdb_id}")
                continue

            if tmdb_id:
                poster_url = get_movie_image(tmdb_id)
                row['PosterURL'] = poster_url
            else:
                continue

            writer.writerow(row)
            print(f"Processed TMDB ID: {tmdb_id} -> {row['PosterURL']}")

if __name__ == "__main__":

    process_csv(INPUT_CSV, OUTPUT_CSV)
    print(f"处理完成，结果保存在 {OUTPUT_CSV}")


