"""
@file    : data_manager.py
@function: 基于 Redis 的电影和用户数据管理服务
@version : V1.0
"""

import redis
import csv

class DataManager:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis = redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)
        self.user_id_next = 0
        
    # 电影数据存储与查询
    def load_movie(self, movie_id, movie_data):
        """将电影数据保存到 Redis 服务器中

        Args:
            movie_id   : 电影 ID
            movie_data : 电影数据 
        """
        key = f"movie:{movie_id}"
        self.redis.hset(key, mapping=movie_data)

    def get_movie(self, movie_id):
        """根据电影ID从Redis中获取电影数据

        Args:
            movie_id   : 电影 ID
        Returns:
            电影数据
        """
        key = f"movie:{movie_id}"
        movie_data = self.redis.hgetall(key)
        return movie_data
    
    def load_movie_from_csv(self, filepath):
        """从csv文件中读入电影数据至Redis

        Args:
            filepath: csv 文件路径
        """
        print("Loading Movie Data...")
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            count = 0
            for row in reader:
                movie_id = row.get("movieId")
                row["movie_avg_rating"] = '{:.4f}'.format(float(row["movie_avg_rating"]))
                if movie_id:
                    self.load_movie(movie_id, row)
                    count += 1
                else:
                    print(f"Missing movie_id in row: {row}");
            print(f"Movie data: {count} rows has loaded successfully.")
            
    def get_all_movies(self):
        """从 Redis 中获取所有电影数据
        """
        # 使用 Redis 的 scan_iter 方法遍历所有电影的 key
        movie_keys = self.redis.scan_iter("movie:*", count = 1000)
        print(movie_keys)
        all_movies = []
        # 使用 pipeline 批量获取所有 key 的数据
        with self.redis.pipeline() as pipe:
            for key in movie_keys:
                pipe.hgetall(key)
            results = pipe.execute()
        
        for movie_data in results:
            all_movies.append(movie_data)
        return all_movies
    # 用户数据存储与查询
    def load_user(self, user_id, user_data):
        """将用户数据保存到 Redis 服务器中

        Args:
            user_id   : 用户 ID
            user_data : 用户数据 
        """
        key = f"user:{user_id}"
        self.user_id_next = max(self.user_id_next, int(user_id)+1)
        self.redis.hset(key, mapping=user_data)

    def get_user(self, user_id):
        """根据用户ID从Redis中获取用户数据

        Args:
            user_id   : 用户 ID
        Returns:
            用户数据
        """
        key = f"user:{user_id}"
        user_data = self.redis.hgetall(key)
        return user_data
        
    
    def load_user_from_csv(self, filepath):
        """从csv文件中读入用户数据至Redis

        Args:
            filepath: csv 文件路径
        """
        print("Loading User Data...")
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            count = 0
            for row in reader:
                user_id = row.get("userId")
                if user_id:
                    self.load_user(user_id, row)
                    count += 1
                else:
                    print(f"Missing user_id in row: {row}");
            print(f"User data: {count} rows has loaded successfully.")   
    
    # 评分数据存储与查询
    def load_rating(self, user_id, movie_id, rating_data):
        """将评分数据保存到 Redis 服务器中

        Args:
            user_id   : 用户 ID
            movie_id   : 电影 ID
            rating_data : 评分数据 
        Returns:
            评分数据
        """
        key = f"rating:user:{user_id}:movie:{movie_id}"
        self.redis.hset(key, mapping=rating_data)

    def get_ratings(self, user_id):
        """根据用户ID从Redis中获取所有评分数据

        Args:
            user_id   : 用户 ID
        """
        pattern = f"rating:user:{user_id}:movie:*"
        keys = self.redis.keys(pattern)
        ratings = []
        
        # 使用 pipeline 批量获取所有 key 的数据
        with self.redis.pipeline() as pipe:
            for key in keys:
                pipe.hgetall(key)
            results = pipe.execute()

        for rating_data in results:
            ratings.append(rating_data)
        return ratings
        
    
    def load_ratings_from_csv(self, filepath):
        """从csv文件中读入用户数据至Redis

        Args:
            filepath: csv 文件路径
        """
        print("Loading Rating Data...")
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            count = 0
            for row in reader:
                user_id = row.get("userId")
                movie_id = row.get("movieId")
                if user_id and movie_id:
                    self.load_rating(user_id, movie_id, row)
                    count += 1
                else:
                    print(f"Missing rating in row: {row}");
            print(f"Rating: {count} rows has loaded successfully.")    
        
    def load_poster_from_csv(self, filepath):
        """从csv文件中读入电影海报链接数据至Redis

        Args:
            filepath: csv 文件路径
        """
        print("Loading Rating Data...")
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            count = 0
            for row in reader:
                movie_id = row.get("movieId")
                poster_url = row.get("PosterURL")
                if movie_id:
                    key = f"poster:{movie_id}"
                    self.redis.set(key, poster_url)
                    count += 1
                else:
                    print(f"Missing URL in row: {row}");
            print(f"PosterURL: {count} rows has loaded successfully.")  
    
    def get_poster_url(self, movie_id, poster_size="w92"):
        """根据电影ID从Redis中获取链接

        Args:
            movie_id   : 电影 ID
            poster_size: 电影图片大小. 可选项: [
                            "w92", "w154", "w185", "w342", "w500", "w780", "original"],
        """
        key = f"poster:{movie_id}"
        poster_path = self.redis.get(key)
        if poster_path and not poster_path == "None":
            return f"https://image.tmdb.org/t/p/{poster_size}{poster_path}"
        else:
            return f"static/images/picerror.png"
    
    def new_profile(self, username, user_profile):
        """新建一个用户档案

        Args:
            username        : 用户名
            user_profile    : 用户档案
        """
        key = f"profile:{username}"
        self.redis.hset(key, mapping=user_profile)
        
    def get_profile(self, username):
        """获取一个用户档案，若不存在，返回 None

        Args:
            username        : 用户名
        """    
        key = f"profile:{username}"    
        result = self.redis.hgetall(key)
        return result
    
# 使用测试
if __name__ == "__main__":
    dm = DataManager()

    path = "../public/data/original/ratings.csv"
    dm.load_ratings_from_csv(path)
    user_ratings = dm.get_ratings(1)
    print(user_ratings)
