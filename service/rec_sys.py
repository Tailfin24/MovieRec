"""
@file    : rec_sys.py
@function: 线上推荐系统服务
@version : V1.0
"""

import tensorflow as tf
from .data_manager import DataManager

recall_path = "public/model/NeuralCF"
sort_path = "public/model/WidenDeep"
movies_path = "public/data/processed/movies.csv"
users_path = "public/data/processed/users.csv"
ratings_path = "public/data/original/ratings.csv"
posters_path = "public/data/processed/posters.csv"

class RecSys:
    def __init__(self):
        self.recall_model = tf.keras.models.load_model(recall_path)
        self.sort_model = tf.keras.models.load_model(sort_path)
        print("Model Loaded Success.")
        self.dm = DataManager()
        self.dm.load_movie_from_csv(movies_path)
        self.dm.load_user_from_csv(users_path)
        self.dm.load_ratings_from_csv(ratings_path)
        self.dm.load_poster_from_csv(posters_path)
        self.PREDICT_BATCH = 128
        self.FIT_BATCH = 10      
        
    def predict_test(self):
        input_data = {
            "movieId": [1000],
            "userId": [1],
            "Gender": ["F"],
            "Age": [25],
            "Occupation": [12],
            "user_avg_rating": [3.5],
            "user_rating_stddev": [0.76],
            "user_rating_count": [273],
            "original_language": ["en"],
            "vote_average": [7.2],
            "vote_count": [9088],
            "genre_1": ["Adventure"],
            "genre_2": ["Fantasy"],
            "genre_3": ["Family"],
            "genre_4": ["None"],
            "genre_5": ["None"],
            "popularity": [200],
            "release_year": [1995],
            "release_month": [12],
            "release_day": [15],
            "movie_avg_rating": [3.4],
            "movie_rating_stddev": [0.88],
            "movie_rating_count": [110]
        }

        input_dataset = tf.data.Dataset.from_tensor_slices(input_data)
        input_dataset = input_dataset.batch(1)        

        output_data = [self.recall_model.predict(input_dataset), self.sort_model.predict(input_dataset)]
        

        # 输出预测结果
        print(output_data)
    
    def recall_generate_input(self, userData, moviesData, batch=128):
        """对给定的 userData 和 moviesData 生成正确的输入数据集

        Args:
            userData       : 用户数据
            movieData      : 电影数据
        Returns:
            生成的数据集.
        """
        input_data = { "movieId": [], "userId": []}
        
        for movieData in moviesData:
            input_data["movieId"].append(int(movieData["movieId"]))
            input_data["userId"].append(int(userData["userId"]))
            
        input_dataset = tf.data.Dataset.from_tensor_slices(input_data)
        input_dataset = input_dataset.batch(batch) 
        
        return input_dataset  
      
    def sort_generate_input(self, userData, moviesData, batch=128):
        """对给定的 userData 和 moviesData 生成正确的输入数据集

        Args:
            userData       : 用户数据
            movieData      : 电影数据
        Returns:
            生成的数据集.
        """
        input_data = { "movieId": [], "userId": [], "Gender": [], "Age": [], "Occupation": [], "user_avg_rating": [],"user_rating_stddev": [],
            "user_rating_count": [], "original_language": [], "vote_average": [], "vote_count": [],
            "genre_1": [], "genre_2": [], "genre_3": [], "genre_4": [], "genre_5": [], "popularity": [], "release_year": [],
            "release_month": [], "release_day": [], "movie_avg_rating": [], "movie_rating_stddev": [],"movie_rating_count": [],
            "fav_movie_1":[], "fav_movie_2":[], "fav_movie_3":[]}
        
        for movieData in moviesData:
            input_data["movieId"].append(int(movieData["movieId"]))
            input_data["userId"].append(int(userData["userId"]))
            input_data["Gender"].append(userData["Gender"])
            input_data["Age"].append(int(userData["Age"]))
            input_data["Occupation"].append(int(userData["Occupation"]))
            input_data["user_avg_rating"].append(float(userData["user_avg_rating"]))
            input_data["user_rating_stddev"].append(float(userData["user_rating_stddev"]))
            input_data["user_rating_count"].append(int(userData["user_rating_count"]))
            input_data["original_language"].append(movieData["original_language"])
            input_data["vote_average"].append(float(movieData["vote_average"]))
            input_data["vote_count"].append(int(movieData["vote_count"]))
            input_data["genre_1"].append(movieData["genre_1"])
            input_data["genre_2"].append(movieData["genre_2"])
            input_data["genre_3"].append(movieData["genre_3"])
            input_data["genre_4"].append(movieData["genre_4"])
            input_data["genre_5"].append(movieData["genre_5"])
            input_data["popularity"].append(float(movieData["popularity"]))
            input_data["release_year"].append(int(movieData["release_year"]))
            input_data["release_month"].append(int(movieData["release_month"]))
            input_data["release_day"].append(int(movieData["release_day"]))
            input_data["movie_avg_rating"].append(float(movieData["movie_avg_rating"]))
            input_data["movie_rating_stddev"].append(float(movieData["movie_rating_stddev"]))
            input_data["movie_rating_count"].append(float(movieData["movie_rating_count"]))
            input_data["fav_movie_1"].append(int(userData["fav_movie_1"]))
            input_data["fav_movie_2"].append(int(userData["fav_movie_2"]))
            input_data["fav_movie_3"].append(int(userData["fav_movie_3"]))
            
        
        input_dataset = tf.data.Dataset.from_tensor_slices(input_data)
        input_dataset = input_dataset.batch(batch) 
        
        return input_dataset
    
    
    def recall(self, userId, num=100):
        """对给定的 userId 进行物品召回处理

        Args:
            userId       : 用户 ID
            num          : 召回物品列表的物品数
        Returns:
            召回的物品id列表.
        """
        userData = self.dm.get_user(userId)
        moviesData = self.dm.get_all_movies()
        input_dataset = self.recall_generate_input(userData, moviesData, self.PREDICT_BATCH)
        outputs = self.recall_model.predict(input_dataset)
        
        # 结果排序
        combined = sorted(zip(moviesData, outputs), key=lambda x: x[1][0], reverse=True)
        top_movieIds = [movie[0]['movieId'] for movie in combined[:num]]
        # print(top_movieIds)
        return top_movieIds
    
    def sort(self, userId, recallList, num=10):
        """对给定的 userId, 以及召回的物品进行物品排序处理

        Args:
            userId       : 用户 ID
            recallList   : 通过召回层召回的电影ID
            num          : 排序列表的物品数
        Returns:
            排序的物品列表.
        """
        userData = self.dm.get_user(userId)
        moviesData = []
        for movieId in recallList:
            moviesData.append(self.dm.get_movie(movieId))
            
        input_dataset = self.sort_generate_input(userData, moviesData, self.PREDICT_BATCH)
        outputs = self.sort_model.predict(input_dataset)
        # print(outputs)
        # 结果排序
        combined = sorted(zip(moviesData, outputs), key=lambda x: x[1][0], reverse=True)
        top_movieIds = [movie[0]['movieId'] for movie in combined[:num]]
        print(top_movieIds)
        return top_movieIds
    
    def recommend(self, userId, num=10):
        """对给定的 userId, 推荐一定数量的电影

        Args:
            userId       : 用户 ID
            recallList   : 通过召回层召回的电影ID
        Returns:
            推荐的电影列表.
        """
        recall_list = self.recall(userId, num*10);
        sort_list = self.sort(userId, recall_list, num);
        movies = []
        for movieId in sort_list:
            movies.append(self.dm.get_movie(movieId))
        return movies
    
    def new_rating(self, userId, rating):
        """针对新输入的一个评分数据，更新数据库
        Args:
            userId       : 用户 ID
            rating       : 评分
        """
        movieId = rating["movieId"]
        # 推送新的评分数据
        self.dm.load_rating(userId, movieId, rating)
        
        # 更新电影数据
        movieData = self.dm.get_movie(movieId)
        rating_sum = int(movieData["movie_rating_count"]) * float(movieData["movie_avg_rating"])
        movieData["movie_rating_count"] = str(int(movieData["movie_rating_count"]) + 1)
        rating_sum += float(rating["rating"])
        movieData["movie_avg_rating"] = '{:.4f}'.format(rating_sum / int(movieData["movie_rating_count"]))
        self.dm.load_movie(movieId, movieData)
        
        # 更新用户数据
        userData = self.dm.get_user(userId)
        rating_sum = float(userData["user_avg_rating"]) * int(userData["user_rating_count"])
        userData["user_rating_count"] = str(int(userData["user_rating_count"]) + 1)
        rating_sum += float(rating["rating"])
        userData["user_avg_rating"] = str(rating_sum / int(userData["user_rating_count"]))
        userRatings = self.dm.get_ratings(userId)
        sorted_ratings = sorted(userRatings, key=lambda x: (x["rating"], x["timestamp"]), reverse=True)
        if (len(sorted_ratings) > 0):
            userData["fav_movie_1"] = sorted_ratings[0]["movieId"]
        if (len(sorted_ratings) > 1):
            userData["fav_movie_2"] = sorted_ratings[1]["movieId"]
        if (len(sorted_ratings) > 2):
            userData["fav_movie_3"] = sorted_ratings[2]["movieId"]
        self.dm.load_user(userId, userData)
        

        
    def new_ratings(self, userId, ratings):
        """针对新输入的评分数据，更新数据库并调整模型
        Args:
            userId       : 用户 ID
            movieId      : 电影 ID
            rating       : 评分
        """
        movieDatas = [] 
        labels = []     
        userData = self.dm.get_user(userId)
        for rating in ratings:
            self.new_rating(userId, rating)  
            # 模型数据
            movieDatas.append(self.dm.get_movie(rating["movieId"]))
            label = 1 if float(rating["rating"]) > 3 else 0
            labels.append(label)
        # 调整模型
        recallInputData = self.recall_generate_input(userData, movieDatas, self.FIT_BATCH)
        recallDataset = recallInputData.map(lambda x: (x, tf.convert_to_tensor(labels)))
        self.recall_model.fit(recallDataset)
        sortInputData = self.sort_generate_input(userData, movieDatas, self.FIT_BATCH)
        sortDataset = sortInputData.map(lambda x: (x, tf.convert_to_tensor(labels)))
        self.sort_model.fit(sortDataset)
        
# 使用测试        
if __name__ == "__main__":
    recall_model_path = "../public/model/NeuralCF"
    sort_model_path = "../public/model/WidenDeep"
    movies_path = "../public/data/processed/movies.csv"
    users_path = "../public/data/processed/users.csv"
    ratings_path = "../public/data/original/ratings.csv"
    rec_sys = RecSys(recall_model_path, sort_model_path, movies_path, users_path, ratings_path)
    
    # rec_sys.predict_test()
    movies = rec_sys.recommend(1)
    
    # new rating test
    new_rating = {
        "userId": 1,
        "movieId": 1,
        "rating": 5.0,
        "timestamp": 964982703
    }
    rec_sys.new_rating(1,1,new_rating)
    
    