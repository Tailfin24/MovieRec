"""
@file    : handlers.py
@function: Flask 线上功能服务
@version : V1.0
"""

from flask import render_template, request, redirect, url_for, flash, session
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from service.rec_sys import RecSys
import random
import datetime

LOAD_PICTURE = True
IMG_SIZE = "w342"

rec_sys = RecSys()
ph = PasswordHasher()

def hello():
    return redirect(url_for('index'))


def index():
    """默认页面"""
    if 'username' not in session:
        return redirect(url_for('login'))
    rating_list = rec_sys.dm.get_ratings(session['userId'])
    rated_movies = []
    for rating in rating_list:
        movie = rec_sys.dm.get_movie(rating["movieId"])
        if not movie:
            continue
        movie["rating"] = rating["rating"]
        
        rated_movies.append(movie)
        
    if LOAD_PICTURE:
        print("Loading Pictures...")
        for movie in rated_movies:
            movie["image_url"] = rec_sys.dm.get_poster_url(movie["movieId"], IMG_SIZE)
        print("Loading Pictures Success.")
        
    return render_template('index.html', username=session['username'], userId=session["userId"], gender=session["gender"], age=session["age"], rated_movies=rated_movies)
    


def register():
    """注册业务逻辑"""
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        confirm_password = request.form['confirm_password']
        gender = request.form.get('gender')
        age = int(request.form.get('age'))
        
        if rec_sys.dm.get_profile(username):
            session.pop('_flashes', None)
            flash("用户名已存在，请选择其它用户名。", "error")
        elif password != confirm_password:
            session.pop('_flashes', None)
            flash("两次密码输入不一致，请重新输入。", "error")
        elif gender not in ('M', "F"):
            session.pop('_flashes', None)
            flash("性别输入不合法！", "error")
        elif age <= 0:
            session.pop('_flashes', None)
            flash("年龄输入不合法！", "error")
        else:
            user_id = rec_sys.dm.user_id_next
            rec_sys.dm.user_id_next += 1
            hashed_password = ph.hash(password)
            
            # 账户信息的更新
            profile = {
                "username": username,
                "userId": user_id,
                "password": hashed_password,
            }
            rec_sys.dm.new_profile(username, profile)
            
            userData = {
                "userId": user_id,
                "Gender": gender,
                "Age": age,
                "Occupation": 0,
                "user_avg_rating": 0.0,
                "user_rating_stddev": 0.0,
                "user_rating_count": 0,
                "fav_movie_1": 0,
                "fav_movie_2": 0,
                "fav_movie_3": 0
            }
            rec_sys.dm.load_user(user_id, userData)
            session.pop('_flashes', None)
            flash("注册成功！请登录。", "success")
            return redirect(url_for('login'))
    session.pop('_flashes', None)
    return render_template('register.html')


def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        user = rec_sys.dm.get_profile(username)
        if user:
            try:
                ph.verify(user["password"], password)
            except VerifyMismatchError:
                session.pop('_flashes', None)
                flash("密码错误，请重新输入。", "error")
            else:   
                session['username'] = user["username"]
                session['userId'] = user["userId"]
                userData = rec_sys.dm.get_user(user["userId"])
                session['gender'] = userData["Gender"]
                session['age'] = userData["Age"]
                session.pop('_flashes', None)
                flash("登陆成功！", "success")
                return redirect(url_for('index'))
        else:
            session.pop('_flashes', None)
            flash("用户名不存在", "error")
    session.pop('_flashes', None)
    return render_template('login.html')


def logout():
    session.pop('username', None)
    session.pop('userId', None)
    session.pop('gender', None)
    session.pop('age', None)
    session.pop('_flashes', None)
    flash("您已成功退出登录。", "success")
    return redirect(url_for('login'))    
 

def rec():
    """推荐页面"""
    if 'username' not in session:
        return redirect(url_for('login'))
    
    userId=session['userId']
    movies = rec_sys.recommend(userId)
    
    # 添加图片
    if LOAD_PICTURE:
        print("Loading Pictures...")
        for movie in movies:
            movie["image_url"] = rec_sys.dm.get_poster_url(movie["movieId"], IMG_SIZE)
        print("Loading Pictures Success.")
    
    return render_template("rec.html", movies=movies)


def rating():
    if 'username' not in session:
        return redirect(url_for('login'))
    if request.method == 'POST':
        ratings = []
        for movieId, rating in request.form.items():
            movieId = int(movieId)  # 电影 ID
            rating = '{:.1f}'.format(float(rating))

            # 获取当前时间戳（以秒为单位）
            timestamp = int(datetime.datetime.now().timestamp())
            rating_dict = {
                "movieId": movieId,
                "userId": session['username'],
                "rating": rating,
                "timestamp": timestamp,
            }
            ratings.append(rating_dict)
        print(ratings)
        rec_sys.new_ratings(session['userId'], ratings)
        session.pop('_flashes', None)
        flash("评分提交成功", "success")
        return redirect(url_for('rating'))
    
    cnt = 0
    movies = []

    while cnt < 10:
        r_id = random.randint(1, 150000)
        movie = rec_sys.dm.get_movie(r_id)
        if not movie:
            continue 
        movies.append(movie)
        cnt += 1
    if LOAD_PICTURE:
        print("Loading Pictures...")
        for movie in movies:
            movie["image_url"] = rec_sys.dm.get_poster_url(movie["movieId"], IMG_SIZE)
        print("Loading Pictures Success.")
    return render_template('rating.html', movies=movies) 