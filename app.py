from flask import Flask
import os
import service.handlers as handlers

app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24)

app.add_url_rule('/', view_func=handlers.hello)
app.add_url_rule('/index', view_func=handlers.index)
app.add_url_rule('/register', view_func=handlers.register, methods=['GET', 'POST'])
app.add_url_rule('/login', view_func=handlers.login, methods=['GET', 'POST'])
app.add_url_rule('/logout', view_func=handlers.logout)
app.add_url_rule('/rec', view_func=handlers.rec)
app.add_url_rule('/rating', view_func=handlers.rating, methods=['GET', 'POST'])

if __name__ == '__main__':
    app.run(debug=True)
    