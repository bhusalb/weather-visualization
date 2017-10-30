from flask import Flask
from flaskext.mysql import MySQL

from app.config import app_config

# db variable initialization
db = MySQL()


def create_app(config_name):
    # existing code remains

    from app import models

    app = Flask(__name__, instance_relative_config=False)
    app.config.from_object(app_config[config_name])
    app.config.from_pyfile('config.py')
    db.init_app(app)

    from .visualization import visualization as visualization_blueprint
    app.register_blueprint(visualization_blueprint, url_prefix='/')

    return app
