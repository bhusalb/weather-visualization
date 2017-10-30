class Config(object):
    """
    Common configurations
    """

    # Put any configurations here that are common across all environments


class DevelopmentConfig(Config):
    """
    Development configurations
    """
    DEBUG = True
    SECRET_KEY = 'p9Bv<3Eid9%$i01sdasd'
    MYSQL_DATABASE_DB = 'data_visualization'
    MYSQL_DATABASE_USER = 'root'
    MYSQL_DATABASE_PASSWORD = 'root'
    MYSQL_DATABASE_HOST = 'localhost'


class ProductionConfig(Config):
    """
    Production configurations
    """

    DEBUG = False


app_config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig
}
