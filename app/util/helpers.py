import pymysql


def get_max_available_date():
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='root',
                                 db='data_visualization',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor()
    sql = "SELECT MAX(ob_date) as MAX_DATE from  weather"
    cursor.execute(sql)
    return cursor.fetchone()['MAX_DATE']


def get_max_min_year_from_database():
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='root',
                                 db='data_visualization',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor()
    sql = "SELECT YEAR(MAX(ob_date)) as MAX_YEAR, YEAR(MIN(ob_date)) as MIN_YEAR  from  weather"
    cursor.execute(sql)
    return cursor.fetchone()



