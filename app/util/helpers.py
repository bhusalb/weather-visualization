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
