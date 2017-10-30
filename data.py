import csv
import pymysql.cursors


def filter_location_csv():
    with open('new-location-1.csv', 'wt') as csvfile:
        with open('location.csv') as csvfile1:
            reader = csv.DictReader(csvfile1, delimiter=',')
            writer = csv.DictWriter(csvfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                if row['country3'] == 'USA':
                    new_row = {};
                    for field in fieldnames:
                        new_row[field] = row[field]

                    writer.writerow(new_row)


def refine_ghcnd_txt():
    with open('new-ghcnd.csv', 'wt') as csvfile:
        with open('ghcnd-stations.txt') as csvfile1:
            reader = csv.reader(csvfile1, delimiter=' ')
            writer = csv.writer(csvfile)
            for row in reader:
                if len(row) == 8:
                    writer.writerow(row[:7] + [None] + row[7:])
                else:
                    writer.writerow(row)


def only_us_states_data(year):
    with open(year + '.csv') as csvfile:
        with open('filtered-' + year + '.csv', 'wt') as csvfile1:
            reader = csv.reader(csvfile)
            writer = csv.writer(csvfile1)
            for row in reader:
                if row[0][:2] == 'US':
                    writer.writerow(row)


def restructure_csv_file(year):
    with open('filtered-' + year + '.csv') as csvfile1:
        with open('restructured-' + year + '.csv', 'wt') as csvfile:
            reader = csv.reader(csvfile1)
            require_params = ['PRCP', 'TMAX', 'TMIN']
            writer = csv.DictWriter(csvfile, fieldnames=['station_name', 'date'] + require_params)
            writer.writeheader()
            is_first = True
            for row in reader:
                if is_first:
                    n_dict = {'station_name': row[0], 'date': row[1]}
                    is_first = False
                else:
                    if n_dict['station_name'] != row[0] or n_dict['date'] != row[1]:
                        writer.writerow(n_dict)
                        n_dict = {'station_name': row[0], 'date': row[1]}

                if row[2] in require_params:
                    n_dict[row[2]] = row[3]


def filter_us_stations():
    with open('new-ghcnd.csv') as csvfile:
        with open('filtered-stations.csv', 'wt') as csvfile1:
            reader = csv.reader(csvfile)
            writer = csv.writer(csvfile1)
            for row in reader:
                if row[0][:2] == 'US':
                    writer.writerow(row)


def weather_data_with_lat_long(year):
    stations = []
    with open('stations-with-fips.csv') as csvfile2:
        reader1 = csv.reader(csvfile2)
        stations = [row for row in reader1]
    with open('reduce-data-sets-' + year + '.csv') as csvfile:
        with open('with-lat-long-' + year + '.csv', 'wt') as csvfile1:

            reader = csv.DictReader(csvfile)
            writer = csv.DictWriter(csvfile1, fieldnames=reader.fieldnames + ['TAVG', 'LAT', 'LONG', 'FIPS'])
            writer.writeheader()
            for row in reader:
                cordinates = {}
                for nrow in stations:
                    if row['station_name'] == nrow[0]:
                        cordinates = {'LAT': nrow[1], 'LONG': nrow[2], 'FIPS': nrow[3]}
                        break
                t_avg = {}
                if row['TMIN'] and row['TMAX']:
                    t_avg = {'TAVG': (float(row['TMIN']) + float(row['TMAX'])) / 2}

                writer.writerow({**row, **cordinates, **t_avg})


def reduce_station_data_column():
    with open('filtered-stations.csv') as csvfile2:
        with open('limited-field-stations.csv', 'wt') as csvfile:
            writer = csv.writer(csvfile)
            reader1 = csv.reader(csvfile2)
            for row in reader1:
                writer.writerow(row[:3])


def get_fips_id(row):
    import stateplane
    return stateplane.identify(float(row[1]), float(row[2]), fmt='countyfp')


def add_fips_id_to_station():
    with open('limited-field-stations.csv') as csvfile2:
        with open('stations-with-fips-1.csv', 'wt') as csvfile:
            writer = csv.writer(csvfile)
            reader1 = csv.reader(csvfile2)
            for row in reader1:
                writer.writerow(row + [get_fips_id(row)])


def reduce_structured_data(year):
    with open('restructured-' + year + '.csv') as csvfile:
        with open('reduce-data-sets-' + year + '.csv', 'wt') as csvfile1:
            reader = csv.DictReader(csvfile)
            writer = csv.DictWriter(csvfile1, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                if row['PRCP'] and row['TMAX'] and row['TMIN']:
                    writer.writerow(row)


def format_for_mysql_import(year):
    with open('with-lat-long-' + year + '.csv') as csvfile:
        with open('final-version-' + year + '.csv', 'wt') as csvfile1:
            reader = csv.DictReader(csvfile)
            writer = csv.writer(csvfile1)
            count = 1
            for row in reader:
                date = row['date'][:4] + '-' + row['date'][4:6] + '-' + row['date'][6:]

                writer.writerow(
                    [count, row['station_name'], row['TMAX'], row['TMIN'], row['TAVG'], row['PRCP'], row['LAT'],
                     row['LONG'], date, row['FIPS']])

                count += 1


def specific_station_data_only(station_name, year):
    with open('filtered-' + year + '.csv') as csvfile:
        with open('filtered-' + station_name + '-' + year + '.csv', 'wt') as csvfile1:
            reader = csv.reader(csvfile)
            writer = csv.writer(csvfile1)
            for row in reader:
                if row[0] == station_name:
                    writer.writerow(row)


def grab_lat_and_long_only():
    with open('limited-field-stations.csv') as csvfile2:
        with open('lat-and-long.csv', 'wt') as csvfile:
            writer = csv.writer(csvfile)
            reader1 = csv.reader(csvfile2)
            for row in reader1:
                print(row[1:3])
                writer.writerow(row[1:3])


def remove_under_score_from_county_code(code):
    return code.replace('_', '')


def refine_receive_form_datascience_toolkit():
    with open('xx.csv') as csvfile:
        with open('refine_data_county_1.csv', 'wt') as csvfile1:
            reader = csv.DictReader(csvfile)
            writer = csv.writer(csvfile1)
            for row in reader:
                if row['friendly_type'] == 'county':
                    writer.writerow([row['latitude'], row['longitude'],
                                     remove_under_score_from_county_code(row['code'])])


def grab_county_code_from_mysql():
    with open('county-codes.csv', 'wt') as csvfile:
        writer = csv.writer(csvfile)
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='root',
                                     db='data_visualization',
                                     cursorclass=pymysql.cursors.DictCursor)

        cursor = connection.cursor()
        sql = "SELECT DISTINCT county FROM lat_long_county"
        cursor.execute(sql)
        for row in cursor.fetchall():
            writer.writerow([row['county']])


def add_county_to_mysql_database():
    import pandas as pd
    df = pd.read_csv('refine_data_county_1.csv')

    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='root',
                                 db='data_visualization',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor()

    with open('county-codes.csv') as csvfile:
        reader = csv.reader(csvfile)
        count = 1
        not_found = 0
        for row in reader:
            partial_query = []
            lat_long = df[df.county.isin([row[0]])][['lat', 'long']]
            for index, item in lat_long.iterrows():
                partial_query.append(
                    '(geo_lat=' + str(item['lat']) + ' and geo_long=' + str(item['long']) + ')')

            partial_query_1 = ' or '.join(partial_query)

            sql = "UPDATE weather SET county='" + row[0] + "' WHERE " + partial_query_1
            print(sql)

            if len(partial_query) != 0:
                cursor.execute(sql)
                connection.commit()
            else:
                print('not_found' + row[0])
                not_found += 1
            print('count: ' + str(count) + ' not found: ' + str(not_found))
            count += 1


def grab_lat_long_from_db():
    sql = 'SELECT DISTINCT CONCAT(geo_lat, geo_long), geo_lat, geo_long FROM weather'

    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='root',
                                 db='data_visualization',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor()

    cursor.execute(sql)
    connection.commit()
    with open('new-lat-long.csv', 'wt') as csvfile:
        writer = csv.writer(csvfile)
        for row in cursor.fetchall():
            writer.writerow([row['geo_lat'], row['geo_long']])


def fix_missing_county_using_fcc_api():
    sql = "SELECT DISTINCT CONCAT(geo_lat,geo_long), geo_lat, geo_long FROM weather WHERE LENGTH(county) <>5"

    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='root',
                                 db='data_visualization',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor()

    cursor.execute(sql)
    connection.commit()
    import requests
    with open('fixing-missing-data.csv', 'wt') as csvfile:
        writer = csv.writer(csvfile)
        count = 1
        for row in cursor.fetchall():
            res = requests.get(
                'http://data.fcc.gov/api/block/find?format=json&latitude=%s&longitude=%s&showall=true' % (
                    str(row['geo_lat']), str(row['geo_long'])))
            writer.writerow([row['geo_lat'], row['geo_long'], res.json()['County']['FIPS']])
            print(count)
            count += 1


def add_missing_data_to_mysql_db():
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='root',
                                 db='data_visualization',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor()
    with open('fixing-missing-data.csv') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            sql = "UPDATE weather SET county='" + row[2] + "' WHERE geo_lat = " + row[0] + " and geo_long = " + row[1]

            cursor.execute(sql)
            connection.commit()
            print(sql)
