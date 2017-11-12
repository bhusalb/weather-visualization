import datetime

import data

now = datetime.datetime.now()


def cron_handler():
    years = [str(now.year)]
    if now.month == 1 and (now.day == 1 or now.day == 2 or now.day == 3):
        years.insert(0, str(now.year - 1))
    print('Cron started!')
    for year in years:
        local_last_modified = data.get_local_csv_last_modified_date(year)
        if (not local_last_modified) or (data.get_last_modified_date_before_downloading(year) > local_last_modified):
            print('starting csv file download from server')
            data.download_csv_from_server(year)
            print('filtering only us data')
            data.only_us_states_data(year)
            print('filtering unavailable data in Database')
            data.only_unavailable_in_database(year)
            print('filtering remove quality data')
            data.remove_quality_fail_data(year)
            print('restructuring data')
            data.restructure_csv_file(year)
            print('reducing data')
            data.reduce_structured_data(year)
            print('restructuring data for mysql import')
            data.restructure_for_mysql_import(year)
            print('import csv data to database')
            data.import_generate_csv_file_to_mysql(year)

    print('cron job completed!')
# data.add_county_lat_long_for_null_on_database()


cron_handler()
