import datetime

import data

now = datetime.datetime.now()


def cron_handler():
    years = [str(now.year)]
    if now.month == 1 and (now.day == 1 or now.day == 2 or now.day == 3):
        years.insert(0, str(now.year - 1))

    # For testing
    # years = ['2015']
    for year in years:
        local_last_modified = data.get_local_csv_last_modified_date(year)
        if (not local_last_modified) or (data.get_last_modified_date_before_downloading(year) > local_last_modified):
            data.download_csv_from_server(year)
            data.only_us_states_data(year)
            data.only_unavailable_in_database(year)
            data.restructure_csv_file(year)
            data.reduce_structured_data(year)
            data.restructure_for_mysql_import(year)
            data.import_generate_csv_file_to_mysql(year)
            data.add_county_lat_long_for_null_on_database()


cron_handler()
