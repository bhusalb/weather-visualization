import calendar, datetime

calendar.setfirstweekday(calendar.SATURDAY)


def get_weeks(year):
    c = calendar.Calendar(calendar.SATURDAY)
    weeks = []
    for quarterly in c.yeardatescalendar(year):
        for monthly in quarterly:
            for weekly in monthly:
                weeks.append(weekly[0])

    return weeks


def get_weeks_for_2016_2017():
    return get_weeks(2016) + get_weeks(2017)


def get_week_from_1st_day_date(date):
    d = datetime.datetime.strptime(date, '%Y-%m-%d')
    d = d.date()
    c = calendar.Calendar(calendar.SATURDAY)
    for quarterly in c.yeardatescalendar(d.year):
        for monthly in quarterly:
            for weekly in monthly:
                if d == weekly[0]:
                    return weekly


def custom_requirement_combine_weeks(date1, date2):
    week1 = get_week_from_1st_day_date(date1)
    week2 = get_week_from_1st_day_date(date2)
    weeks = []
    for i in range(0, 7):
        weeks.append([str(week1[i]), str(week2[i])])
    return weeks
