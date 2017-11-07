from flask import render_template
from . import visualization
import json
from app import db, util
from flask import Response, request
from app import util

@visualization.route('/')
def homepage():
    show_map = False
    dates = None
    if request.args.get('week_1') and request.args.get('week_2'):
        show_map = True
        dates = util.calender.custom_requirement_combine_weeks(request.args.get('week_1'), request.args.get('week_2'))
    return render_template('visualization/homepage.html', title='Home Page',
                           weeks=util.calender.get_weeks_for_2016_2017(), show_map=show_map, dates=dates)


@visualization.route('api')
def api():
    if not (request.args.get('week_1') and request.args.get('week_2')):
        return 'hehe'

    print(request.args)
    cursor = db.get_db().cursor()
    cursor.execute(
        "SELECT (a.t_avg - b.t_avg) as t_diff, (a.prcp - b.prcp) as prcp_diff, a.county as county FROM (SELECT * FROM weather WHERE ob_date=%s) a JOIN (SELECT * FROM weather WHERE ob_date=%s) b ON a.station_name = b.station_name GROUP BY a.county, a.id",
        (request.args.get('week_1'), request.args.get('week_2'))
    )

    # cursor.execute("SELECT t_avg, prcp, county from weather where ob_date='2016-01-02' group by county")

    data = cursor.fetchall()

    data = [['t_diff', 'prcp_diff', 'county']] + list(data)

    def generate():
        for row in data:
            if type(row[0]) == 'float':
                row[0] = (9.0 / 50) * row[0] + 32

            row = [str(item) for item in row]
            yield ','.join(row) + '\n'

    return Response(generate(), mimetype='text/csv')
