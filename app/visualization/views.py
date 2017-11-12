from flask import render_template
from . import visualization
import json
from app import db, util
from flask import Response, request
from app import util


@visualization.route('/')
def homepage():
    max_available_date = util.helpers.get_max_available_date()
    show_map = False
    dates = None
    if request.args.get('week_1') and request.args.get('week_2'):
        show_map = True
        dates = util.calender.custom_requirement_combine_weeks(request.args.get('week_1'), request.args.get('week_2'))
    return render_template('visualization/homepage.html', title='Home Page',
                           weeks=util.calender.get_weeks_for_2016_2017(),
                           show_map=show_map,
                           dates=dates,
                           max_available_date=max_available_date
                           )


@visualization.route('api')
def api():
    if not (request.args.get('week_1') and request.args.get('week_2')):
        return 'hehe'

    print(request.args)
    cursor = db.get_db().cursor()
    cursor.execute(
        "SELECT TENTHS_C_TO_F(a.t_avg - b.t_avg) as t_diff, (a.prcp - b.prcp) / 10 as prcp_diff, c.county as county FROM weather AS a JOIN weather AS b USING(station_name) JOIN stations as c USING(station_name) WHERE a.ob_date=%s AND b.ob_date=%s GROUP BY county",
        (request.args.get('week_1'), request.args.get('week_2'))
    )

    # cursor.execute("SELECT t_avg, prcp, county from weather where ob_date='2016-01-02' group by county")

    data = cursor.fetchall()

    data = [['t_diff', 'prcp_diff', 'county']] + list(data)

    def generate():
        for row in data:
            row = [str(item) for item in row]
            yield ','.join(row) + '\n'

    return Response(generate(), mimetype='text/csv')
