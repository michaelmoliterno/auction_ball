import cloudsql

import plotly.plotly as py
import plotly.graph_objs as go


if __name__ == "__main__":

    list = cloudsql.league_seasons_draft_summary()

    ploty = {}
    ploty_prop = {}
    years = {}

    for x in list:
        year = x[1]
        draft_count = x[2]

        if year in years:
            val = years[year]
            years[year] = val + draft_count
        else:
            years[year] = draft_count


    for x in list:

        key = x[0]
        year = x[1]
        count = x[2]

        tuple = (year,count)

        if key in ploty:
            val = ploty[key]
            val.append(tuple)
        else:
            ploty[key] = [tuple]


    for x in list:

        key = x[0]
        year = x[1]
        count = x[2]

        tuple = (year,float(count)/years[year])

        if key in ploty_prop:
            val = ploty_prop[key]
            val.append(tuple)
        else:
            ploty_prop[key] = [tuple]


    print years
    print ploty
    print ploty_prop

    data = []

    for key,val in ploty_prop.iteritems():
        plot_vals = zip(*val)
        trace = go.Scatter(
            x = plot_vals[0],
            y = plot_vals[1],
            mode = 'lines',
            name = key
        )

        data.append(trace)

    py.plot(data, filename='draft_type_summary_by_year_prop')