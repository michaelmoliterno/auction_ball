import cloudsql

import plotly.plotly as py
import plotly.graph_objs as go

from dateutil.parser import parse

import pymongo
from pymongo import MongoClient
import pandas as pd
import numpy as np

connection = MongoClient()
db = connection['espn_draft_picks']
league_settings = db['league_settings']

auction_drafts = db['auction_drafts']
snake_drafts = db['snake_drafts']

if __name__ == "__main__":

    leagues_df = pd.DataFrame(list(league_settings.find({"Format":"League Manager",
                                                        "Scoring Type":"Head to Head Each Category"})))
    #leagues_df = pd.DataFrame(list(league_settings.find()))

    cols = ['Auction Budget',
            'Current Roster Size',
            'Draft Date',
            'Draft Type',
            'Format',
            'Number of Teams',
            'Scoring Type',
            'stats',
            'positions',
            'Allow Draft Pick Trading',
            'League Name',
            'Location',
            'Note',
            'Keeper Note',
            'Total Starters',
            'season',
            'key']

    leagues_df = leagues_df[cols]

    cols = ['AuctionBudget',
                'CurrentRosterSize',
                'DraftDate',
                'DraftType',
                'Format',
                'NumberofTeams',
                'ScoringType',
                'stats',
                'positions',
                'AllowDraftPickTrading',
                'LeagueName',
                'Location',
                'Note',
                'KeeperNote',
                'TotalStarters',
                'season',
                'key']

    leagues_df.columns = cols

    # Location_type_dict = leagues_df['Location'].value_counts()
    # print Location_type_dict.head(25)
    #
    #
    # LeagueName_type_dict = leagues_df['LeagueName'].value_counts()
    # print LeagueName_type_dict.head(25)


    # opening_days = {2016:parse("April 4 2016"),
    #                 2015:parse("April 6 2015"),
    #                 2014:parse("March 31 2014"),
    #                 2013:parse("April 1 2013")}
    data = []
    for year in range(2011,2017):

        season = leagues_df[leagues_df.season == "{}".format(year)]
        print year
        print season['DraftType'].value_counts()







        #     #opening day stuff
        # #
        #     season = leagues_df[leagues_df.season == "{}".format(year)]
        #
        #     dates = season['DraftDate'].str.split(',').str.get(1).dropna()
        #
        #     draft_dates = dates.astype(str) + ", {}".format(year)
        #
        #     date_counts = pd.to_datetime(draft_dates)#.value_counts().sort_index()
        #
        #     time_delta_counts = date_counts - opening_days[year]
        #
        #     date_diff_counts = (time_delta_counts/ np.timedelta64(1, 'D')).astype(int)
        #
        #     #print date_counts.index - opening_days[year]
        #
        #     date_diff_counts = date_diff_counts[date_diff_counts >= -28]
        #     date_diff_counts = date_diff_counts[date_diff_counts <= 14]
        #
        #     date_diff_counts_now = date_diff_counts[date_diff_counts <= -13]
        #
        #     # print year
        #     # print date_diff_counts.size
        #     # print date_diff_counts_now.size
        #     #
        #     # print date_diff_counts_now.size/float(date_diff_counts.size)
        #
        #     # date_diff_props = date_diff_counts.value_counts().sort_index()/date_diff_counts.size
        #     #
        #     # if year == 2016:
        #     #     date_diff_props = date_diff_props/4
        #     #
        #     # trace = go.Scatter(
        #     #     x = date_diff_props.index,
        #     #     y = date_diff_props.round(3),
        #     #     mode = 'lines',
        #     #     name = year
        #     # )
        #     #
        #     # data.append(trace)
        #
        #
        # py.plot(data, filename='draft_dates_by_year')


        # trace = go.Scatter(x=date_counts.index.tz_localize('utc'), y=date_counts)
        # data = [trace]
        #
        # url = py.plot(data, filename='2016_drafts')



        # draft_type_dict = season['DraftType'].value_counts().to_dict()
        #
        # draft_type_df =  pd.DataFrame(draft_type_dict.items())
        # draft_type_df['year'] = year
        # draft_type_dfs.append(draft_type_df)
        #
        # scoring_type_dict = season['ScoringType'].value_counts().to_dict()
        #
        # scoring_type_df =  pd.DataFrame(scoring_type_dict.items())
        # scoring_type_df['year'] = year
        # scoring_type_dfs.append(scoring_type_df)

        # stats_type_dict = season['stats'].value_counts()
        # print stats_type_dict.head(25)


        #print season.apply(lambda x: season['stats'].value_counts()).T.stack()




    #
    # pd.concat(draft_type_dfs, ignore_index=True).to_csv('draft_type_dfs.csv')
    #
    # pd.concat(scoring_type_dfs, ignore_index=True).to_csv('scoring_type_dfs.csv')







        # season = season[season.NumberofTeams == "10"]
        #
        #
        # data = [
        #         go.Histogram(
        #             x=season.CurrentRosterSize
        #         )
        #     ]
        #
        # plot_url = py.plot(data, filename="{}CurrentRosterSize10".format(year))




    #
    #
    #
    #
    #     keeper_string = '{} Keepers Per Team'.format(year)
    #
    #     temp_df = leagues_df[leagues_df.season == "{}".format(year)]
    #
    #     series = temp_df[[keeper_string]]
    #     series = series.replace("None","0")
    #     series = series.astype(float)
    #
    #     league_count = len(series.index)
    #     keepers = series[series>0].dropna()
    #     keepers_count = len(keepers.index)
    #
    #
    #     data_series = keepers[keeper_string]
    #     #print year, league_count, keepers_count, keepers_count/float(league_count)
    #
    #     print year, keepers.mean(), keepers.median(), keepers.max(), keepers.min()
    #     #

    #
    #     #df = leagues_df[['{} Keepers Per Team'.format(year), '{} Keepers Per Team'.format(year)]]
    #
    #
    #     #.astype(float)
    #
    #
    #
    #
    #     # print df.groupby('season').median()['{} Keepers Per Team'.format(year)]
    #     # print df.groupby('season').median()['{} Keepers Per Team'.format(year)]
    #     # print df.groupby('season').max()['{} Keepers Per Team'.format(year)]
    #     # print df.groupby('season').min()['{} Keepers Per Team'.format(year)]
    #
    #     #leagues_df.groupby('season').median()['{} Keeper Lock Date'.format(year)]
    #     # season_keeper_total = season['{} Keepers Per Team'.format(year)]
    #     # season_keeper_lock_date = season['{} Keeper Lock Date'.format(year)]
    #     #
    #     # df = pd.concat([season_keeper_total,
    #     #                 season_keeper_lock_date,
    #     #                ])
    #     #
    #     # df.columns = [  '{} Keepers Per Team'.format(year),
    #     #                 '{} Keeper Lock Date'.format(year)
    #     #              ]
    #     #
    #     # season_dfs.append(df)
    #
    # #print pd.concat(season_dfs, ignore_index=True)
    # #season = season[cols]
    # #season = season.set_index('key')
    # #
    # # season = season.append(season_keeper_total)
    # # season = season.append(season_keeper_lock_date)
    # #
    # # season_dfs.append(season)
    #
    # #all_seasons =  pd.concat(season_dfs, ignore_index=True)
    #
    # #print all_seasons.head()
    # #print all_seasons.tail()











    #print season

    #
    # list = cloudsql.league_seasons_draft_summary()
    #
    # ploty = {}
    # ploty_prop = {}
    # years = {}
    #
    # for x in list:
    #     year = x[1]
    #     draft_count = x[2]
    #
    #     if year in years:
    #         val = years[year]
    #         years[year] = val + draft_count
    #     else:
    #         years[year] = draft_count
    #
    #
    # for x in list:
    #
    #     key = x[0]
    #     year = x[1]
    #     count = x[2]
    #
    #     tuple = (year,count)
    #
    #     if key in ploty:
    #         val = ploty[key]
    #         val.append(tuple)
    #     else:
    #         ploty[key] = [tuple]
    #
    #
    # for x in list:
    #
    #     key = x[0]
    #     year = x[1]
    #     count = x[2]
    #
    #     tuple = (year,float(count)/years[year])
    #
    #     if key in ploty_prop:
    #         val = ploty_prop[key]
    #         val.append(tuple)
    #     else:
    #         ploty_prop[key] = [tuple]
    #
    #
    # print years
    # print ploty
    # print ploty_prop
    #
    # data = []
    #
    # for key,val in ploty_prop.iteritems():
    #     plot_vals = zip(*val)
    #     trace = go.Scatter(
    #         x = plot_vals[0],
    #         y = plot_vals[1],
    #         mode = 'lines',
    #         name = key
    #     )
    #
    #     data.append(trace)
    #
    # py.plot(data, filename='draft_type_summary_by_year_prop')
