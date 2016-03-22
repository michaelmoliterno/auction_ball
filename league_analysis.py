import cloudsql

import plotly.plotly as py
import plotly.graph_objs as go

import pymongo
from pymongo import MongoClient
import pandas as pd
import numpy as np

connection = MongoClient()
db = connection['espn_draft_picks']
league_settings = db['league_settings']

if __name__ == "__main__":

    leagues_df = pd.DataFrame(list(league_settings.find({"Format":"League Manager"})))

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



    #
    # ratio = lambda x: x.value_counts(normalize=True)
    #
    # #output_lambda
    #
    # draft_type_dfs = []
    # scoring_type_dfs = []
    #
    for year in range(2015,2016):
    #
        season = leagues_df[leagues_df.season == "{}".format(year)]
        #output_lambda = season.apply(lambda x: [x.value_counts().to_dict()]).apply(lambda x: x[0]).to_dict()

        print season['DraftDate']

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
