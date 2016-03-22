import pymongo
from pymongo import MongoClient
import pandas as pd
import math
import json
from bson import BSON
from bson import json_util

def convert_mod_pick_num(mod_pick_num):
    listy = range(9,-1,-1)
    valy = listy[mod_pick_num]
    return valy

connection = MongoClient()
db = connection['espn_draft_picks']

Auction_10_Standard_Rotisserie_2015 = db['2015_Auction_10_Standard_Rotisserie']
Snake_10_Standard_Rotisserie_2015 = db['2015_Snake_10_Standard_Rotisserie']

Auction_10_Standard_Rotisserie_2016 = db['2016_Auction_10_Standard_Rotisserie']
Snake_10_Standard_Rotisserie_2016 = db['2016_Snake_10_Standard_Rotisserie']

Auction_12_Standard_Rotisserie_2016 = db['2016_Auction_12_Standard_Rotisserie']
Snake_12_Standard_Rotisserie_2016 = db['2016_Snake_12_Standard_Rotisserie']

seasons = {

# "lm_roto_10_2011":( db['2011_Auction_10_League_Manager_Rotisserie_260'],
#                     db['2011_Snake_10_League_Manager_Rotisserie_260'],
#                     10,
#                     2011
#                     ),
# "lm_roto_10_2012":( db['2012_Auction_10_League_Manager_Rotisserie_260'],
#                     db['2012_Snake_10_League_Manager_Rotisserie_260'],
#                     10,
#                     2012
#                     ),
# "lm_roto_10_2013":( db['2013_Auction_10_League_Manager_Rotisserie_260'],
#                     db['2013_Snake_10_League_Manager_Rotisserie_260'],
#                     10,
#                     2013
#                   ),
# "lm_roto_10_2014":(
#                     db['2014_Auction_10_League_Manager_Rotisserie_260'],
#                     db['2014_Snake_10_League_Manager_Rotisserie_260'],
#                     10,
#                     2014
#                   ),
# "lm_roto_10_2015":(
#                     db['2015_Auction_10_League_Manager_Rotisserie_260'],
#                     db['2015_Snake_10_League_Manager_Rotisserie_260'],
#                     10,
#                     2015
#                     ),
# "lm_roto_10_2016":(
#                     db['2016_Auction_10_League_Manager_Rotisserie_260'],
#                     db['2016_Snake_10_League_Manager_Rotisserie_260'],
#                     10,
#                     2016
#                   )
}


PLAYER_ID_MAP_DF = pd.read_csv('PLAYERIDMAP.csv')

seasons = {
        "standard_roto_10_2015" : (Auction_10_Standard_Rotisserie_2015,Snake_10_Standard_Rotisserie_2015,10,2015),
        "standard_roto_10_2016" : (Auction_10_Standard_Rotisserie_2016,Snake_10_Standard_Rotisserie_2016,10,2016),
        #"standard_roto_12_2016" : (Auction_12_Standard_Rotisserie_2016,Snake_12_Standard_Rotisserie_2016,12,2016),
        }


def get_players_df():
    player_universe = db['player_universe']

    all_players = player_universe.find()
    #return json.dumps(all_players, sort_keys=True, indent=4, default=json_util.default)

    all_players_list = [doc['_id'] for doc in all_players]
    df = pd.DataFrame(all_players_list).set_index('player_id')
    #print df['player_id']
    return df


def get_draft_ranks_diffs(seasons):

    season_dfs = []

    for season_name, collections in seasons.iteritems():

        auction = collections[0]
        snake = collections[1]
        num_teams = collections[2]
        year = collections[3]

        snakes_df = pd.DataFrame(list(snake.find()))
        auctions_df = pd.DataFrame(list(auction.find()))

        league_count_auction = len(auction.distinct('league_id'))
        league_count_snakes = len(snake.distinct('league_id'))


        snake_counts = snakes_df.groupby('player_id').count()["_id"]
        auction_counts = auctions_df.groupby('player_id').count()["_id"]

        #auction_counts_filtered = auction_counts[auction_counts>10]
        #player_ids = list(auction_counts_filtered.index)
        #auctions_df_filtered = auctions_df[auctions_df['player_id'].isin(player_ids)]
        #snakes_df_filtered = snakes_df[snakes_df['player_id'].isin(player_ids)]

        median_auction_price = auctions_df.groupby('player_id').median()['price']
        mean_auction_price = auctions_df.groupby('player_id').mean()['price']

        median_nom_order = auctions_df.groupby('player_id').median()['nom_order']
        mean_nom_order = auctions_df.groupby('player_id').mean()['nom_order']

        median_snake_pick = snakes_df.groupby('player_id').median()['pick_num']
        mean_snake_pick = snakes_df.groupby('player_id').mean()['pick_num']

        auction_summary = pd.concat([
                                    median_auction_price,
                                    #median_auction_price.rank(ascending=False),
                                    mean_auction_price,
                                    #mean_auction_price.rank(ascending=False),
                                    auction_counts,
                                    median_nom_order,
                                    mean_nom_order
                                    ],
                                    axis=1,
                                    join='inner'
                                    )

        auction_summary.columns = [
                                    'median_auction_price',
                                    #'median_auction_price_rank',
                                    'mean_auction_price',
                                    #'mean_auction_price_rank',
                                    'auction_count',
                                    'median_nom_order',
                                    'mean_nom_order']

        #auction_summary["total_auctions"] = league_count_auction
        auction_summary["prop_auctions"] = auction_summary["auction_count"]/league_count_auction


        snake_summary = pd.concat([
                                    median_snake_pick,
                                    #median_snake_pick.rank(ascending=True),
                                    mean_snake_pick,
                                    #mean_snake_pick.rank(ascending=True),
                                    snake_counts
                                  ],
                                    axis=1, join='inner'
                                  )

        snake_summary.columns = [
                                'median_snake_pick',
                                #'median_snake_pick_rank',
                                'mean_snake_pick',
                                #'mean_snake_pick_rank',
                                'snake_count'
                                ]

        #snake_summary["total_snakes"] =
        snake_summary["prop_snakes"] = snake_summary["snake_count"]/league_count_snakes

        draft_summary = pd.concat([auction_summary,snake_summary],axis=1)



        draft_summary['prop_diff_auction_snake'] = draft_summary["prop_auctions"] - draft_summary["prop_snakes"]


        draft_summary = draft_summary[draft_summary.median_auction_price >= 5]
        draft_summary = draft_summary[draft_summary.prop_snakes > .05]
        #draft_summary = draft_summary.loc[:,draft_summary.loc['prop_snakes']>.05]
        #draft_summary = draft_summary.loc[:,draft_summary.loc['prop_snakes']>.05]

        draft_summary['median_auction_price_rank'] = draft_summary['median_auction_price'].rank(ascending=False)
        draft_summary['mean_auction_price_rank'] = draft_summary['mean_auction_price'].rank(ascending=False)

        draft_summary['median_snake_pick_rank'] = draft_summary['median_snake_pick'].rank(ascending=True)
        draft_summary['mean_snake_pick_rank'] = draft_summary['mean_snake_pick'].rank(ascending=True)

        draft_summary['median_draft_type_diff'] = draft_summary['median_snake_pick_rank'] - draft_summary['median_auction_price_rank']
        draft_summary['mean_draft_type_diff'] = draft_summary['mean_snake_pick_rank'] -  draft_summary['mean_auction_price_rank']


        draft_summary.index = draft_summary.index.astype(int)

        players_df = get_players_df()
        players_df.index = players_df.index.astype(int)

        draft_summary = draft_summary.join(players_df,how='left')

        draft_summary['year'] = year

        season_dfs.append(draft_summary)


    all_years = pd.concat(season_dfs, ignore_index=True)
    # all_years = all_years[all_years.prop_snakes > .1]
    # all_years = all_years[all_years.median_auction_price > 10]

    return all_years[[  'player_name',
                        'year',
                        'median_auction_price',
                        'mean_auction_price',
                        'median_auction_price_rank',
                        'mean_auction_price_rank',
                        'median_snake_pick',
                        'mean_snake_pick',
                        'median_snake_pick_rank',
                        'mean_snake_pick_rank',
                        'median_draft_type_diff',
                        'mean_draft_type_diff',
                        'prop_snakes',
                        'prop_auctions',
                        'prop_diff_auction_snake',
                      ]]

def get_aggregate_roster_values(seasons):
    for season_name, collections in seasons.iteritems():

        print season_name

        auction = collections[0]
        snake = collections[1]
        num_teams = collections[2]
        year = collections[3]

        auctions_df = pd.DataFrame(list(auction.find()))
        league_count_auction = len(auction.distinct('league_id'))

        print league_count_auction

        snakes_df = pd.DataFrame(list(snake.find()))
        league_count_snakes = len(snake.distinct('league_id'))

        print league_count_snakes

        auction_counts = auctions_df.groupby('player_id').count()["_id"]
        auction_counts_filtered = auction_counts[auction_counts>10]
        player_ids = list(auction_counts_filtered.index)
        auctions_df_filtered = auctions_df[auctions_df['player_id'].isin(player_ids)]
        median_auction_price = auctions_df_filtered.groupby('player_id').median()['price']
        #mean_auction_price = auctions_df_filtered.groupby('player_id').mean()['price']

        snakes_df = snakes_df.join(median_auction_price,
                                                on='player_id',
                                                how='left',
                                                lsuffix='',
                                                rsuffix='med'
                                                )

        snakes_df.fillna(1)

        snakes_df["mod_round_num"] = snakes_df["round_num"]%2
        snakes_df["mod_pick_num"] = (snakes_df["pick_num"]-1)%num_teams

        snakes_df.loc[snakes_df['mod_round_num'] == 0, 'mod_pick_num'] = \
        snakes_df['mod_pick_num'].apply(convert_mod_pick_num)

        avg_roster_value_by_pick_num = snakes_df.groupby('mod_pick_num').sum()["price"]/league_count_snakes

        snake_team_summaries = snakes_df.groupby(['league_id','mod_pick_num']).sum()["price"]


        auctions_df = auctions_df.join(median_auction_price,
                                                on='player_id',
                                                how='left',
                                                lsuffix='',
                                                rsuffix='med'
                                        )

        auctions_df.fillna(1)
        avg_roster_value_by_team_num = auctions_df.groupby('team_id').sum()["price"]/league_count_auction
        auction_team_summaries = auctions_df.groupby(['league_id','team_id']).sum()["pricemed"]

        #print snake_team_summaries
        print avg_roster_value_by_pick_num
        #print auction_team_summaries
        print avg_roster_value_by_team_num

        snake_team_summaries = snake_team_summaries.reset_index()
        auction_team_summaries = auction_team_summaries.reset_index()

        print snake_team_summaries.groupby(['mod_pick_num']).std()["price"]/math.sqrt(league_count_snakes)
        print auction_team_summaries.groupby(['team_id']).std()["pricemed"]/math.sqrt(league_count_auction)


if __name__ == "__main__":
    #get_aggregate_roster_values(seasons)
    df = get_draft_ranks_diffs(seasons)
    df.to_csv('rank_diffs_by_year.csv')
    print df
