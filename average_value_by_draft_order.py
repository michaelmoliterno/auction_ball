import pymongo
from pymongo import MongoClient
import pandas as pd
import math



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
        "standard_roto_10_2015" : (Auction_10_Standard_Rotisserie_2015,Snake_10_Standard_Rotisserie_2015,10,2015),
        #"standard_roto_10_2016" : (Auction_10_Standard_Rotisserie_2016,Snake_10_Standard_Rotisserie_2016,10,2016),
        #"standard_roto_12_2016" : (Auction_12_Standard_Rotisserie_2016,Snake_12_Standard_Rotisserie_2016,12,2016),
        }

for season_name, collections in seasons.iteritems():

    print season_name

    auction = collections[0]
    snake = collections[1]
    num_teams = collections[2]
    year = collections[3]

    # auctions_df = auctions_df[season==year]
    # snakes_df = snakes_df[season==year]

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


    # snakes_df = snakes_df.join(mean_auction_price,
    #                                         on='player_id',
    #                                         how='left',
    #                                         lsuffix='',
    #                                         rsuffix='mean'
    #                                     )

    snakes_df.fillna(1)

    snakes_df["mod_round_num"] = snakes_df["round_num"]%2
    snakes_df["mod_pick_num"] = (snakes_df["pick_num"]-1)%num_teams
    #snakes_df["team_num_mod"] = -1

    snakes_df.loc[snakes_df['mod_round_num'] == 0, 'mod_pick_num'] = \
    snakes_df['mod_pick_num'].apply(convert_mod_pick_num)

    avg_roster_value_by_pick_num = snakes_df.groupby('mod_pick_num').sum()["price"]/league_count_snakes

    #nom_order_mod_mean_sums = snakes_df.groupby('mod_pick_num').sum()["pricemean"]/league_count_snakes
    #print nom_order_mod_med_sums
    #print nom_order_mod_mean_sums

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
    #print snake_team_summaries
    print snake_team_summaries.groupby(['mod_pick_num']).std()["price"]/math.sqrt(league_count_snakes)
    print auction_team_summaries.groupby(['team_id']).std()["pricemed"]/math.sqrt(league_count_auction)

    # print snake_team_summaries.groupby(['league_id']).var()["price"]
    # print auction_team_summaries.groupby(['league_id']).var()["pricemed"]


    #print team_summaries.head(50)

    # median_nom_order = auctions_df.groupby('player_id').median()['nom_order']
    # mean_nom_order = auctions_df.groupby('player_id').mean()['nom_order']
    # #
    # # median_snake_pick = snakes_df.groupby('player_id').median()['pick_num']
    # # mean_snake_pick = snakes_df.groupby('player_id').mean()['pick_num']
    #
    #
    # auction_summary = pd.concat([median_auction_price,
    #                              median_auction_price.rank(ascending=False),
    #                              mean_auction_price,
    #                              mean_auction_price.rank(ascending=False),
    #                              auction_counts,
    #                              median_nom_order,
    #                              mean_nom_order],
    #                              axis=1, join='inner')
    #
    # auction_summary.columns = ['median_auction_price','median_auction_price_rank',
    #                            'mean_auction_price','mean_auction_price_rank','auction_count',
    #                            'median_nom_order','mean_nom_order']
    #
    # auction_summary["total_auctions"] = max(auction_counts)
    #
    # auction_summary["prop_auctions"] = auction_summary["auction_count"]/auction_summary["total_auctions"]
    #
    # print auction_summary

    #median_auction_price.to_json('2015_Auction_10_Standard_Rotisserie_median.json')
    #mean_auction_price.to_json('2015_Auction_10_Standard_Rotisserie_mean.json')







# ## auction stuff
# auctions_df = auctions_df.join(median_auction_price,
#                                         on='player_id',
#                                         how='left',
#                                         lsuffix='',
#                                         rsuffix='med'
#                                         )
#
#
# auctions_df = auctions_df.join(mean_auction_price,
#                                         on='player_id',
#                                         how='left',
#                                         lsuffix='',
#                                         rsuffix='mean'
#
#                                     )
#
# auctions_df['nom_order_mod'] = auctions_df['nom_order']%10
# auctions_df.fillna(1)
#
#
# nom_order_mod_med_sums = auctions_df.groupby('nom_order_mod').sum()["pricemed"]/league_count_auction
# nom_order_mod_mean_sums = auctions_df.groupby('nom_order_mod').sum()["pricemean"]/league_count_auction
#
# print nom_order_mod_med_sums
# print nom_order_mod_mean_sums
