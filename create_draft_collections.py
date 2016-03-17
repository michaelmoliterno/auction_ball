import pymongo
from pymongo import MongoClient

# from bs4 import BeautifulSoup
# from dateutil.parser import parse
# from datetime import datetime
# from datetime import datetime, timedelta
# import urlparse
# import os
connection = MongoClient()
db = connection['espn_draft_picks']
league_settings = db['league_settings']
auction_drafts = db['auction_drafts']
snake_drafts = db['snake_drafts']


scoring_types = {
    	#"h2hp":"Head to Head Points",
    	#"h2hec":"Head to Head Each Category",
    	#"stp":"Total Season Points",
    	"roto":"Rotisserie",
    	#"h2hmc":"Head to Head Most Categories"
    }
draft_types = {
                #"Offline",
                "Auction": db['auction_drafts'],
                "Snake":db['snake_drafts'],
                #"Autopick",
                }

years = ["2015","2016"]
num_teams = ["10","12"]
formats = {
            #"lm":"League Manager",
            "std":"Standard",
            #"espn_custom":"ESPN Custom"
          }

auction_budget = "260"



# for year in years:
#     for draft_type in draft_types:
#         for num_team in num_teams:
#             for lg_fm, league_format in formats.iteritems():
#                 for sc_type, scoring_type in scoring_types.iteritems():
for num_team in num_teams:
    for year in years:
        for draft_type, draft_collection in draft_types.iteritems():

            league_ids= list(league_settings.find(
                                    {
                                        "season":year,
                                        "Draft Type":draft_type,
                                        "Number of Teams":num_team,
                                        "Format":"Standard",
                                        "Scoring Type":"Rotisserie",
                                        #"Auction Budget":auction_budget,
                                    },
                                    {
                                        "_id":0,
                                        "league_id":1
                                    }
                                )
                            )

            collection_name = "{}_{}_{}_{}_{}".format(
                                                    year,
                                                    draft_type,
                                                    num_team,
                                                    "Standard",
                                                    "Rotisserie",
                                                    #auction_budget,
                                                )
            print collection_name

            if len(league_ids) > 0:

                ids =[d['league_id'] for d in league_ids]
                ids = list(set(ids))

                pipe = [{'$match':{'league_id':{'$in': ids},'season_id':year}},
                        { '$out' : collection_name }]

                db[collection_name].drop()
                draft_collection.aggregate(pipeline=pipe)

                print "{} {} piped!".format(len(ids),collection_name)
