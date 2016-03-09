import pymongo
from pymongo import MongoClient

connection = MongoClient()
db = connection['espn_draft_picks']

#auction_drafts = db['auction_drafts']
snake_drafts = db['snake_drafts']

pipe = [{'$group':{'_id':'$player_id', 'total':{'$toLower':'$player_name'}}},
        { '$out' : "player_universe" }
        ]
snake_drafts.aggregate(pipeline=pipe)
print 'done!'