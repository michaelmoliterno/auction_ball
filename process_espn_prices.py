# import PyPDF2
# from PyPDF2 import PdfFileReader
import pymongo
from pymongo import MongoClient

connection = MongoClient()
db = connection['espn_draft_picks']
espn_prices = db['espn_prices']


DIR = '/home/mjfm/espn_scraper/espn_prices'

#r1110 = PdfFileReader(file( "{}/11_10_mlbdk2k11_300.pdf".format(DIR), "rb"))
# r1210 = PdfFileReader(file( "{}/12_10_mlbdk2k12_mix300CS.pdf".format(DIR), "rb"))
# r1310 = PdfFileReader(file( "{}/13_10_MLBDK2K13_mixed300.pdf".format(DIR), "rb"))
# r1410 = PdfFileReader(file( "{}/14_10_MLBDK2K14_CS_Mix300.pdf".format(DIR), "rb"))
# r1510 = PdfFileReader(file( "{}/15_10_Mix300CS.pdf".format(DIR), "rb"))

# listy = [r1110,
#          #r1210,r1310,r1410,r1510
#          ]
# for i, year in enumerate(listy):
#     with open("{}/text_vals/201{}_10_team".format(DIR,i+1),"w") as text_file:
#         text_file.write(year.getPage(0).extractText())

TEXT_DIR = "{}/text_vals".format(DIR)

file_2012 = '2012_10_team'
file_2013 = '2013_10_team'
file_2014 = '2014_10_team'
file_2015 = '2015_10_team'
file_2016 = '2016_10_12.txt'

# # #2016
# year = 2016
# with open("{}/{}".format(TEXT_DIR,file_2016)) as f:
#     for line in f:
#
#         player_dict = {}
#         splits =  line.split("\t")
#
#         rank_name = splits[0]
#         rank_name_split = rank_name.split(' ',1)
#
#
#         rank = int(rank_name_split[0].replace(".",""))
#         name = rank_name_split[1]
#
#
#         team = splits[1]
#         pos_rank = splits[2]
#
#
#         price_10 = splits[3]
#
#         if price_10 == '--':
#             price_10 = 0
#         else:
#             price_10 = int(price_10.replace("$",""))
#
#         #price_12 = splits[4]
#
#         player_dict["rank"] = rank
#         player_dict["name"] = name
#         player_dict["team"] = team
#         player_dict["price"] = price_10
#         player_dict["year"] = year
#         player_dict["pos_rank"] = pos_rank
#         player_dict["key"] = "{}-{}".format(year,rank)
#         print player_dict
#         espn_prices.insert_one(player_dict)

# #2015
# with open("{}/{}".format(TEXT_DIR,file_2015)) as f:
#     year = 2015
#     for i, line in enumerate(f):
#         if i%3 == 0:
#             player_dict = {}
#             #print 0, line
#             rank_summary = line.split(".")
#             rank = int(rank_summary[0].strip())
#             pos_rank = rank_summary[1].strip()
#             player_dict["pos_rank"] = pos_rank.replace("(","").replace(")","")
#             player_dict["year"] = year
#             player_dict["rank"] = rank
#             player_dict["key"] = "{}-{}".format(year,rank)
#         elif i%3 == 1:
#             #print 1, line
#             player_summary = line.split(",")
#             player_name = player_summary[0].strip()
#             player_team_pos = player_summary[1].strip()
#             player_dict["name"] = player_name
#             player_dict["team"] = player_team_pos.split(" ")[0].strip()
#             player_dict["pos"] = player_team_pos.split(" ")[1].strip().replace("(","").replace(")","")
#         elif i%3 == 2:
#             #print 2, line
#             price = line.replace("$","")
#             price = int(price)
#             player_dict["price"] = price
#             #print player_dict
#             espn_prices.insert_one(player_dict)


# # 2014
# with open("{}/{}".format(TEXT_DIR,file_2014)) as f:
#     year = 2014
#     for i, line in enumerate(f):
#         if i%3 == 0:
#             player_dict = {}
#             #print 0, line
#             rank_summary = line.split("-")
#             rank = int(rank_summary[0].strip())
#             pos_rank = rank_summary[1].strip()
#             player_dict["pos_rank"] = pos_rank
#             player_dict["year"] = year
#             player_dict["rank"] = rank
#             player_dict["key"] = "{}-{}".format(year,rank)
#         elif i%3 == 1:
#             #print 1, line
#             player_summary = line.split(",")
#             player_name = player_summary[0].strip()
#             player_team = player_summary[1].strip()
#             player_pos = player_summary[2]
#             player_dict["name"] = player_name
#             player_dict["team"] = player_team
#             player_dict["pos"] = player_pos
#         elif i%3 == 2:
#             #print 2, line
#             price = line.replace("$","")
#             price = int(price)
#             player_dict["price"] = price
#             print player_dict
#             espn_prices.insert_one(player_dict)

# 2012 - 2013
# with open("{}/{}".format(TEXT_DIR,file_2013)) as f:
#     year = 2013
#     for i, line in enumerate(f):
#         if i%3 == 0:
#             player_dict = {}
#             #print 0, line
#             rank = line.replace(".","")
#             rank = int(rank)
#             player_dict["year"] = year
#             player_dict["rank"] = rank
#             player_dict["key"] = "{}-{}".format(year,rank)
#         elif i%3 == 1:
#             #print 1, line
#             player_summary = line.split(",")
#             player_name = player_summary[0].strip()
#             player_team = player_summary[1].strip()
#             player_pos = player_summary[2:]
#             player_dict["name"] = player_name
#             player_dict["team"] = player_team
#             player_dict["pos"] = player_pos
#         elif i%3 == 2:
#             #print 2, line
#             price = line.replace("$","")
#             price = int(price)
#             player_dict["price"] = price
#             espn_prices.insert_one(player_dict)
