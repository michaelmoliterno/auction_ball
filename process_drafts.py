from bs4 import BeautifulSoup
import os
import urlparse
from cloudsql import dbCursor
import pymongo
from pymongo import MongoClient
import cloudsql
import sys

env = 'gce'
season = 2014
type = 'snake'

def get_drafts(season_id,draft_type):
    with dbCursor(env) as cursor:
        cursor.execute("""
          SELECT league_id
          FROM league_seasons
          WHERE season_id = {} and draft_type = '{}'
          """.format(season_id,draft_type))
        active_terms_list = [int(item[0]) for item in cursor.fetchall()]
        return active_terms_list

def get_auction_league_draft_picks(draft_results):

    league_picks = []

    for team_num, team in enumerate(draft_results.findAll('table')[1:]):

        team_name = team.find('a').text.strip()
        team_link = team.find('a')['href']
        parsed=urlparse.urlparse(team_link)
        team_id=urlparse.parse_qs(parsed.query)['teamId'][0]
        season_id = urlparse.parse_qs(parsed.query)['seasonId'][0]
        league_id = urlparse.parse_qs(parsed.query)['leagueId'][0]

        for pick_num, pick_details in enumerate(team.findAll('tr')[1:]):
            for i, pick_detail in enumerate(pick_details.findAll('td')):
                if i == 0:
                    try:
                        nom_order = int(pick_detail.text.strip())
                    except ValueError as e:
                        nom_order = -1
                        sys.stderr.write(e.message)
                elif i== 2:
                    price_string = pick_detail.text.strip()
                    price = int(price_string.replace('$',''))
                elif i == 1:
                    player_link = pick_detail.find('a')
                    if not player_link == None:
                        player_name = player_link.text.strip()
                        player_id = player_link['playerid']
                        unique_team_id = player_link['teamid']
                    else:
                        player_name = ''
                        player_id = -9999999999
                        unique_team_id = -9999999999

            pick_dict = {"key":"{}-{}-{}-{}-{}".format(season_id,league_id,team_id,nom_order,player_id),
                        "league_id":league_id,
                        "team_name":team_name,
                        "team_id":team_id,
                        "unique_team_id":unique_team_id,
                        "season_id":season_id,
                        "nom_order":nom_order,
                        "player_name":player_name,
                        "player_id":player_id,
                        "player_name_id":"%s (%s)"%(player_name, player_id),
                        "price":price}

            league_picks.append(pick_dict)

    return league_picks

def get_snake_league_draft_picks(draft_results):

    league_picks = []

    for round_num, draft_round in enumerate(draft_results.findAll('table')[1:]):

        for pick_num, pick in enumerate(draft_round.findAll('tr',{"class":"tableBody"})):

            for i, td in enumerate(pick.findAll('td')):
                if i == 0:
                    pick_num= int(td.text)
                elif i == 1:
                    player_link = td.find('a')
                    if not player_link == None:
                        player_name = player_link.text.strip()
                        player_id = player_link['playerid']
                        unique_team_id = player_link['teamid']
                    else:
                        player_name = ''
                        player_id = -9999999999
                        unique_team_id = -9999999999
                elif i == 2:
                    team = td.find('a')
                    team_link = team['href']
                    team_name=team.text.strip()
                    parsed=urlparse.urlparse(team_link)
                    team_id=urlparse.parse_qs(parsed.query)['teamId'][0]
                    season_id = urlparse.parse_qs(parsed.query)['seasonId'][0]
                    league_id = urlparse.parse_qs(parsed.query)['leagueId'][0]


            pick_dict = {"key":"{}-{}-{}-{}-{}".format(season_id,league_id,team_id,pick_num,player_id),
                            "league_id":league_id,
                            "team_name":team_name,
                            "team_id":team_id,
                            "unique_team_id":unique_team_id,
                            "season_id":season_id,
                            "player_name":player_name,
                            "player_id":player_id,
                            "pick_num":pick_num,
                            "round_num":round_num+1}

            league_picks.append(pick_dict)

    return league_picks

def get_processed_unprocessed(drafts_collection,season,all_season_leagues):
    processed_season_leagues = list(drafts_collection.find({"season_id": unicode(season)}).distinct('league_id'))
    processed_season_leagues = [int(x) for x in processed_season_leagues]
    unprocessed_season_leagues = list(set(all_season_leagues)-set(processed_season_leagues))
    return processed_season_leagues, unprocessed_season_leagues

def get_leagues_by_season(season, drafts_collection, type):

    all_season_leagues =  get_drafts(season, type)

    processed_season_leagues, unprocessed_season_leagues = \
        get_processed_unprocessed(drafts_collection,season,all_season_leagues)

    return processed_season_leagues, unprocessed_season_leagues

if __name__ == "__main__":

    sys.stdout.write("processing {} for {}".format(type,season))

    connection = MongoClient()
    db = connection['espn_draft_picks']

    if type == 'auction':
        drafts_collection = db['auction_drafts']
        type = 'Auction Draft'
        getter = get_auction_league_draft_picks
    elif type == 'snake':
        drafts_collection = db['snake_drafts']
        type = 'Snake Draft'
        getter = get_snake_league_draft_picks

    processed_season_leagues, unprocessed_season_leagues = \
        get_leagues_by_season(season, drafts_collection, type)

    for league in processed_season_leagues:
        cloudsql.update_league_season(league,season,"processed",1)

    dir = "/home/mjfm/espn_scraper/draft_files/{}".format(season)

    #draft_files = os.listdir(dir)

    #for draft_num,draft_recap in enumerate(draft_files):
    for draft_num, draft_recap in enumerate(unprocessed_season_leagues):

        "processing draft {} of {}".format(draft_num+1, len(unprocessed_season_leagues))

        #if int(draft_recap) in unprocessed_season_leagues:

        sys.stdout.write("processing {} {}".format(type,draft_recap))
        draft_file = open('%s/%s'%(dir,draft_recap),'r')
        soup = BeautifulSoup(draft_file,"lxml")
        league_picks = getter(soup)

        try:
            drafts_collection.insert_many(league_picks)
            cloudsql.update_league_season(int(draft_recap),season,"processed",1)

        except pymongo.errors.BulkWriteError as e:

            sys.stderr.write("error processing {}".format(draft_recap))
            sys.stderr.write(e.message)
