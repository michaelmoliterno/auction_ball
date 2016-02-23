from bs4 import BeautifulSoup
import os
import urlparse
from cloudsql import dbCursor
import pymongo
from pymongo import MongoClient
import cloudsql
import sys

def get_drafts(season_id,draft_type = 'Auction Draft'):
    with dbCursor(env) as cursor:
        cursor.execute("""
          SELECT league_id
          FROM league_seasons
          WHERE season_id = {} and draft_type = '{}'
          """.format(season_id,draft_type))
        active_terms_list = [int(item[0]) for item in cursor.fetchall()]
        return active_terms_list

def get_league_draft_picks(draft_results):

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

if __name__ == "__main__":
    env = 'gce'
    season = 2015

    connection = MongoClient()
    db = connection['espn_draft_picks']

    ### auctions
    auction_drafts = db['auction_drafts']

    all_season_auctions =  get_drafts(season, 'Auction Draft')

    processed_season_auctions = list(auction_drafts.find({"season_id": unicode(season)}).distinct('league_id'))
    processed_season_auctions = [int(x) for x in processed_season_auctions]

    unprocessed_season_auctions = list(set(all_season_auctions)-set(processed_season_auctions))

    print "total auctions:",len(all_season_auctions)
    print "already processed:",len(processed_season_auctions)
    print "unprocessed:",len(unprocessed_season_auctions)

    for league in processed_season_auctions:
        cloudsql.update_league_season(league,season,"processed",1)

    dir = "/home/mjfm/espn_scraper/draft_files/{}".format(season)

    for draft_num,draft_recap in enumerate(os.listdir(dir)):

        if int(draft_recap) in unprocessed_season_auctions:
            sys.stdout.write("processing {}".format(draft_recap))
            draft_file = open('%s/%s'%(dir,draft_recap),'r')
            soup = BeautifulSoup(draft_file,"lxml")

            league_picks = get_league_draft_picks(soup)

            try:
                auction_drafts.insert_many(league_picks)
                cloudsql.update_league_season(int(draft_recap),season,"processed",1)
            except pymongo.errors.BulkWriteError as e:
                sys.stderr.write(e.message)

