from bs4 import BeautifulSoup
import urllib2
import sys
import random
import time
from cloudsql import dbCursor
import pymongo
from pymongo import MongoClient

env = 'gce'

def get_unsearched_leagues(season):

    string = """
    select league_id
    from league_seasons
    where coalesce(draft_complete,valid,draft_type,header,processed,openable) is null
    and season_id = {}
    """.format(season)

    with dbCursor(env) as cursor:
        cursor.execute(string)
        active_terms_list = [item[0] for item in cursor.fetchall()]

    return active_terms_list


def get_leagues_with_drafts(season):

    string = """
    select league_id
    from league_seasons
    where draft_type is not null
    and season_id = {}
    """.format(season)

    with dbCursor(env) as cursor:
        cursor.execute(string)
        active_terms_list = [item[0] for item in cursor.fetchall()]

    return active_terms_list


def get_league_season_draft(league_id,season_id):
        draft_url = "http://games.espn.go.com/flb/tools/draftrecap?leagueId={}&seasonId={}".format(league_id,season_id)
        page = urllib2.urlopen(draft_url)
        soup = BeautifulSoup(page,"lxml")
        return soup, draft_url


def get_league_season_settings(league_id,season_id):
        draft_url = "http://games.espn.go.com/flb/leaguesetup/settings?leagueId={}&seasonId={}".format(league_id,season_id)
        page = urllib2.urlopen(draft_url)
        soup = BeautifulSoup(page,"lxml")
        return soup, draft_url


def get_league_season_standings(league_id,season_id):
        draft_url = "http://games.espn.go.com/flb/standings?leagueId={}&seasonId={}".format(league_id,season_id)
        page = urllib2.urlopen(draft_url)
        soup = BeautifulSoup(page,"lxml")
        return soup, draft_url


def get_league_season_trancounter(league_id,season_id):
        draft_url = "http://games.espn.go.com/flb/tools/transactioncounter?leagueId={}&seasonId={}".format(league_id,season_id)
        page = urllib2.urlopen(draft_url)
        soup = BeautifulSoup(page,"lxml")
        return soup, draft_url


def update_league(league_id,field,value):
    string = """
        UPDATE leagues set {} = {}
        WHERE league_id = {}
    """.format(field,value,league_id)
    with dbCursor(env) as cursor:
        cursor.execute(string)


def update_league_season(league_id,season_id,field,value):
    string = """
        UPDATE league_seasons set {} = {}
        WHERE league_id = {} and season_id = {}
    """.format(field,value,league_id,season_id)
    with dbCursor(env) as cursor:
        cursor.execute(string)


def get_league_settings(soup):

    league_settings = {}

    for i, table in enumerate(soup.find("table").findAll("table")):

        ##roster settings
        if table.find('td').text == 'Roster':
            for p in table.find("td", {"class": "dataSummary settingLabel"}).findAll('p'):
                vals = p.text.split(":")
                league_settings[vals[0]] = vals[1]

        ##scoring settings
        elif table.find('td').text == 'Scoring':
            stats = []
            for tr in table.findAll("td", {"class": "statName"}):
                stat = tr.text
                stat = stat[stat.find("(")+1:stat.find(")")]
                stats.append(stat)
            league_settings["stats"] = stats

        #position settings
        elif table.find('td').text == 'Position':
            positions = {}
            for tr in table.findAll('tr')[1:]:
                position_long = tr.next_element.text
                position = position_long[position_long.find("(")+1:position_long.find(")")]
                positions[position] = tr.next_element.nextSibling.text
            league_settings["positions"] = positions

        ##other settings
        elif table.find('td').text in  ['Basic Settings','Keepers Rules','Draft Settings']:
            for j, tr in enumerate(table.findAll('tr')):
                if j==0:
                    continue
                else:
                    league_settings[tr.next_element.text] = tr.next_element.nextSibling.text
        else:
            pass

    return league_settings


if __name__ == "__main__":

    season = 2014

    leagues_with_drafts = get_leagues_with_drafts(season)

    connection = MongoClient()
    db = connection['espn_draft_picks']
    league_settings_db = db['league_settings']

    for league in leagues_with_drafts:

        n = float(random.random())/100
        time.sleep(n)

        sys.stdout.write("Processing league {}\n".format(league))

        try:
            soup, draft_url = get_league_season_settings(league,season)
        except urllib2.URLError as e:
            sys.stderr.write("error opening {}".format(league))
            sys.stderr.write(e.message)
            continue

        league_settings = get_league_settings(soup)
        league_settings["league_id"] = str(league)
        league_settings["season"] = str(season)
        league_settings["key"] = "{}-{}".format(league,season)

        try:
            league_settings_db.insert_one(league_settings)
        except pymongo.errors.DuplicateKeyError as e:
            sys.stderr.write("error saving {} settings, duplicate key".format(league))
            sys.stderr.write(e.message)
            continue



