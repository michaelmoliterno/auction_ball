from bs4 import BeautifulSoup
import urllib2
import sys
import random
import time
from cloudsql import dbCursor
import cloudsql

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


def get_leagues_with_incomplete_drafts(season):

    string = """
    select league_id
    from league_seasons
    where draft_complete = 0
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



if __name__ == "__main__":

    season = 2012

    leagues_with_drafts_2015 = get_leagues_with_drafts(2015)
    leagues_without_drafts_2015 = get_leagues_with_incomplete_drafts(2015)

    #for league in range(1,150000):
    for league in leagues_without_drafts_2015:

        n = float(random.random())/1000
        time.sleep(n)

        sys.stdout.write("Processing league {}\n".format(league))

        try:
            soup, draft_url = get_league_season_draft(league,season)
        except urllib2.URLError as e:
            update_league_season(league,season,"openable",0)
            sys.stderr.write("error opening {}".format(league))
            sys.stderr.write(e.message)
            continue

        update_league_season(league,season,"openable",1)

        try:
            header = soup.find("div", {"class": "games-pageheader"}).h1.text
        except AttributeError as e:
            header = "ERROR_FINDING_HEADER"
            update_league_season(league,season,"header",0)
            sys.stderr.write(e.message)
            with open('/home/mjfm/espn_scraper/draft_files/{}_soup/{}_{}'.format(season,league,header), 'w') as f:
                for line in soup.prettify('utf-8', 'minimal'):
                    f.write(str(line))
            continue

        update_league_season(league,season,"header",1)

        if header == "Log In":
            update_league(league,"public_{}".format(season),0)
            update_league_season(league,season,"public",0)
            continue
        else:
            update_league(league,"public_{}".format(season),1)
            update_league_season(league,season,"public",1)

        if header == "We're Sorry":

            error = soup.find("div", {"class": "games-alert-mod alert-mod2 games-error-red-alert"}).text

            if error:

                if error == 'League Draft Not Complete.':
                    update_league_season(league,season,"draft_complete",0)

                elif error == 'Invalid league specified.':
                    update_league_season(league,season,"valid",0)

        else:
            update_league_season(league,season,"valid",1)
            update_league_season(league,season,"draft_complete",1)

            draft = soup.find("div", {"class": "games-fullcol games-fullcol-extramargin"})

            draft_table = draft.find('table')

            with open('/home/mjfm/espn_scraper/draft_files/{}/{}'.format(season,league), 'w') as f:
                for line in draft_table.prettify('utf-8', 'minimal'):
                    f.write(str(line))

            draft_deets = draft.find("div", {"class": "games-alert-mod alert-mod2 games-grey-alert"})
            for deet_type in draft_deets.findAll('b'):
                if str(deet_type.text).strip() == "Type:":
                    draft_type = deet_type.nextSibling.strip()
                    update_league_season(league,season,"draft_type","'{}'".format(draft_type))