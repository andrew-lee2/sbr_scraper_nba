import datetime
from datetime import date
import socket
import socks
import win_inet_pton
import requests
from bs4 import BeautifulSoup
import pandas as pd

####
# after we see that this is working will need to refactor
# then will need to write part that creates database up until existing games
# and then one that updates for cron job
###

# Andrew Lee
# 11.27.16
# nfl mod for sbr scraper

def connectTor():
    '''
    Connect to Tor for privacy purposes
    will need to change this
    '''
    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 9150, True)
    socket.socket = socks.socksocket
    print "connected to Tor!"

def soup_url(type_of_line, period_of_game, tdate=str(date.today()).replace('-', '')):
    '''
    inputs: type of line (string), period_of_game (string) time period
    get html code for odds based on desired line type and date
    most likely will need to change function to just accept a date
    '''

    # dict to map type of lines and period of game
    web_dict = {
        'spread' : '',
        'mline' : 'money-line/',
        'total' : 'totals/',
        'full' : '',
        'first_half' : '1st-half/',
        'second_half' : '2nd-half/'
        }

    url_gametype = web_dict[type_of_line]
    url_time_period = web_dict[period_of_game]

    # this is the url for nba, change here for other sports
    url = 'http://www.sportsbookreview.com/betting-odds/nfl-football/' +  \
        url_gametype + url_time_period + '?date=' + tdate

    raw_data = requests.get(url)
    soup_big = BeautifulSoup(raw_data.text, 'html.parser')

    # the oddsgridmodule changes, mlb = 3 nlf = 16
    soup = soup_big.find_all('div', id='OddsGridModule_16')[0]
    # add in time stamp for when data was scraped
    now = datetime.datetime.now()
    timestamp = now.strftime("%H:%M:%S")

    return soup, timestamp

def replace_unicode(string):
    '''
    takes a string and replaces formating with decimal numbers
    '''
    return string.replace(u'\xa0', ' ').replace(u'\xbd', '.5')

def map_team_names(team_name):
    '''
    input string, output string
    maps city to team city abbrev
    '''
    team_map_dict = {
        'Arizona'  : 'ARI',
        'Atlanta' : 'ATL',
        'Baltimore' : 'BAL',
        'Buffalo' : 'BUF',
        'Carolina' : 'CAR',
        'Chicago' : 'CHI',
        'Cincinnati' : 'CIN',
        'Cleveland' : 'CLE',
        'Dallas' : 'DAL',
        'Denver' : 'DEN',
        'Detroit' : 'DET',
        'Green Bay' : 'GB',
        'Houston' : 'HOU',
        'Indianapolis' : 'IND',
        'Jacksonville' : 'JAC',
        'Kansas City' : 'KC',
        'Los Angeles' : 'LAM',
        'Miami' : 'MIA',
        'Minnesota' : 'MIN',
        'New England' : 'NE',
        'New Orleans' : 'NO',
        'N.Y. Giants' : 'NYG',
        'N.Y. Jets' : 'NYJ',
        'Oakland' : 'OAK',
        'Philadelphia' : 'PHI',
        'Pittsburgh' : 'PIT',
        'San Diego' : 'SD',
        'Seattle' : 'SEA',
        'San Francisco' : 'SF',
        'St. Louis' : 'STL',
        'Tampa Bay' : 'TB',
        'Tennessee' : 'TEN',
        'Washington' : 'WAS'
    }
    return team_map_dict[team_name]

def parse_and_write_data(soup, date, time, not_ml=True):

# Parse HTML to gather line data by book
    def book_line(book_id, line_id, homeaway):
        '''
        Get Line info from book ID
        needs book_id, type of line, and if team is home or away

        BookID  BookName
        238     Pinnacle
        19      5Dimes
        93      Bookmaker
        1096    BetOnline
        169     Heritage
        123     BetDSI
        999996  Bovada
        139     Youwager
        999991  SIA
        '''
        line = soup.find_all('div', attrs={'class':'el-div eventLine-book', 'rel':book_id})[line_id].find_all('div')[homeaway].get_text().strip()
        return line
    '''
    
    '''
    #  need to decide what cols we want here for nfl, mostly the same except there is no line for ML
    if not_ml:
        df_info = pd.DataFrame(
            columns=('key', 'date', 'time', 'h/a',
                     'team', 'opp_team',
                     'pinnacle_line', 'pinnacle_odds',
                     '5dimes_line', '5dimes_odds',
                     'heritage_line', 'heritage_odds',
                     'bovada_line', 'bovada_odds',
                     'betonline_line', 'betonline_odds'))
    else:
        df_info = pd.DataFrame(
            columns=('key', 'date', 'time', 'h/a',
                     'team', 'opp_team', 'pinnacle', '5dimes',
                     'heritage', 'bovada', 'betonline'))
    # end of thought

    ## get line/odds info for unique book. Need error handling to account for blank data
    def try_except_book_line(book_id, i, h_a):
        '''
        tries to find the book line
        need to deal with what happens when nothign is returned
        '''
        try:
            return book_line(book_id, i, h_a)
        except IndexError:
            return ''

    counter = 0
    number_of_games = len(soup.find_all('div', attrs={'class':'el-div eventLine-rotation'}))
    for i in range(0, number_of_games):
        away_info_list = []
        home_info_list = []
        print str(i+1) + '/' + str(number_of_games)

        ## Gather all useful data from unique books
        # consensus_data =  soup.find_all('div', 'el-div eventLine-consensus')[i].get_text()

        # find the team for away and home
        team_a = soup.find_all('div', attrs={'class':'el-div eventLine-team'})[i].find_all('div')[0].get_text().strip()
        team_h = soup.find_all('div', attrs={'class':'el-div eventLine-team'})[i].find_all('div')[1].get_text().strip()

        # right here write and call new function that will map to abbrevs
        team_a = map_team_names(team_a)
        team_h = map_team_names(team_h)

        # home (1) and away (0)
        book_num_list = ['238', '19', '169', '999996', '1096']
        book_away = [0] * len(book_num_list)
        book_home = [0] * len(book_num_list)
        for j, num in enumerate(book_num_list):
            book_away[j] = try_except_book_line(num, i, 0)
            book_home[j] = try_except_book_line(num, i, 1)

        # i think this is the key?
        away_info_list.append(str(date) + '_' + team_a.replace(u'\xa0', ' ') + '_' + team_h.replace(u'\xa0', ' '))
        away_info_list.append(date)
        away_info_list.append(time)
        away_info_list.append('away')
        away_info_list.append(team_a)
        away_info_list.append(team_h)

        # this one will be for away, next will be home maybe could combine but w/e
        for book_a in book_away:
            if not_ml:
                book_a = replace_unicode(book_a)
                book_line_a = book_a[:book_a.find(' ')]
                book_odds_a = book_a[book_a.find(' ') + 1:]
                away_info_list.append(book_line_a)
                away_info_list.append(book_odds_a)
            else:
                away_info_list.append(replace_unicode(book_a))

        home_info_list.append(str(date) + '_' + team_a.replace(u'\xa0', ' ') + '_' + team_h.replace(u'\xa0', ' '))
        home_info_list.append(date)
        home_info_list.append(time)
        home_info_list.append('home')
        home_info_list.append(team_h)
        home_info_list.append(team_a)

        for book_h in book_home:
            if not_ml:
                book_h = replace_unicode(book_h)
                book_line_h = book_h[:book_h.find(' ')]
                book_odds_h = book_h[book_h.find(' ') + 1:]
                home_info_list.append(book_line_h)
                home_info_list.append(book_odds_h)
            else:
                home_info_list.append(replace_unicode(book_h))

        ## Take data from A and H (lists) and put them into DataFrame
        df_info.loc[counter] = ([away_info_list[j] for j in range(0, len(away_info_list))])
        df_info.loc[counter+1] = ([home_info_list[j] for j in range(0, len(home_info_list))])
        counter += 2

    return df_info

def select_and_rename(df_info, line_str):
    '''
    inputs: df of line info, line_str is the type of line in string
    Select only useful column names from a DataFrame
    Rename column names so that when merged, each df will be unique
    '''

    if line_str[:2] == 'ml':
        df_info = df_info[['key', 'time', 'team', 'opp_team', 'pinnacle',
                           '5dimes', 'heritage', 'bovada', 'betonline']]
    ## Change column names to make them unique
        df_info.columns = ['key', line_str + '_time', 'team', 'opp_team',
                           line_str + '_PIN', line_str + '_FD', line_str + '_HER',
                           line_str + '_BVD', line_str + '_BOL']
    else:
        df_info = df_info[['key', 'time', 'team', 'opp_team',
                           'pinnacle_line', 'pinnacle_odds',
                           '5dimes_line', '5dimes_odds',
                           'heritage_line', 'heritage_odds',
                           'bovada_line', 'bovada_odds',
                           'betonline_line', 'betonline_odds']]
        df_info.columns = ['key', line_str + '_time', 'team', 'opp_team',
                           line_str + '_PIN_line', line_str + '_PIN_odds',
                           line_str + '_FD_line', line_str + '_FD_odds',
                           line_str + '_HER_line', line_str + '_HER_odds',
                           line_str + '_BVD_line', line_str + '_BVD_odds',
                           line_str + '_BOL_line', line_str + '_BOL_odds']
    return df_info

def main():
    '''
    main function will need to modify in order to build full database and cron job
    '''

    connectTor()

    # something wrong with the scraper with tor

    ## Get today's lines
    # todays_date = str(date.today()).replace('-', '')
    ## change todays_date to be whatever date you want to pull in the format 'yyyymmdd'
    ## One could force user input and if results in blank, revert to today's date.
    todays_date = '20161120'

    ## store BeautifulSoup info for parsing
    # testing BS parts need to rename some vars

    line_types = ['spread', 'mline', 'total']
    time_types = ['full', 'first_half', 'second_half']
    # apparently this is pep 8
    soup_list = [0] * len(line_types) * len(time_types)
    time_list = [0] * len(line_types) * len(time_types)
    counter = 0

    for l_type in line_types:
        for t_type in time_types:
            try:
                soup_list[counter], time_list[counter] = soup_url(l_type, t_type, todays_date)
                print "getting today's {} {} ({}/9)".format(t_type, l_type, counter + 1)
            except:
                soup_list[counter] = ''
                time_list[counter] = ''
                print "couldn't get today's {} {}".format(t_type, l_type)

            counter += 1

    # testing BS part uncomment below for parsing
    # will need to run thru names
    line_names = ['sf', 'sfh', 'ssh', 'mlf', 'mlfh', 'mlsh', 'tf', 'tfh', 'tsh']
    ml_bool_list = [True, True, True, False, False, False, True, True, True]

    write_df = pd.DataFrame()

    # loop (call parse_and_write_data and select_and_rename) then merge to df
    for i, soup_l in enumerate(soup_list):
        print "writing today's {} ({}/9)".format(line_names[i], i + 1)
        df_data_temp = parse_and_write_data(soup_l, todays_date, time_list[i], not_ml=ml_bool_list[i])
        df_data_temp = select_and_rename(df_data_temp, line_names[i])
        # merge into write df
        if write_df.empty:
            write_df = df_data_temp
        else:
            write_df = write_df.merge(df_data_temp, how='left', on=['key', 'team', 'opp_team'])

    # # with open('\SBR_MLB_Lines.csv', 'a') as f:
    # write_df.to_csv('test_mlb.csv', index=False)#, header = False)

if __name__ == '__main__':
    main()
