import datetime
from datetime import date
import time
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
## Connect to Tor for privacy purposes
# will need to change this
    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 9150, True)
    socket.socket = socks.socksocket
    print("connected to Tor!")

def soup_url(type_of_line, period_of_game, tdate=str(date.today()).replace('-', '')):
## get html code for odds based on desired line type and date
# most likely will need to change function to just accept a date

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

    # for testing:
    # tdate = '20160925'
    url = 'http://www.sportsbookreview.com/betting-odds/nfl-football/' +  \
        url_gametype + url_time_period + '?date=' + tdate
    print (url)
    now = datetime.datetime.now()
    raw_data = requests.get(url)
    soup_big = BeautifulSoup(raw_data.text, 'html.parser')
    # the oddsgridmodule might change mlb = 3 nlf = 16
    soup = soup_big.find_all('div', id='OddsGridModule_16')[0]
    timestamp = now.strftime("%H:%M:%S")

    return soup, timestamp

def replace_unicode(string):
    return string.replace(u'\xa0', ' ').replace(u'\xbd', '.5')

def parse_and_write_data(soup, date, time, not_ML=True):

# Parse HTML to gather line data by book
    def book_line(book_id, line_id, homeaway):
        ## Get Line info from book ID
        line = soup.find_all('div', attrs = {'class':'el-div eventLine-book', 'rel':book_id})[line_id].find_all('div')[homeaway].get_text().strip()
        return line
    '''
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
    #  need to decide what cols we want here for nfl, mostly the same except there is no line for ML
    if not_ML:
        df = pd.DataFrame(
                columns=('key', 'date', 'time', 'h/a',
                         'team', 'opp_team',
                         'pinnacle_line', 'pinnacle_odds',
                         '5dimes_line', '5dimes_odds',
                         'heritage_line', 'heritage_odds',
                         'bovada_line', 'bovada_odds',
                         'betonline_line', 'betonline_odds'))
    else:
        df = pd.DataFrame(
            columns=('key', 'date', 'time', 'h/a',
                     'team', 'opp_team', 'pinnacle', '5dimes',
                     'heritage', 'bovada', 'betonline'))
    # end of thought

    ## get line/odds info for unique book. Need error handling to account for blank data
        def try_except_book_line(id, i, x):
            try:
                return book_line(id, i, x)
            except IndexError:
                return ''

    counter = 0
    number_of_games = len(soup.find_all('div', attrs={'class':'el-div eventLine-rotation'}))
    for i in range(0, number_of_games):
        away_info_list = []
        home_info_list = []
        print(str(i+1) + '/' + str(number_of_games))

        ## Gather all useful data from unique books
        # consensus_data =  soup.find_all('div', 'el-div eventLine-consensus')[i].get_text()

        # find the team for away and home
        info_a = soup.find_all('div', attrs={'class':'el-div eventLine-team'})[i].find_all('div')[0].get_text().strip()
        hyphen_a = info_a.find('-')
        # paren_A = info_A.find("(")
        team_a = info_a[:hyphen_a - 1]

        info_h = soup.find_all('div', attrs={'class':'el-div eventLine-team'})[i].find_all('div')[2].get_text().strip()
        hyphen_h = info_h.find('-')
        # paren_H = info_H.find("(")
        team_h = info_h[:hyphen_h - 1]

        # generalize
        # home (1) and away (0)
        book_num_list = ['238', '19', '169', '999996', '1096']
        book_away = [0] * len(book_num_list)
        book_home = [0] * len(book_num_list)
        for j, num in enumerate(book_num_list):
            book_away[j] = try_except_book_line(num, i, 0)
            book_home[j] = try_except_book_line(num, i, 1)

        # pinnacle_A = try_except_book_line('238', i, 0)
        # fivedimes_A = try_except_book_line('19', i, 0)
        # heritage_A = try_except_book_line('169', i, 0)
        # bovada_A = try_except_book_line('999996', i, 0)
        # betonline_A = try_except_book_line('1096', i, 0)

        # # generalize
        # pinnacle_H = try_except_book_line('238', i, 1)
        # fivedimes_H = try_except_book_line('19', i, 1)
        # heritage_H = try_except_book_line('169', i, 1)
        # bovada_H = try_except_book_line('999996', i, 1)
        # betonline_H = try_except_book_line('1096', i, 1)

        # i think this is the key?
        away_info_list.append(str(date) + '_' + team_a.replace(u'\xa0', ' ') + '_' + team_h.replace(u'\xa0', ' '))
        away_info_list.append(date)
        away_info_list.append(time)
        away_info_list.append('away')
        away_info_list.append(team_a)
        away_info_list.append(team_h)

        # book_away_line = []
        # book_away_odds = []
        # book_home_line = []
        # book_home_odds = []

        # this one will be for away, next will be home maybe could combine but w/e
        for book_a in book_away:
            if not_ML:
                book_a = replace_unicode(book_a)
                book_line_a = book_a[:book_a.find(' ')]
                book_odds_a = book_a[book_a.find(' ') + 1:]
                away_info_list.append(book_line_a)
                away_info_list.append(book_odds_a)
            else:
                away_info_list.append(replace_unicode(book_a))

        # # generalize
        # if not_ML:
        #     pinnacle_A = replace_unicode(pinnacle_A)
        #     pinnacle_A_line = pinnacle_A[:pinnacle_A.find(' ')]
        #     pinnacle_A_odds = pinnacle_A[pinnacle_A.find(' ') + 1:]
        #     A.append(pinnacle_A_line)
        #     A.append(pinnacle_A_odds)
        #     fivedimes_A = replace_unicode(fivedimes_A)
        #     fivedimes_A_line = fivedimes_A[:fivedimes_A.find(' ')]
        #     fivedimes_A_odds = fivedimes_A[fivedimes_A.find(' ') + 1:]
        #     A.append(fivedimes_A_line)
        #     A.append(fivedimes_A_odds)
        #     heritage_A = replace_unicode(heritage_A)
        #     heritage_A_line = heritage_A[:heritage_A.find(' ')]
        #     heritage_A_odds = heritage_A[heritage_A.find(' ') + 1:]
        #     A.append(heritage_A_line)
        #     A.append(heritage_A_odds)
        #     bovada_A = replace_unicode(bovada_A)
        #     bovada_A_line = bovada_A[:bovada_A.find(' ')]
        #     bovada_A_odds = bovada_A[bovada_A.find(' ') + 1:]
        #     A.append(bovada_A_line)
        #     A.append(bovada_A_odds)
        #     betonline_A = replace_unicode(betonline_A)
        #     betonline_A_line = betonline_A[:betonline_A.find(' ')]
        #     betonline_A_odds = betonline_A[betonline_A.find(' ') + 1:]
        #     A.append(betonline_A_line)
        #     A.append(betonline_A_odds)
        # else:
        #     A.append(replace_unicode(pinnacle_A))
        #     A.append(replace_unicode(fivedimes_A))
        #     A.append(replace_unicode(heritage_A))
        #     A.append(replace_unicode(bovada_A))
        #     A.append(replace_unicode(betonline_A))

        home_info_list.append(str(date) + '_' + team_a.replace(u'\xa0', ' ') + '_' + team_h.replace(u'\xa0', ' '))
        home_info_list.append(date)
        home_info_list.append(time)
        home_info_list.append('home')
        home_info_list.append(team_h)
        home_info_list.append(team_a)

        for book_h in book_home:
            if not_ML:
                book_h = replace_unicode(book_h)
                book_line_h = book_h[:book_h.find(' ')]
                book_odds_h = book_h[book_h.find(' ') + 1:]
                away_info_list.append(book_line_h)
                away_info_list.append(book_odds_h)
            else:
                away_info_list.append(replace_unicode(book_h))

    #    if not_ML:
    #         pinnacle_H = replace_unicode(pinnacle_H)
    #         pinnacle_H_line = pinnacle_H[:pinnacle_H.find(' ')]
    #         pinnacle_H_odds = pinnacle_H[pinnacle_H.find(' ') + 1:]
    #         H.append(pinnacle_H_line)
    #         H.append(pinnacle_H_odds)
    #         fivedimes_H = replace_unicode(fivedimes_H)
    #         fivedimes_H_line = fivedimes_H[:fivedimes_H.find(' ')]
    #         fivedimes_H_odds = fivedimes_H[fivedimes_H.find(' ') + 1:]
    #         H.append(fivedimes_H_line)
    #         H.append(fivedimes_H_odds)
    #         heritage_H = replace_unicode(heritage_H)
    #         heritage_H_line = heritage_H[:heritage_H.find(' ')]
    #         heritage_H_odds = heritage_H[heritage_H.find(' ') + 1:]
    #         H.append(heritage_H_line)
    #         H.append(heritage_H_odds)
    #         bovada_H = replace_unicode(bovada_H)
    #         bovada_H_line = bovada_H[:bovada_H.find(' ')]
    #         bovada_H_odds = bovada_H[bovada_H.find(' ') + 1:]
    #         H.append(bovada_H_line)
    #         H.append(bovada_H_odds)
    #         betonline_H = replace_unicode(betonline_H)
    #         betonline_H_line = betonline_H[:betonline_H.find(' ')]
    #         betonline_H_odds = betonline_H[betonline_H.find(' ') + 1:]
    #         H.append(betonline_H_line)
    #         H.append(betonline_H_odds)
    #     else:
    #         H.append(replace_unicode(pinnacle_H))
    #         H.append(replace_unicode(fivedimes_H))
    #         H.append(replace_unicode(heritage_H))
    #         H.append(replace_unicode(bovada_H))
    #         H.append(replace_unicode(betonline_H))

        ## Take data from A and H (lists) and put them into DataFrame
        df.loc[counter] = ([away_info_list[j] for j in range(len(away_info_list))])
        df.loc[counter+1] = ([home_info_list[j] for j in range(len(home_info_list))])
        counter += 2
    return df

def select_and_rename(df, text):
    ## Select only useful column names from a DataFrame
    ## Rename column names so that when merged, each df will be unique

    if text[-2:] == 'ml':
        df = df[['key', 'time', 'team', 'opp_team', 'pinnacle',
                 '5dimes', 'heritage', 'bovada', 'betonline']]
    ## Change column names to make them unique
        df.columns = ['key', text + '_time', 'team', 'opp_team',
                      text + '_PIN', text + '_FD', text + '_HER', text + '_BVD', text + '_BOL']
    else:
        df = df[['key', 'time', 'team', 'opp_team',
                 'pinnacle_line', 'pinnacle_odds',
                 '5dimes_line', '5dimes_odds',
                 'heritage_line', 'heritage_odds',
                 'bovada_line', 'bovada_odds',
                 'betonline_line', 'betonline_odds']]
        df.columns = ['key', text + '_time', 'team', 'opp_team',
                      text + '_PIN_line', text + '_PIN_odds',
                      text + '_FD_line', text + '_FD_odds',
                      text + '_HER_line', text + '_HER_odds',
                      text + '_BVD_line', text + '_BVD_odds',
                      text + '_BOL_line', text + '_BOL_odds']
    return df

def main():
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
                print("getting today's {} {} ({}/9)".format(t_type, l_type, counter + 1))
            except:
                soup_list[counter] = ''
                time_list[counter] = ''
                print("couldn't get today's {} {}".format(t_type, l_type))

            counter += 1

    #testing BS part uncomment below for parsing
# will need to run thru names
    line_names = ['sf', 'sfh', 'ssh', 'mlf', 'mlfh', 'mlsh', 'tf', 'tfh', 'tsh']
    ml_bool_list = [True, True, True, False, False, False, True, True, True]
    write_df = pd.DataFrame()

    # loop (call parse_and_write_data and select_and_rename) then merge to df
    for i, soup_l in enumerate(soup_list):
        print("writing today's {} ({}/9)".format(line_names[i], i + 1))
        df_data_temp = parse_and_write_data(soup_l, todays_date, time_list[i], not_ML=ml_bool_list[i])
        df_data_temp = select_and_rename(df_data_temp, line_names[i])
        # merge into write df
        write_df = write_df.merge(df_data_temp, how='left', on = ['key', 'team', 'opp_team'])

    # #### Each df_xx creates a data frame for a bet type
    # print("writing today's MoneyLine (1/6)")
    # df_ml = parse_and_write_data(soup_ml, todays_date, time_ml, not_ML = False)
    # ## Change column names to make them unique
    # df_ml.columns = ['key','date','ml_time','h/a','team','pitcher',
    #                  'hand','opp_team','opp_pitcher','opp_hand',
    #                  'ml_PIN','ml_FD','ml_HER','ml_BVD','ml_BOL']

    # print("writing today's RunLine (2/6)")
    # df_rl = parse_and_write_data(soup_rl, todays_date, time_rl)
    # df_rl = select_and_rename(df_rl, 'rl')

    # print("writing today's totals (3/6)")
    # df_tot = parse_and_write_data(soup_tot, todays_date, time_tot)
    # df_tot = select_and_rename(df_tot, 'tot')
    
    # print("writing today's 1st-half MoneyLine (4/6)")
    # df_1h_ml = parse_and_write_data(soup_1h_ml, todays_date, time_1h_ml, not_ML = False)
    # df_1h_ml = select_and_rename(df_1h_ml,'1h_ml')
    
    # # print("writing today's 1st-half RunLine (5/6)")
    # # df_1h_rl = parse_and_write_data(soup_1h_rl, todays_date, time_1h_rl)
    # # df_1h_rl = select_and_rename(df_1h_rl,'1h_rl')
    
    # # print("writing today's 1st-half totals (6/6)")
    # # df_1h_tot = parse_and_write_data(soup_1h_tot, todays_date, time_1h_tot)
    # # df_1h_tot = select_and_rename(df_1h_tot,'1h_tot')
    
    # ## Merge all DataFrames together to allow for simple printout
    # write_df = df_ml
    # write_df = write_df.merge(
    #             df_rl, how='left', on = ['key','team','pitcher','hand','opp_team'])
    # write_df = write_df.merge(
    #             df_tot, how='left', on = ['key','team','pitcher','hand','opp_team'])
    # write_df = write_df.merge(
    #             df_1h_ml, how='left', on = ['key','team','pitcher','hand','opp_team'])
    # # write_df = write_df.merge(
    # #             df_1h_rl, how='left', on = ['key','team','pitcher','hand','opp_team'])
    # # write_df = write_df.merge(
    # #             df_1h_tot, how='left', on = ['key','team','pitcher','hand','opp_team'])
    
    # # with open('\SBR_MLB_Lines.csv', 'a') as f:
    # write_df.to_csv('test_mlb.csv', index=False)#, header = False)
  
if __name__ == '__main__':
    main()
