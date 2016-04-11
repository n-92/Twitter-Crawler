"""
Author : Aung Naing Oo
Description : Script to crawl twitter and search for food keyword related topics
Purpose : In partial fulfilment of 4034 Assignment

"""
from twython import Twython
import xlrd
import urllib
import re
import json
from xlwt import Workbook
from xlutils.copy import copy
import threading
import time
import sys

class FilePaths:
    """ This class will hold all the file paths"""
    def __init__(self):
        pass

    tweets_text_file = './tweets.txt'
    tweets_xl_file ='./generated_Tweets.xls'

class KeyWords:
    tweet_sheet = "Tweets"
    pizza_sheet= "Pizza"
    drink_sheet="Drink"
    soup_sheet="Soup"
    chicken_sheet="Chicken"
    salad_sheet="Salad"
    steak_sheet="Steak"

    statuses = 'statuses'
    created_at = 'created_at'
    content= 'text'
    tweet_id='id'
    food = 'food'
    tweets = 'tweets'
    pizza = 'pizza'
    drink = 'drink'
    soup = 'soup'
    chicken = 'chicken'
    salad = 'salad'
    steak = 'steak'

class Writer:
    """Responsible for Reading from and Writing out to
    flat files"""
    def __init__(self,f):
        self.path = f

    def writeOut(self, dat):
        with open(self.path, "w") as tweet_file:
            json.dump(dat, tweet_file)


    def getTweetsFromFile(self, loc='tweets.txt'):
        return json.load(open(loc,'r'))

class RepeatEvery(threading.Thread):
    """Class used to run a function, as a thread 
        for a certain number of times."""

    def __init__(self, interval, func, *args, **kwargs):
        threading.Thread.__init__(self)
        self.interval = interval  # seconds between calls
        self.func = func          # function to call
        self.args = args          # optional positional argument(s) for call
        self.kwargs = kwargs      # optional keyword argument(s) for call
        self.runable = True

    def run(self):
        while self.runable:
            self.func(*self.args, **self.kwargs)
            time.sleep(self.interval)

    def stop(self):
        self.runable = False


class ExcelFunctions:
    """Contains all the necessary functions that are used to deal with
        xls file"""

    def __init__(self):
        pass

    def openExcel(self, excel_file):
        """Open Excel file"""

        try:
            data = xlrd.open_workbook(excel_file)
            self.wdata = copy(data)
            return data
        except Exception as e:
            print str(e)
            sys.exit(0)

    def openSheet(self, excel_file, sheet_name):
        """Open Excel Tab
        """

        table = excel_file.sheet_by_name(sheet_name)
        self.writable_sheet = self.wdata.get_sheet(0)
        return table

    def getColumnData(self,xl_sheet,col_index):
        return xl_sheet.col_values(col_index)

    def insertIntoSheet(self,l, col_val,tweet_count):
        """ This is the function used to insert Data into Excel Sheet"""
        last_non_empty_row = tweet_count+1
        row = last_non_empty_row + 1
        for element in l:
            self.writable_sheet.write(row, col_val,element)
            row = row+1
        self.wdata.save(FilePaths.tweets_xl_file)

class TwitterSearcher:

    def __init__(self):
        #Dictionary to contain all food and drinks
        self.food_dict = {'pizza':[],'drink':[], 'soup':[], 'chicken':[], 'salad':[], 'steak':[] }
        self.APP_KEY ="2xFtaWli8rritPbvEaeLBoEe7"
        self.ACCESS_TOKEN = "AAAAAAAAAAAAAAAAAAAAAH2yuAAAAAAA%2BOyVpueDQg0mE57fdm7LwXamoeM%3D8ZlsVwP93kez9pcPCwGdHKTAhFLyI9PvTNvlytWi2hN3w0ZL9J"
        self.twitter_object = Twython(self.APP_KEY, access_token=self.ACCESS_TOKEN)
#Lists to be inserted into Excel File Later on
        self.statuses = []
        self.days=[]
        self.tweet_ids=[]
        self.tweet_times=[]
        self.topics=[]

    def populateFoodDictionary(self, keywords, l):
        refined_list = []
        for element in l:
            print element
            e = element.strip().lower()
            refined_list.append(urllib.quote(e)) #containing either food or bad or both

        if (keywords == KeyWords.pizza):
            self.food_dict['pizza'] = refined_list
        elif (keywords == KeyWords.drink):
            self.food_dict['drink'] = refined_list
        elif (keywords == KeyWords.soup):
            self.food_dict['soup'] = refined_list
        elif (keywords == KeyWords.chicken):
            self.food_dict['chicken'] = refined_list
        elif (keywords == KeyWords.salad):
            self.food_dict['salad'] = refined_list
        elif (keywords == KeyWords.steak):
            self.food_dict['steak'] = refined_list


    def getFoodDictionary(self):
        return self.food_dict

    def pullTweets(self, l):
        #Searching can be done whether you're authenticated via OAuth1 or OAuth2
        tweets = []
        for query in l:
            print query
            tweet = self.twitter_object.search(q=query, result_type='mixed',lang='en',locale='en' )
            print(json.dumps(tweet))
            tweets.append(tweet)
        return tweets

    def listPopulate(self, tweet_handle, topic):
        for l in tweet_handle:
            for key, value in l.iteritems():
                if type(l[KeyWords.statuses]) is list:
                    for x in l[KeyWords.statuses]:
                        self.statuses.append(x[KeyWords.content])
                        self.tweet_times.append(x[KeyWords.created_at])
                        self.days.append(x[KeyWords.created_at].split()[0])
                        self.tweet_ids.append(x[KeyWords.tweet_id])
                        if topic == KeyWords.pizza:
                            self.topics.append(KeyWords.pizza)
                        elif topic == KeyWords.drink:
                            self.topics.append(KeyWords.drink)
                        elif topic == KeyWords.soup:
                            self.topics.append(KeyWords.soup)
                        elif topic == KeyWords.chicken:
                            self.topics.append(KeyWords.chicken)
                        elif topic == KeyWords.salad:
                            self.topics.append(KeyWords.salad)
                        elif topic == KeyWords.steak:
                            self.topics.append(KeyWords.steak)


def crawl_activity():
    """P.S: This section needs clearing up because of laziness, I just mechanically 
        copied and pasted many times."""

#Load all the functions
    xl_object = ExcelFunctions()
    writer_object = Writer(FilePaths.tweets_text_file)
    twitter_object = TwitterSearcher()
#End of Loading

    parsed_xl_tweets = xl_object.openExcel(FilePaths.tweets_xl_file)
    parsed_tweets_sheet = xl_object.openSheet(parsed_xl_tweets,KeyWords.tweet_sheet) 
    parsed_pizza_list = xl_object.openSheet(parsed_xl_tweets, KeyWords.pizza_sheet)
    parsed_drink_list = xl_object.openSheet(parsed_xl_tweets, KeyWords.drink_sheet)
    parsed_soup_list = xl_object.openSheet(parsed_xl_tweets, KeyWords.soup_sheet)
    parsed_chicken_list = xl_object.openSheet(parsed_xl_tweets, KeyWords.chicken_sheet)
    parsed_salad_list = xl_object.openSheet(parsed_xl_tweets, KeyWords.salad_sheet)
    parsed_steak_list = xl_object.openSheet(parsed_xl_tweets, KeyWords.steak_sheet)

#Start for Pizza
    twitter_object.populateFoodDictionary(KeyWords.pizza,xl_object.getColumnData(parsed_pizza_list,0)[1:])
    json_tweets = twitter_object.pullTweets(twitter_object.getFoodDictionary()[KeyWords.pizza])
    writer_object.writeOut(json_tweets)

    twitter_object.listPopulate(writer_object.getTweetsFromFile(), KeyWords.pizza)

    xl_object.insertIntoSheet(twitter_object.topics, col_val =0, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.days, col_val =5, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.tweet_ids, col_val =2, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.tweet_times, col_val =3, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.statuses, col_val =4, tweet_count = len(xl_object.getColumnData(parsed_tweets_sheet,1)))

#Start for Drinks
#    twitter_object.populateFoodDictionary(KeyWords.drink,xl_object.getColumnData(parsed_drink_list,0)[1:])
#    json_tweets = twitter_object.pullTweets(twitter_object.getFoodDictionary()[KeyWords.drink])
#    writer_object.writeOut(json_tweets)
#
#    twitter_object.listPopulate(writer_object.getTweetsFromFile(),KeyWords.drink)
#
#    xl_object.insertIntoSheet(twitter_object.topics, col_val =0, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
#    xl_object.insertIntoSheet(twitter_object.days, col_val =1, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
#    xl_object.insertIntoSheet(twitter_object.tweet_ids, col_val =2, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
#    xl_object.insertIntoSheet(twitter_object.tweet_times, col_val =3, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
#    xl_object.insertIntoSheet(twitter_object.statuses, col_val =4, tweet_count = len(xl_object.getColumnData(parsed_tweets_sheet,1)))

#Start for Chicken
    twitter_object.populateFoodDictionary(KeyWords.chicken,xl_object.getColumnData(parsed_chicken_list,0)[1:])
    json_tweets = twitter_object.pullTweets(twitter_object.getFoodDictionary()[KeyWords.chicken])
    writer_object.writeOut(json_tweets)

    twitter_object.listPopulate(writer_object.getTweetsFromFile(),KeyWords.chicken)

    xl_object.insertIntoSheet(twitter_object.topics, col_val =0, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.days, col_val =5, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.tweet_ids, col_val =2, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.tweet_times, col_val =3, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.statuses, col_val =4, tweet_count = len(xl_object.getColumnData(parsed_tweets_sheet,1)))

#Start for Soup
    twitter_object.populateFoodDictionary(KeyWords.soup,xl_object.getColumnData(parsed_soup_list,0)[1:])
    json_tweets = twitter_object.pullTweets(twitter_object.getFoodDictionary()[KeyWords.soup])
    writer_object.writeOut(json_tweets)

    twitter_object.listPopulate(writer_object.getTweetsFromFile(),KeyWords.soup)

    xl_object.insertIntoSheet(twitter_object.topics, col_val =0, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.days, col_val =5, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.tweet_ids, col_val =2, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.tweet_times, col_val =3, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.statuses, col_val =4, tweet_count = len(xl_object.getColumnData(parsed_tweets_sheet,1)))

#Start for Salad
    twitter_object.populateFoodDictionary(KeyWords.salad,xl_object.getColumnData(parsed_salad_list,0)[1:])
    json_tweets = twitter_object.pullTweets(twitter_object.getFoodDictionary()[KeyWords.salad])
    writer_object.writeOut(json_tweets)

    twitter_object.listPopulate(writer_object.getTweetsFromFile(),KeyWords.salad)

    xl_object.insertIntoSheet(twitter_object.topics, col_val =0, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.days, col_val =5, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.tweet_ids, col_val =2, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.tweet_times, col_val =3, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.statuses, col_val =4, tweet_count = len(xl_object.getColumnData(parsed_tweets_sheet,1)))

#Start for Steak
    twitter_object.populateFoodDictionary(KeyWords.steak,xl_object.getColumnData(parsed_steak_list,0)[1:])
    json_tweets = twitter_object.pullTweets(twitter_object.getFoodDictionary()[KeyWords.steak])
    writer_object.writeOut(json_tweets)

    twitter_object.listPopulate(writer_object.getTweetsFromFile(),KeyWords.steak)

    xl_object.insertIntoSheet(twitter_object.topics, col_val =0, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.days, col_val =5, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.tweet_ids, col_val =2, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.tweet_times, col_val =3, tweet_count=len(xl_object.getColumnData(parsed_tweets_sheet,1)))
    xl_object.insertIntoSheet(twitter_object.statuses, col_val =4, tweet_count = len(xl_object.getColumnData(parsed_tweets_sheet,1)))


if __name__ == "__main__":
    thread = RepeatEvery(450, crawl_activity) #crawl every 450 seconds #just to be safe, since there is a restriction of 
                                                #approximately querying in a 15 minute window
    print "starting"
    thread.start()
    thread.join(21600)  #run for n hours
    thread.stop()
    print "stopped"
