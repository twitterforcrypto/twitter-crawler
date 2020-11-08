from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from redis import Redis
from rq import Queue
from dateutil.parser import parse
import pandas as pd
import json
import time
import re
import sys
import requests
import io
import os
import schedule
import time
import ssl
import shutil

def scraper():
    try:
        ckey= ""
        csecret= ""
        atoken= ""
        asecret= ""

        class listener(StreamListener):

            def on_data(self, data):

                try:
                    #Set relevant variables
                    all_data = json.loads(data)   
                    ttime = (time.strftime("%H:%M:%S"))
                    date = (time.strftime("%d/%m/%Y"))

                    ## FINDING THE TWEET TEXT
                    # Regular Tweet
                    tweet = all_data['text']

                    #Obtain if Retweeted
                    retweeted = False
                    if tweet.startswith('RT'):
                        retweeted = True

                    #Obtain source
                    source = all_data["source"]
                    soup = BeautifulSoup(source, "lxml")
                    source = soup.find('a').text

                    # Check for Extended tweets
                    if "extended_tweet" in all_data:
                        try:
                            tweet = all_data['extended_tweet']['full_text']
                        except:
                            pass

                    # Check for Extended or Regular tweets in Retweets
                    if "retweeted_status" in all_data:
                        try:
                            tweet = all_data['retweeted_status']['text']
                        except:
                            pass
                        try:
                            tweet = all_data['retweeted_status']['extended_tweet']['full_text']
                        except:
                            pass

                    #User variables
                    user_default_profile_image = all_data["user"]["default_profile_image"]
                    user_id = all_data["user"]["id"]
                    user_verified = all_data["user"]["verified"]
                    user_followers_count = all_data["user"]["followers_count"]
                    user_statuses_count = all_data["user"]["statuses_count"]
                    user_following_count = all_data["user"]["friends_count"]
                    user_favourites_count = all_data["user"]["favourites_count"]
                    user_location = all_data["user"]["location"]
                    user_actualname  = all_data["user"]["name"]
                    user_name = all_data["user"]["screen_name"]
                    user_lang = all_data["user"]["lang"]
                    user_created_at = all_data["user"]["created_at"]
                    in_reply_to_user_id = all_data['in_reply_to_user_id']
                    in_reply_to_status_id = all_data['in_reply_to_status_id']
                    tweettext = tweet

                    # #Obtain URLs from tweet and read and extend shortlinks to full URLs. Then, remove any URL from tweettext
                    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', tweet)
                    if not urls:
                        extendedurl = None
                        urls = None
                    else:
                        extendedurl = []
                        for url1 in urls:
                            extendedurl.append(url1)
                            #try:
                            #    req = requests.get(url1).url
                            #    extendedurl.append(req)
                            #except httplib.IncompleteRead as e:
                            #    req = e.partial
                            #    extendedurl.append(req)
                            #    pass
                        tweet = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', "", tweet)

                    #Obtain hashtags
                    hashtags = [tag.strip("#") for tag in tweettext.split() if tag.startswith("#")]
                    tags = []
                    lenhashtags = 0
                    if not hashtags:
                        tags = None
                    else:
                        for item in hashtags:
                            tags.append(item)
                        lenhashtags= len(hashtags)

                    #Obtain mentions
                    ment = [mention.strip("@") for mention in tweettext.split() if mention.startswith('@')]  
                    mentions = []
                    lenmentions = 0
                    if not ment:
                        mentions = None 
                    else:
                        for item1 in ment:
                            if ':' in item1:
                                item1 = item1[:(len(item1)-1)]  
                            mentions.append(item1)
                        lenmentions = len(mentions)

                    #Add variables to one row in pandas DataFrame
                    row = pd.DataFrame({"date": [date], "time" : [ttime], "user_name" : [user_name], "text": [tweet], "hashtags" : [tags], "number_of_hashtags" : [lenhashtags], "mentions" : [mentions],
                        "number_of_mentions" : [lenmentions], "retweeted" : [retweeted], "in_reply_to_status_id" : [in_reply_to_status_id], "in_reply_to_user_id" : [in_reply_to_user_id], "user_id" : [user_id], "user_followers_count" : [user_followers_count], "user_statuses_count" : [user_statuses_count],
                        "user_following_count" : [user_following_count], "user_verified" : [user_verified], "user_favourites_count" : [user_favourites_count],
                        "user_location" : [user_location], "user_default_profile_image" : [user_default_profile_image], "user_actualname" : [user_actualname], 
                        "user_lang" : [user_lang], "user_created_at" : [user_created_at], "urls" : [extendedurl], "source" : [source]})

                    try:
                        with open('data.csv','a', errors='ignore') as datafr:
                            row = row[['date', 'time', 'user_name', 'text', 'hashtags', 'number_of_hashtags', 'mentions', 'number_of_mentions', 'retweeted', 'in_reply_to_status_id', 'in_reply_to_user_id', 'user_id', 'user_followers_count', 'user_statuses_count', 'user_following_count', 
                            'user_verified', 'user_favourites_count', 'user_location', 'user_default_profile_image', 'user_actualname', 'user_lang', 'user_created_at', 'urls', 'source']]
                            row.to_csv(datafr, header=False, sep=';', index=False, encoding='ansi')
                    except:
                        print("error1")
                        pass

                    return True

                except:
                    print("error2")
                    pass

            def on_error(self, status):
                print(status)

        auth = OAuthHandler(ckey, csecret)
        auth.set_access_token(atoken, asecret)

        #stream all data with the search terms in strings
        twitterStream = Stream(auth, listener(), tweet_mode='extended')
        twitterStream.filter(track=['queryterm1', 'queryterm2'], languages=['en'])

    except:
        print("error3")
        pass

try:
    schedule.every(10).minutes.do(scraper)
    while 1:
        schedule.run_pending()
        time.sleep(0.001)
except:
  pass
