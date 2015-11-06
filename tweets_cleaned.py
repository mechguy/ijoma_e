
#Enyinnaya Ijoma
#Date: November, 2015
#Insight Coding Challenge

# Import the necessary methods from tweepy library
from __future__ import unicode_literals
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
from pprint import pprint


# loads Twitter credentials from .twitter file that is in the same directory as this script
file_dir = os.path.dirname(os.path.realpath(__file__)) 
with open(file_dir + '/.twitter') as twitter_file:  
    twitter_cred = json.load(twitter_file)

access_token = twitter_cred["access_token"]
access_token_secret = twitter_cred["access_token_secret"]
consumer_key = twitter_cred["consumer_key"]
consumer_secret = twitter_cred["consumer_secret"]

class StdOutListener(StreamListener):
    """ A listener handles tweets that are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, filename):
        self.filename = filename

    # this is the event handler for new data
    def on_data(self, data):
        if not os.path.isfile(self.filename):    # check if file doesn't exist
            f = file(self.filename, 'w')
            f.close()
        with open(self.filename, 'ab') as f:
            print "writing to {}".format(self.filename)
            f.write(data)
        f.closed
        
    # this is the event handler for errors    
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    listener = StdOutListener(file_dir + "/tweets.txt")
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    print "Use CTRL + C to exit at any time.\n"
    stream = Stream(auth, listener)
    stream.filter(locations=[-180,-90,180,90]) # this is the entire world, any tweet with geo-location enabled

   # print "Use CTRL + C to exit at any time.\n"
    stream = Stream(auth, l)
    stream.filter(track=[
        '#BigData', '#Spark', '#ApacheKafka', '#ApacheStorm', '#ApacheSpark', 'Data Engineering', 'Hadoop', 'MapReduce',
        'Mahout', 'MLlib', 'Logstash', 'RabbitMQ', 'Fluentd', 'AWS', 'Avro', '#Samza', 'HBase', '#Cassandra', 'MongoDB',
        'Elasticsearch', 'Kibana', 'Neo4j', 'CouchDB', 'Redis', 'Memcached', '#Hive', '#ApacehPig', 'Cascalog', 'Giraph',
        '#Presto', '#Impala', '#ApacheDrill', 'GraphX', 'GraphLab', '#Redshift', 'Solr', '#Riak', 'Hazelcast'
        ])
        
#####start first by uploading json and pandas#####

import os
import json
import pandas as pd
import matplotlib.pyplot as plt
        
        
     #read the data in into an array
    
       stream_data_path = '../data/twitter_data.txt'
    
    stream_data = []
    stream_file = open(stream_data_path, "r")
    for line in stream_file:
        try:
            tweet = json.loads(line)
            stream_data.append(tweet)
        except:
            continue
    

  
  # structure the tweets data into a pandas DataFrame to simplify the data manipulation
  
    tweets = pd.DataFrame()
    tweets['text'] = map(lambda tweet: tweet['text'], stream_data)
    tweets['hashtag'] = map(lambda tweet: tweet['lang'], stream_data)
    
  
  
   # We will create a chart for hastag
  
   tweets_by_hashtag = tweets['lang'].value_counts()
  
  fig, ax = plt.subplots()
  ax.tick_params(axis='x', labelsize=15)
  ax.tick_params(axis='y', labelsize=10)
  ax.set_xlabel('Languages', fontsize=15)
  ax.set_ylabel('Number of tweets' , fontsize=15)
  ax.set_title('Top 5 languages', fontsize=15, fontweight='bold')
tweets_by_lang[:5].plot(ax=ax, kind='bar', color='red')


# calculate the total number of times each word has been tweeted

def tweeted_words():
    dictionary={}  #set up an empty dictionary
    read_file=open(r'tweet_input\tweets.txt','r')  # program open the text file for reading later and assign to the object
    write_file=open(r'tweet_output\ft1.txt','w')   # set up object(write_file) for writing to the text file
    text=read_file.read() # all what the object read from the text file assign to the object "text"
    words=text.split() # separate text into words by space delimiter
    
    
    for input_word in words: # looping through words
        if input_word not in dictionary: # if word is not in the dictionary
            dictionary[input_word]=1     # assign value 1 to the word(key)
        else:
            dictionary[input_word]+=1    # if a word already in dictionary then increment value by 1   

    wordsList=[(v,k) for v, k in dictionary.items()] # 'transfer' dictionary to list
    wordsList.sort() # sort the list by word (which is key) in alphabetic order 
    for word in wordsList:  # looping through list  
        write_file.write("%25s:%4d"%(word[0],word[1])+'\n')  # write output to the text file in formated order 
                                                             #(first key(word) and then his  value)
    write_file.close()    # close text file for writing 
    read_file.close()     # close text file for reading
    
    
        
tweeted_words() # initiate program run


# The program calculate the median number of unique words per tweet, and updates this median as tweets come in

def unique_words():
    summary=0
    tweet_count=0
    Grand_Sum=0.0
    words_dict={}  #set up an empty dictionary
    read_file=open(r'tweet_input\tweets.txt','r') # open file for reading
    write_file=open(r'tweet_output\ft2.txt','w')  # open file for writing
    line_text=read_file.readlines() # read lines from the read file and holds all lines in the "line_text" object
    
    for line in line_text: # looping through lines
        for words in line.split(): # looping through each line and holds all words of the current line in the object "words" 
            if words not in words_dict: # looking for unique words
                    summary+=1 # collect unique words in each tweet
                    words_dict[words]=1 # fill up dictionary by unique words
        tweet_count+=1 # keep tracking of the tweets number
        Grand_Sum+=summary # collect all unique words
        write_file.write('%3.1f'%(Grand_Sum/tweet_count)+'\n')  #write the median number after each tweet in the file
        summary=0         # clean up the number of unique words of current tweet
        words_dict={}     # clean up current tweet dictionary of unique words
    write_file.close()    # close text file for writing 
    read_file.close()     # close text file for reading 
    
    
