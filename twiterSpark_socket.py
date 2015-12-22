#!/usr/bin/env python2.7
#http://will-farmer.com/twitter-civil-unrest-analysis-with-apache-spark.html
"""
Twitter Panic!

Real time monitoring of civil disturbance through the Twitter API

NOTE, ONLY PYTHON 2.7 IS SUPPORTED
"""
from __future__ import print_function
import os
import sys
import ast
import json

import re
import string
import requests
import matplotlib.pyplot as plt
import threading
import Queue
import time
import requests_oauthlib
#import cartopy.crs as ccrs
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from pylab import rcParams
import numpy as np
import multiprocessing
# import nltk
# from nltk.tokenize import word_tokenize
# from nltk.corpus import stopwords
# Path for spark source folder
os.environ['SPARK_HOME']="/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/"
# Append the python dir to PYTHONPATH so that pyspark could be found
sys.path.append('/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/python/')
# Append the python/build to PYTHONPATH so that py4j could be found
sys.path.append('/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/python/lib/py4j-0.8.2.1-src.zip')
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml.feature import HashingTF,IDF, Tokenizer

access_token = "582342005-QGM3VSdAL1cjAPzL6jaHebOHUfdqVtwddcHJhHBS"
access_token_secret = "keEVSlaNz5fegUq8ytMrTXq62paf41UI8KlH6aBH5DrWU"
consumer_key = "PjlYiBasD06wnMOH54cxwWnDO"
consumer_secret = "EXVZnDVb3wLA6KhwOfp9weBSngJEUi1TJxNvRZsW9yp3IJ3bL7"
auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret,access_token, access_token_secret)

BATCH_INTERVAL = 10  # How frequently to update (seconds)
BLOCKSIZE = 500  # How many tweets per update



def data_plotting(q):
    plt.ion() # Interactive mode
    #fig = plt.figure(figsize=(30, 30))
    llon = -130
    ulon = -60
    llat = 20
    ulat = 50
    rcParams['figure.figsize'] = (14,10)
    my_map = Basemap(projection='merc',
                resolution = 'l', area_thresh = 1000.0,
                llcrnrlon=llon, llcrnrlat=llat, #min longitude (llcrnrlon) and latitude (llcrnrlat)
                urcrnrlon=ulon, urcrnrlat=ulat) #max longitude (urcrnrlon) and latitude (urcrnrlat)

    my_map.drawcoastlines()
    my_map.drawcountries()
    my_map.drawmapboundary()
    my_map.fillcontinents(color = 'white', alpha = 0.3)
    my_map.shadedrelief()
    # xs,ys = my_map(np.asarray(-100), np.asarray(30))
    plt.pause(0.0001)
    plt.show()
    a=1
    # To create a color map
    clusterNum=3
    colors = plt.get_cmap('jet')(np.linspace(0.0, 1.0, clusterNum))

    while True:
        if q.empty():
            #xs,ys = my_map(np.asarray(-100), np.asarray(30))
            #a=(-1.0)*a
            #my_map.plot(xs, ys,  marker='o', markersize= 20+a*10, alpha = 0.75)
            #plt.draw()
            time.sleep(5)

        else:
            obj=q.get()
            d=[x[0] for x in obj]
            c=[x[1] for x in obj]
            data = np.array(d)
            pcolor=np.array(c)
            print(c)
            try:

                #ax.scatter(data[:, 0], data[:, 1], transform=ccrs.PlateCarree())
                xs,ys = my_map(data[:, 0], data[:, 1])
                #print (xs)
                #print (ys)
                my_map.scatter(xs, ys,  marker='o', alpha = 0.5,color=colors[pcolor])
                plt.pause(0.0001)
                plt.draw()
                time.sleep(5)
            except IndexError: # Empty array
                pass




def get_coord2(post):
    coord = tuple()
    try:
        if post['coordinates'] == None:
            coord = post['place']['bounding_box']['coordinates']
            coord = reduce(lambda agg, nxt: [agg[0] + nxt[0], agg[1] + nxt[1]], coord[0])
            coord = tuple(map(lambda t: t / 4.0, coord))
        else:
            coord = tuple(post['coordinates']['coordinates'])
    except TypeError:
        #print ('error get_coord')
        coord=(0,0)
    return coord

def get_coord(line):
    """
    Converts each object into /just/ the associated coordinates

    :param line: list
        List from dataset
    """
    coord = tuple()
    try:
        if line[1] == None:
            coord = line[2]['bounding_box']['coordinates']
            coord = reduce(lambda agg, nxt: [agg[0] + nxt[0], agg[1] + nxt[1]], coord[0])
            coord = tuple(map(lambda t: t / 4.0, coord))
        else:
            coord = tuple(line[1]['coordinates'])
    except TypeError:
        print ('error get_coord')
        coord=(0,0)
    return coord


def get_location(post_array):
    try:
        if post_array[3] == None:
            location=None
        else:
            location = post_array[3]['location']
    except TypeError:
        print ('error get_location')
        location=None
    return location

def decompose(line):
    try:
        post= json.loads(line.decode('utf-8'))
        contents = [post['text'], post['coordinates'], post['place'],post['user']]
        return contents
    except:
        #e = sys.exc_info()[0]
        return(None)

def test():
    x=0;
    while True:
        x=x+1
        time.sleep(3)
        print(x)

def parse(lp):
    label = float(lp[lp.find('(') + 1: lp.find(',')])
    vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))
    return LabeledPoint(label, vec)

# def parse2(lp):
#     label = float(1)
#     vec = Vectors.dense(lp[])
#     return LabeledPoint(label, vec)

def get_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError, e:
    return False
  return json_object

def tokenize(text):
    tokens = []
    text = text.encode('ascii', 'ignore') #to decode
    text=re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*(),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text) # to replace url with ''
    text=text.lower()
    for word in text.split():
        if word \
            not in string.punctuation \
            and len(word)>1 \
            and word != '``':
                tokens.append(word)
    return tokens

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.mllib.feature import StandardScaler
if __name__ == '__main__':
    q = multiprocessing.Queue()
    job_for_another_core2 = multiprocessing.Process(target=data_plotting,args=(q,))
    job_for_another_core2.start()
    # Set up spark objects and run
    sc  = SparkContext('local[4]', 'Social Panic Analysis')
    # Create a local StreamingContext with two working thread and batch interval of 1 second
    # x=[1,2,3]
    # yrdd=sc.parallelize(x)
    ssc = StreamingContext(sc, 20)
    #ssc.checkpoint("checkpoint")
    # Create a DStream that will connect to hostname:port, like localhost:9999
    dstream = ssc.socketTextStream("localhost", 9999)
    #dstream_tweets.count().pprint()
    #
    dstream_tweets=dstream.map(lambda post: get_json(post))\
         .filter(lambda post: post != False)\
        .filter(lambda post: 'created_at' in post)\
         .map(lambda post: [get_coord2(post)[0],get_coord2(post)[1]])\
         .filter(lambda post: post[0] != 0)

    #dstream_tweets=dstream.map(lambda line: decompose(line))
        #.map(lambda line: ast.literal_eval(line))

    #dstream_tweets.pprint()
    trainingData=dstream_tweets
    #trainingData.pprint()
    model = StreamingKMeans(k=3, decayFactor=0.1).setRandomCenters(2, 1.0, 3)
    model.trainOn(trainingData)
    testdata=dstream_tweets.map(lambda x: ([x[0],x[1]],[x[0],x[1]]))
        #.map(parse)
    #testdata.pprint()
    clust=model.predictOnValues(testdata)
    #testdata.join(clust).pprint()
    #clust.pprint()
    #print(model.predictOnValues(yrdd))

    # Analysis

    clust.foreachRDD(lambda time, rdd: q.put(rdd.collect()))
    #print (clust.count())


    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    # To count distinct users
    # dstream_userid_count=dstream_tweets.map(lambda post_array: post_array[3]['id'])\
    #     .map(lambda userid: (str(userid),1))\
    #     .updateStateByKey(updateFunc)
    #dstream_userid_count.pprint()


    # To count distinct locations and their tweets
    # dstream_location_count=dstream_tweets.map(get_location)\
    #     .map(lambda loc: (loc,1))\
    #     .updateStateByKey(updateFunc)


    # dstream_userid=dstream_tweets.map(lambda post_array: post_array[3]['id'])\
    #     .map(lambda userid: (str(userid),1))

    # Reduce last 30 seconds of data, every 10 seconds
    # wintweetcount = dstream_tweets.countByWindow(20, 10)
    # wintweetcount.pprint()

    #dstream_location_count.pprint(30)
    #dstream_location_count.filter(lambda t:t[0]==None).pprint()
    #dstream_location_count.foreachRDD(lambda time, rdd: print (rdd.collect()))
    #dstream_coord.pprint()
    # Convert to something usable....

    #dstream_coord.foreachRDD(lambda time, rdd: print (str(time)+ '-' +str(rdd.count())))

    # Run!
    ssc.start()
    ssc.awaitTermination()
