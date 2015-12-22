import socket
import sys
from thread import *
import requests
import requests_oauthlib
import tweepy
from tweepy import OAuthHandler
import json
import oauth2 as oauth
from datetime import datetime
#Variables that contains the user credentials to access Twitter API
access_token = "582342005-QGM3VSdAL1cjAPzL6jaHebOHUfdqVtwddcHJhHBS"
access_token_secret = "keEVSlaNz5fegUq8ytMrTXq62paf41UI8KlH6aBH5DrWU"
consumer_key = "PjlYiBasD06wnMOH54cxwWnDO"
consumer_secret = "EXVZnDVb3wLA6KhwOfp9weBSngJEUi1TJxNvRZsW9yp3IJ3bL7"
auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret,access_token, access_token_secret)


HOST = ''   # Symbolic name meaning all available interfaces
PORT = 9999 # Arbitrary non-privileged port

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print 'Socket created'

#Bind socket to local host and port
try:
    s.bind((HOST, PORT))
except socket.error , msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    sys.exit()

print 'Socket bind complete'
import time
#Start listening on socket
s.listen(10)
print 'Socket now listening'

#Function for handling connections. This will be used to create threads
def clientthread(conn):
    url='https://stream.twitter.com/1.1/statuses/filter.json'
    data      = [('language', 'en'), ('locations', '-130,20,-60,50'),('track','ibm,google,microsoft')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in data])
    response  = requests.get(query_url, auth=auth, stream=True)
    print(query_url, response) # 200 <OK>
    count = 0
    for line in response.iter_lines():  # Iterate over streaming tweets
        try:
            if count > 10000000:
                break
            post= json.loads(line.decode('utf-8'))
            #contents = [post['text'], post['coordinates'], post['place']]
            count+= 1
            conn.send(line+'\n')
            #time.sleep(1)

            print (str(datetime.now())+' '+'count:'+str(count))
        except:
            e = sys.exc_info()[0]
            print( "Error: %s" % e )
    conn.close()


#now keep talking with the client
while 1:
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    print 'Connected with ' + addr[0] + ':' + str(addr[1])

    #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    start_new_thread(clientthread ,(conn,))

s.close()

