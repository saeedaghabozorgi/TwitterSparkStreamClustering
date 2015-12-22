from multiprocessing import Process
import nltk
import time


def child_fn():
    print "Fetch URL"
    import urllib2
    print urllib2.urlopen("https://www.google.com").read()[:100]
    print "Done"


while True:
    child_process = Process(target=child_fn)
    child_process.start()
    child_process.join()
    print "Child process returned"
    time.sleep(1)