import nltk
import sqlite3
import pandas as pd
import os

def import_corpora():
    # load posts data from cache

    con = sqlite3.connect(os.path.abspath("./posts_scraper/cache.db"))
    reddit = pd.read_sql("SELECT * FROM reddit_data", con)
    twitter = pd.read_sql("SELECT * FROM twitter_data", con)
    con.close()

    return reddit, twitter

def tst():

    stopwords = nltk.corpus.stopwords.words("english")
    words = [w for w in nltk.corpus.state_union.words() if w.isalpha()] # split into words
    words = [w for w in words if w.lower() not in stopwords] # remove stopwords

    fd = nltk.FreqDist(words) # frequency distribution
    fd.most_common(3)
    fd.tabulate(5)

    lower_fd = nltk.FreqDist([w.lower() for w in fd])

import_corpora()