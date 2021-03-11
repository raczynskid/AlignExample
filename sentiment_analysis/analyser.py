import nltk
import sqlite3
import pandas as pd
import os
from nltk.sentiment import SentimentIntensityAnalyzer

class SentimentAnalyzer:
    def __init__(self, data: pd.DataFrame):
        self.sia = SentimentIntensityAnalyzer()
        self.stopwords = nltk.corpus.stopwords.words("english")
        self.data = data
        self.blobs = None

    def calculate_sentiment(self):
        self.data["polarity_score"] = self.data["body"].apply(lambda x: self.sia.polarity_scores(x)["compound"])

    def load_blobs(self):
        # group by subs
        subreddit_groups = self.data.groupby(by="subreddit")

        # create text blobs per sub
        words = {}
        for subreddit, df in subreddit_groups:
            # tokenize into wordlists per subreddit, drop stopwords
            words[subreddit] = nltk.word_tokenize(" ".join(df["body"].tolist()))


        self.blobs = words

    def basic_stats(self):
        if self.blobs is None:
            self.load_blobs()
        d = {}
        for sub, blob in self.blobs.items():
            d[sub] = nltk.FreqDist([w for w in blob
                                    if (w.lower() not in self.stopwords)
                                    and (w.isalpha())]).most_common(5)

        return d


def import_corpora():
    # load posts data from cache

    con = sqlite3.connect(os.path.abspath("./posts_scraper/cache.db"))
    reddit = pd.read_sql("SELECT * FROM reddit_data", con)
    twitter = pd.read_sql("SELECT * FROM twitter_data", con)
    con.close()

    return reddit, twitter

reddit, twitter = import_corpora()
sa = SentimentAnalyzer(reddit)

print(sa.basic_stats())
