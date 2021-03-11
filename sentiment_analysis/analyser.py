import nltk
import sqlite3
import pandas as pd
import os
from posts_scraper.Scraper import import_corpora
from nltk.sentiment import SentimentIntensityAnalyzer


class SentimentAnalyzer:
    def __init__(self, data: pd.DataFrame):
        self.sia = SentimentIntensityAnalyzer()
        self.stopwords = nltk.corpus.stopwords.words("english")
        self.data = data
        self.blobs = None

    def basic_cleaning(self):
        self.data["body"].apply(lambda x: x.replace(r"\n", " "))
        self.data["body"].apply(lambda x: x.replace(r"  ", " "))

    def calculate_sentiment(self):
        self.data["polarity_score"] = self.data["body"].apply(lambda x: self.sia.polarity_scores(x)["compound"])
        self.apply_weights()

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

    def apply_weights(self):
        self.data["weighted_polarity_score"] = self.data["polarity_score"] * self.data["score"]

    def subreddit_counts(self, include_default_sub=True):
        if include_default_sub:
            return self.data["subreddit"].value_counts()
        else:
            return self.data.loc[self.data["subreddit"] != "Invisalign"]["subreddit"].value_counts()

if __name__ == '__main__':

    reddit, twitter = import_corpora("posts_scraper/cache.db")
    sa = SentimentAnalyzer(reddit)

    t = sa.subreddit_counts(include_default_sub=False)

