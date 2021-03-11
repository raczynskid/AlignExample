import nltk
import sqlite3
import pandas as pd
import os
from posts_scraper.Scraper import import_corpora
from nltk.sentiment import SentimentIntensityAnalyzer
import re

class SentimentAnalyzer:
    def __init__(self, data: pd.DataFrame):
        self.sia = SentimentIntensityAnalyzer()
        self.stopwords = nltk.corpus.stopwords.words("english") + ['rt', 'https']

        if "retweets" in data.columns:
            self.tweeter = True
            self.data = data.loc[data["lang"] == "en"]
        else:
            self.tweeter = False
            self.data = data
        self.blobs = None
        self.sub_counts = None
        self.word_counts = None

    def basic_cleaning(self):
        functs = [lambda x: x.replace(r"\n", " ") if x is not None else x,
                  lambda x: x.replace(r"  ", " ") if x is not None else x,
                  lambda x: re.sub(r'(^|\s)((https?:\/\/)?[\w-]+(\.[a-z-]+)+\.?(:\d+)?(\/\S*)?)|(https)', '', x) if x is not None else x]

        for f in functs:
            self.data["body"].apply(f)

    def calculate_sentiment(self):
        self.data["polarity_score"] = self.data["body"].apply(lambda x: self.sia.polarity_scores(x)["compound"] if x is not None else "")
        self.apply_weights()

    def load_blobs(self):
        # group by subs
        subreddit_groups = self.data.groupby(by="subreddit")

        # create text blobs per sub
        words = {}
        for subreddit, df in subreddit_groups:
            # skip subreddits with < 5 posts with invisalign
            if df.shape[0] > 5:
                # tokenize into wordlists per subreddit, drop stopwords
                words[subreddit] = nltk.word_tokenize(" ".join(df["body"].tolist()))

        self.blobs = words

    def basic_stats(self):
        if self.blobs is None:
            self.load_blobs()
        d = {}
        for sub, blob in self.blobs.items():
            d[sub] = nltk.FreqDist([w.lower() for w in blob
                                    if (w.lower() not in self.stopwords)
                                    and (w.isalpha())
                                    and (w.lower() != "invisalign")]).most_common(5)


        self.word_counts = pd.DataFrame.from_dict(d, orient="index")
        return self.word_counts

    def twitter_basic_stats(self):
        if "retweets" in self.data.columns:
            words = nltk.word_tokenize(" ".join([w.lower() for w in self.data["body"].tolist()
                                                 if w is not None
                                                 and w not in self.stopwords]))
            matches = [w for w in words if w.lower().isalpha() and w not in self.stopwords]
            d = nltk.FreqDist(matches).most_common(50)
            self.word_counts = pd.DataFrame.from_dict(d)
            self.word_counts.set_index(0)
        return self.word_counts

    def apply_weights(self):
        if "retweets" in self.data.columns:
            weight_col = "retweets"
        else:
            weight_col = "score"
        self.data["weighted_polarity_score"] = self.data["polarity_score"] * self.data[weight_col]

    def subreddit_counts(self, include_default_sub=True):
        if "retweets" in self.data.columns:
            return None
        if include_default_sub:
            self.sub_counts = self.data["subreddit"].value_counts()
        else:
            self.sub_counts = self.data.loc[self.data["subreddit"] != "Invisalign"]["subreddit"].value_counts()
        return self.sub_counts

    def export_to_db(self, db_path):
        con = sqlite3.connect(db_path)
        if self.tweeter:
            self.data.to_sql(name="twitter_data", con=con, index=False)
            self.word_counts.to_sql(name="twitter_wordcount", con=con, index=False)
        else:
            self.data.to_sql(name="reddit_data", con=con, index=False)
            self.sub_counts.to_sql(name="reddit_sub_counts", con=con, index=False)
            self.word_counts.applymap(str).to_sql(name="reddit_word_counts", con=con, index=False)
        con.close()

def save_result_cache():

    reddit, twitter = import_corpora(r"D:\Python\Align\posts_scraper\cache.db")

    for dataset in [reddit]:
        sa = SentimentAnalyzer(dataset)
        print(sa.data.head(10))
        sa.basic_cleaning()
        sa.calculate_sentiment()
        sa.subreddit_counts()
        sa.twitter_basic_stats()
        sa.basic_stats()
        sa.export_to_db(r"results.db")



