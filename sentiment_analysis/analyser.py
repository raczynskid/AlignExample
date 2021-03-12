import nltk
import sqlite3
import pandas as pd
import os
from posts_scraper.Scraper import import_corpora
from nltk.sentiment import SentimentIntensityAnalyzer
import re

class RedditSentimentAnalyzer:
    def __init__(self):
        self.sia = SentimentIntensityAnalyzer()
        self.stopwords = nltk.corpus.stopwords.words("english") + ['rt', 'https']
        self.data = import_corpora(r"D:\Python\Align\posts_scraper\reddit_cache.db")
        self.basic_cleaning()
        self.top_positive = []
        self.top_negative = []

    def basic_cleaning(self):
        functs = [lambda x: x.replace(r"  ", " ") if x is not None else x,
                  lambda x: re.sub(r'(^|\s)((https?:\/\/)?[\w-]+(\.[a-z-]+)+\.?(:\d+)?(\/\S*)?)|(https)|(\n)', '', x)
                  if x is not None else x]

        for f in functs:
            self.data["body"] = self.data["body"].apply(f)

    def no_context_sentiment(self, body: str) -> float:
        text = nltk.Text(nltk.sent_tokenize(body))
        applicable_sentences = []
        for sentence in text.tokens:
            if "invisalign" in sentence.lower():
                applicable_sentences.append(sentence)
        sentence_scores = [self.sia.polarity_scores(s)["compound"] for s in applicable_sentences if s is not None]
        try:
            specific_score = sum(sentence_scores)/len(sentence_scores)
        except ZeroDivisionError:
            specific_score = 0

        if specific_score > 0.9:
            self.top_positive.append(applicable_sentences)
        elif specific_score < -0.9:
            self.top_negative.append(applicable_sentences)

        return specific_score

    def calculate_sentiment(self):
        self.data["context_polarity_score"] = self.data["body"].apply(lambda x: self.sia.polarity_scores(x)["compound"] if x is not None else 0)
        self.data["no_context_polarity_score"] = self.data["body"].apply(lambda x: self.no_context_sentiment(x) if x is not None else 0)


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


    def export_to_db(self, db_path):
        con = sqlite3.connect(db_path)
        self.data.to_sql(name="reddit_data", con=con)
        con.close()

if __name__ == '__main__':

    sa = RedditSentimentAnalyzer()
    sa.calculate_sentiment()
    sa.export_to_db('reddit_result_cache.db')





