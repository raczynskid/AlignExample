from posts_scraper.api_access.pushshift_wrapper import PushshiftScraper
from posts_scraper.api_access.twitter_api_wrapper import TwitterScraper
import pandas as pd
import sqlite3


def scrape():
    ps = PushshiftScraper(queries=["invisalign"], days=360, interval=6)  # 10, 6
    ts = TwitterScraper("invisalign").results_to_dataframe(max_tweets=5000)

    return ps.data, ts


def cache(db: str, ps=None, ts=None):
    con = sqlite3.connect(db)

    if ps is not None:
        ps.to_sql(name="reddit_data", con=con, if_exists="append")
    if ts is not None:
        ts.to_sql(name="twitter_data", con=con, if_exists="append")
    con.close()

def import_corpora(db: str):
    # load posts data from cache

    con = sqlite3.connect(db)
    reddit = pd.read_sql("SELECT * FROM reddit_data", con)
    twitter = pd.read_sql("SELECT * FROM twitter_data", con)
    con.close()

    return reddit, twitter


if __name__ == '__main__':
    #reddit, twitter = import_corpora("D:\Python\Align\posts_scraper\cache.db")
    reddit, twitter = scrape()
    cache(db="cache.db", ps=reddit)