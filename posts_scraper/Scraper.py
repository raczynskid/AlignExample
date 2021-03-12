from posts_scraper.api_access.pushshift_wrapper import PushshiftScraper
from posts_scraper.api_access.twitter_api_wrapper import TwitterScraper
import pandas as pd
import sqlite3


def scrape():
    ps = PushshiftScraper(queries=["invisalign"], days=1080, interval=6)

    return ps.data


def cache(db: str, ps=None):
    con = sqlite3.connect(db)

    if ps is not None:
        ps.to_sql(name="reddit_data", con=con, if_exists="append")
    con.close()

def import_corpora(db: str):
    # load posts data from cache

    con = sqlite3.connect(db)
    reddit = pd.read_sql("SELECT * FROM reddit_data", con)
    con.close()

    return reddit


if __name__ == '__main__':
    reddit = scrape()
    cache('reddit_cache.db', reddit)
