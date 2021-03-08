import requests
import pandas as pd
import json
import time
import datetime

class PushshiftScraper:
    def __init__(self, **kwargs):
        self.data = self.convert_dates(self.paginate_by_utc(**kwargs)).sort_values(by="created_utc", ascending=False)

    def paginate_by_utc(self, queries: list, days: int, interval: int) -> pd.DataFrame:
        """
        due to change in pushshift api max return limit need to paginate by comment add date
        :param queries: list of strings to be included in the search eg ["invisalign", "teeth"]
        :param days: period in days to search for
        :param interval: data granularity in hours (paginate by x hours)
                         example: paginate_by_utc(["invisalign"], 10, 3) - find invisalign posts for last 10 days
                                                                           with 3h granularity to avoid going over
                                                                           100 comments api limit - increase up to 24 to
                                                                           improve performance, decrease to improve
                                                                           data quality
        :return: dataframe containing concatenated api response
        """
        data_chunks = []
        # cycle through queries
        for query in queries:
            after = interval

            # for each query take 1 day period of comments and move on
            for _ in range((24//interval) * days):
                before = after - interval
                url = f"https://api.pushshift.io/reddit/comment/search/?q={query}&after={after}h&before={before}h&sort=asc&size=100&fields=author,author_flair_text,body,comment_type,created_utc,id,link_id,permalink,score,subreddit,subreddit_id"
                df = pd.json_normalize(json.loads(requests.get(url).text), record_path='data')
                time.sleep(1)
                if len(df) > 0:
                    print(len(df))
                    data_chunks.append(df)

                after += interval

        result = pd.concat(data_chunks).reset_index(drop=True)
        return result

    def convert_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        df["created_utc"] = df["created_utc"].apply(lambda x: datetime.datetime.fromtimestamp(x))
        return df



