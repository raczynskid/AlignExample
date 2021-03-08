from searchtweets import ResultStream, gen_request_parameters, load_credentials
import pandas as pd


class TwitterScraper:
    def __init__(self, query: str, limit: int = 1000):
        self.limit_results = limit

        self.search_args = load_credentials(r"D:\Python\Align\posts_scraper\api_access\twitter_keys.yaml",
                                            yaml_key="search_tweets_v2",
                                            env_overwrite=False)

        self.query = gen_request_parameters(query, results_per_call=100,
                                            tweet_fields="created_at,lang,public_metrics"
                                            )

    def results_to_dataframe(self, max_pages: int = 10,
                             max_results: int = 1000,
                             max_tweets: int = 1000) -> pd.DataFrame:
        """creates generator for tweets and formats into dataframe"""

        # override limits with single contructor value
        if self.limit_results is not None:
            max_results, max_tweets = (self.limit_results, self.limit_results)

        # setup result stream
        rs = ResultStream(request_parameters=self.query,
                          max_results=max_results,
                          max_pages=max_pages,
                          max_tweets=max_tweets,
                          **self.search_args)

        # put int dataframe (hacky with list() fix later)
        df = pd.DataFrame.from_dict(list(rs.stream()))[['public_metrics', 'text',
                                                          'lang', 'id', 'created_at', 'newest_id']]
        # parse retweets from metrics
        df["public_metrics"] = df["public_metrics"].apply(lambda x: x['retweet_count'] if isinstance(x, dict) else 0)

        # rename columns
        df.columns = ['retweets', 'body', 'lang', 'id', 'created_utc', 'newest_id']

        return df
