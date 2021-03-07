import praw
from posts_scraper.data.list_of_subreddits import sport_subreddit_list
from requests import Session
session = Session()
session.verify = r"C:\Users\Kiermasz\Align\Lib\site-packages\requests\cacert.pem"
reddit = praw.Reddit(
    client_id="K7o_XVRtuuLztA",
    client_secret="oik01__3RhseTNOqgMgHXG5KPnjkdQ",
    requestor_kwargs={"session": session},  # pass Session
    user_agent="data scraper for align interview",
)

subs = [reddit.subreddits.search_by_name(sub_name, include_nsfw=False) for sub_name in sport_subreddit_list]

for group in subs[:10]:
    for sub in group:
        threads = sub.top(limit=100)
        for thread in threads:
            thread.comments.replace_more(limit=0)
            comments_of_interest = [(thread.title, comment.body)
                                    for comment in thread.comments
                                    if ("teeth" in comment.body.lower())
                                    or ("tooth" in comment.body.lower())
                                    or ("teeth" in thread.title.lower())]
            if len(comments_of_interest) > 0:
                print(comments_of_interest)

def get_child_posts():
    url = "https://www.reddit.com/r/AskScienceFiction/comments/lyf6mk/lotrwhat_did_gandalf_exactly_do_when_he_hit_the/gpshko6?utm_source=share&utm_medium=web2x&context=3"
    submission = reddit.submission(url=url)

    for top_level_comment in submission.comments:
        print(top_level_comment.body)