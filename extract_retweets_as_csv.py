import calendar
import csv
import json
import sys
import time

from datetime import datetime


def extract_text(tweet):
    """Gets the full text from a tweet if it's short or long (extended)."""

    def get_available_text(t):
        if t['truncated'] and 'extended_tweet' in t:
            # if a tweet is retreived in 'compatible' mode, it may be
            # truncated _without_ the associated extended_tweet
            #eprint('#%s' % t['id_str'])
            return t['extended_tweet']['full_text']
        else:
            # return t['text'] if 'text' in t else t['full_text']
            return t['full_text'] if 'full_text' in t else t['text']

    if 'retweeted_status' in tweet:
        rt = tweet['retweeted_status']
        return 'RT @%s: %s' % (rt['user']['screen_name'], extract_text(rt))

    if 'quoted_status' in tweet:
        qt = tweet['quoted_status']
        return get_available_text(tweet) + " --> " + extract_text(qt)

    return get_available_text(tweet)


# e.g. Tue Dec 31 06:15:21 +0000 2019
# TWITTER_TS_FORMAT='%a %b %d %H:%M:%S +0000 %Y'
TWITTER_TS_FORMAT='%a %b %d %H:%M:%S %z %Y' # BEWARE: Using %z here might screw up other code.


def parse_ts(ts_str, fmt=TWITTER_TS_FORMAT):
    time_struct = time.strptime(ts_str, fmt)
    dt = datetime.fromtimestamp(time.mktime(time_struct))
    return int(calendar.timegm(dt.timetuple()))


if __name__=='__main__':

    if len(sys.argv < 3):
        print('Usage: python this_script.py <input tweets>.json <extracted retweets>.csv')

    in_file = sys.argv[1]
    out_file = sys.argv[2]

    with open(in_file, 'r', encoding='utf-8') as in_f:
        with open(out_file, 'w', newline='', encoding='utf-8') as out_f:
            cols = [
                'timestamp', 'retweetid', 'userid', 'original tweetid',
                'original userid', 'retweet text'
            ]
            writer = csv.DictWriter(out_f, fieldnames=cols)

            writer.writeheader()
            for l in in_f:
                t = json.loads(l)
                if 'retweeted_status' in t and t['retweeted_status']:
                    rt = t  # the retweet
                    ot = t['retweeted_status']  # the original tweet
                    row = {
                        'retweetid'       : rt['id_str'],
                        'userid'          : rt['user']['id_str'],
                        'original tweetid': ot['id_str'],
                        'original userid' : ot['user']['id_str'],
                        'retweet text'    : extract_text(rt),  # RT @orig: what orig said...
                        'timestamp'       : parse_ts(rt['created_at'])  # epoch seconds
                    }
                    writer.writerow(row)
