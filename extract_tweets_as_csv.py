import calendar
import csv
import json
import sys
import time

from argparse import ArgumentParser
from datetime import datetime


class Options:
    def __init__(self):
        self._init_parser()

    def _init_parser(self):
        usage = 'extract_retweets_as_csv.py -i <tweets>.json -o <retweets>.csv [--format TWITTER|GNIP]'

        self.parser = ArgumentParser(usage=usage)
        self.parser.add_argument(
            '-i',
            required=True,
            dest='in_file',
            help='A file of tweets'
        )
        self.parser.add_argument(
            '-o',
            required=False,
            default=None,
            dest='out_file',
            help='Retweets as CSV (a selection of fields)'
        )
        self.parser.add_argument(
            '-f', '--format',
            required=False,
            default='TWITTER',
            choices=['TWITTER', 'GNIP'],
            dest='tweet_format',
            help='Format of tweets, TWITTER or GNIP (default TWITTER)'
        )

    def parse(self, args=None):
        return self.parser.parse_args(args)


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


def get_ot_from_rt(rt):
    if 'retweeted_status' in rt:
        return rt['retweeted_status']
    else:
        return None


def expanded_urls_from(tweet, include_retweet=False):
    # urls are counted in quote tweets if they're part of the added comment
    url_entities = tweet['entities']['urls']
    if 'extended_tweet' in tweet:
        url_entities = tweet['extended_tweet']['entities']['urls']
    urls = [u['expanded_url'] for u in url_entities if u['expanded_url']] # skip ''
    if include_retweet and is_rt(tweet):  # 'retweeted_status' in tweet and tweet['retweeted_status']:
        urls += expanded_urls_from(get_ot_from_rt(tweet))  #['retweeted_status'])
    return urls


def mentioned_screen_names_from(tweet, desired_field='screen_name'):
    def extract_mention_ids(entities):
        return [m[desired_field] for m in entities]
    m_entities = tweet['entities']['user_mentions']
    if 'extended_tweet' in tweet:
        m_entities = tweet['extended_tweet']['entities']['user_mentions']
    return extract_mention_ids(m_entities)


# e.g. Tue Dec 31 06:15:21 +0000 2019
# TWITTER_TS_FORMAT='%a %b %d %H:%M:%S +0000 %Y'
TWITTER_TS_FORMAT='%a %b %d %H:%M:%S %z %Y' # BEWARE: Using %z here might screw up other code.
# e.g. "2017-10-30T00:48:56.000Z"
GNIP_TS_FORMAT='%Y-%m-%dT%H:%M:%S.000Z'

def parse_ts(ts_str, fmt=TWITTER_TS_FORMAT):
    time_struct = time.strptime(ts_str, fmt)
    dt = datetime.fromtimestamp(time.mktime(time_struct))
    return int(calendar.timegm(dt.timetuple()))


def make_csv_safe(str_with_newlines):
    return str_with_newlines.replace('\n', '\\n')


def is_rt(t, tweet_format):
    if tweet_format == 'GNIP':
        return t['verb'] == 'share'
    else:  # tweet_format == 'TWITTER', i.e., standard twitter format
        return 'retweeted_status' in t and t['retweeted_status'] != None


def extract_gnip_id(str):
    return str[str.rindex(':')+1:]


def extract_row(t, tweet_format):
    rt = t  # the retweet
    if tweet_format == 'GNIP':
        print('GNIP not currently supported')
        sys.exit(0)
        # ot = t['object']  # the original tweet
        # return {
        #     'retweetid'       : extract_gnip_id(rt['id']), # e.g. "tag:search.twitter.com,2005:924800252065980416"
        #     'userid'          : extract_gnip_id(rt['actor']['id']),  # e.g. "id:twitter.com:15523288"
        #     'original tweetid': extract_gnip_id(ot['id']),
        #     'original userid' : extract_gnip_id(ot['actor']['id']),
        #     'retweet text'    : make_csv_safe(rt['body']), #extract_text(rt),  # RT @orig: what orig said...
        #     'timestamp'       : parse_ts(rt['postedTime'], fmt=GNIP_TS_FORMAT)  # epoch seconds, e.g. "2017-10-30T00:48:56.000Z"
        # }
    else:  # tweet_format == 'TWITTER'
        u_sn = t['user']['screen_name']
        t_id = t['id_str']
        return {
            'timestamp' : parse_ts(t['created_at']),  # epoch seconds
            'created_at': t['created_at'],
            'screen_name': u_sn,
            'tweet_url': f'https://twitter.com/{u_sn}/status/{t_id}',
            'text': make_csv_safe(extract_text(t)),
            'is_retweet': is_rt(t, tweet_format),
            'mentions': json.dumps(mentioned_screen_names_from(t)),
            'urls': expanded_urls_from(t)
        }


if __name__=='__main__':

    options = Options()
    opts = options.parse(sys.argv[1:])

    in_file = opts.in_file
    out_file = opts.out_file
    tweet_format = opts.tweet_format
    print(f'Reading {in_file} and writing to {out_file}')

    with open(in_file, 'r', encoding='utf-8') as in_f:
        with open(out_file, 'w', newline='', encoding='utf-8') as out_f:
            cols = [
                'timestamp', 'created_at', 'screen_name', 'tweet_url', 'text',
                'is_retweet', 'mentions', 'urls'
            ]
            writer = csv.DictWriter(out_f, fieldnames=cols)

            writer.writeheader()
            for l in in_f:
                t = json.loads(l)
                # if is_rt(t, tweet_format):
                writer.writerow(extract_row(t, tweet_format))
