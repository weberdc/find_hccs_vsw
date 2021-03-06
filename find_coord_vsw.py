#!/usr/bin/env python3

import csv
import gzip
import json
import networkx as nx
import re
import regex
import statistics
import sys
import time
import utils

from argparse import ArgumentParser
from typing import Pattern

# Searches timestamped interactions for coordination using a genuine sliding
# window

class Options:
    def __init__(self):
        self._init_parser()

    def _init_parser(self):
        usage = 'find_coord_via_sliding_window.py -i <interactions_file> -o <graphml filebase> -d1 <window size> -d2 <window size>'

        self.parser = ArgumentParser(usage=usage)
        self.parser.add_argument(
            '-i',
            required=True,
            dest='interactions_file',
            help='A file of timestamped interactions'
        )
        self.parser.add_argument(
            '-o',
            required=True,
            dest='out_filebase',
            help='Filebase of where to write the LCNs to'
        )
        self.parser.add_argument(
            '-d1',
            required=True,
            dest='d1_arg',
            help='Delta 1 window size, value + unit, e.g. 10s = 10 seconds (m=mins, h=hr, d=day, w=week)'
        )
        self.parser.add_argument(
            '-d2',
            default='-1',
            dest='d2_arg',
            help='Delta 2 window size, value + unit, e.g. 10s = 10 seconds (m=mins, h=hr, d=day, w=week) (default: same as delta 1)'
        )
        self.parser.add_argument(
            '--raw',
            dest='raw_data',
            choices=['TWEETS'],
            default=None,
            help='Expect raw JSON data objects as input (default: None)'
        )
        self.parser.add_argument(
            '-cmp', '--comparison-strategy',
            dest='comparison_strategy',
            choices=['EXACT_MATCH','CASE_INSENSITIVE', 'TEXT_SIMILARITY'],
            default='EXACT_MATCH',
            help='Comparison strategy to use (default: EXACT_MATCH)'
        )
        self.parser.add_argument(
            '--text-similarity-threshold',
            dest='text_similarity_threshold',
            type=float,
            default=0.9,
            help='Threshold for a match [0,1] (default: 0.9)'
        )
        self.parser.add_argument(
            '--text-similarity-min-tokens',
            dest='text_similarity_min_tokens',
            type=int,
            default=5,
            help='Minimum tokens to consider a text match (default: 5)'
        )
        self.parser.add_argument(
            '--extract',
            dest='extract_what',
            choices=Extractor.EXTRACTABLES,
            default=None,
            help='What to extract from the raw JSON data objects (default: HASHTAGS)'
        )
        self.parser.add_argument(
            '--ts-col',
            default='timestamp',
            dest='timestamp_column',
            help='Name of timestamp column (default: timestamp)'
        )
        self.parser.add_argument(
            '--src-col',
            default='source',
            dest='source_column',
            help='Name of source column, i.e., potentially coordinating account (default: source)'
        )
        self.parser.add_argument(
            '--tgt-col',
            default='target',
            dest='target_column',
            help='Name of target column, i.e., value binding coordinating accounts (default: target)'
        )
        self.parser.add_argument(
            '--id-col',
            default='t_id',
            dest='id_column',
            help='Name of post ID column, (default: t_id)'
        )
        self.parser.add_argument(
            '--exclude-targets',
            default='',
            dest='exclude_targets',
            help='Target values to ignore, separated by | (default: "")'
        )
        self.parser.add_argument(
            '--keep-history',
            dest='keep_history',
            action='store_true',
            default=False,
            help='Maintains lists of connecting reasons and order of co-activities, using a lot of memory (default: False)'
        )
        self.parser.add_argument(
            '--dry-run',
            dest='dry_run',
            action='store_true',
            default=False,
            help='Dry run - will not write to disk (default: False)'
        )
        self.parser.add_argument(
            '--final-g-only',
            dest='final_g_only',
            action='store_true',
            default=False,
            help='Will only write out final combined CN (default: False)'
        )
        self.parser.add_argument(
            '--final-g-min-ew',
            dest='final_g_min_edge_weight',
            type=float,
            default=-1.0,
            help='Filters the CN edge weights before writing to disk (default: -1)'
        )
        self.parser.add_argument(
            '-v', '--verbose',
            dest='verbose',
            action='store_true',
            default=False,
            help='Verbose logging (default: False)'
        )

    def parse(self, args=None):
        return self.parser.parse_args(args)


class Extractor:
    EXTRACTABLES = ['HASHTAGS', 'URLS', 'RETWEETS', 'REPLIES', 'MENTIONS', 'QUOTES', 'TEXT', 'DOMAINS']
    def __init__(self, exclude_targets):
        self.to_exclude = exclude_targets

    def extract(self, post):
        pass


class CsvExtractor(Extractor):
    def __init__(self, id_col, ts_col, src_col, tgt_col, exclude_targets):
        super().__init__(exclude_targets)
        self.ts_col = ts_col
        self.src_col = src_col
        self.tgt_col = tgt_col
        self.id_col = id_col

    def extract(self, row):
        if row[self.tgt_col] in self.to_exclude:
            return []

        return [{
            't_id': row[self.id_col],
            'ts' :  int(row[self.ts_col]),
            'src':  row[self.src_col],
            'tgt':  row[self.tgt_col]
        }]


class TweetExtractor(Extractor):
    # used to avoid catching Twitter URLs and domains
    TWEET_URL_REGEX = re.compile('https://twitter.com/[^/]*/status/.*')

    def __init__(self, what, exclude_targets):
        super().__init__(exclude_targets)
        self.field = what

    def extract(self, post):
        # may result in multiple extractions
        t = json.loads(post)
        extractions = []
        extract_template = {
            'ts' : parse_ts(t['created_at']),
            'src': t['user']['id_str'],
            't_id': t['id_str']
        }
        # extract useful bits
        if self.field == 'RETWEETS' and utils.is_rt(t):
            extract_template['tgt'] = t['retweeted_status']['id_str']
            extractions.append(extract_template)
        elif self.field == 'QUOTES' and utils.is_qt(t):
            extract_template['tgt'] = t['quoted_status']['id_str']
            extractions.append(extract_template)
        elif self.field == 'REPLIES' and utils.is_reply(t):
            extract_template['tgt'] = t['in_reply_to_user_id_str']
            extractions.append(extract_template)
        elif self.field == 'TEXT' and not utils.is_rt(t): # avoid retweets
            extract_template['tgt'] = utils.extract_text(t)
            extractions.append(extract_template)
        elif self.field == 'HASHTAGS':
            for ht in utils.lowered_hashtags_from(t, include_retweet=True):
                if ht in self.to_exclude:
                    continue
                ht_extract = extract_template.copy()
                ht_extract['tgt'] = ht
                extractions.append(ht_extract)
        elif self.field == 'URLS':
            for url in utils.expanded_urls_from(t, include_retweet=True):
                if TWEET_URL_REGEX.match(url) or url in self.to_exclude:
                    continue
                url_extract = extract_template.copy()
                url_extract['tgt'] = url
                extractions.append(url_extract)
        elif self.field == 'DOMAINS':
            domains = [
                utils.extract_domain(url) for url in utils.expanded_urls_from(t, include_retweet=True)
            ]
            for domain in domains:
                if domain == 'twitter.com' or domain in self.to_exclude:
                    continue
                domain_extract = extract_template.copy()
                domain_extract['tgt'] = domain
                extractions.append(domain_extract)
        elif self.field == 'MENTIONS':
            if utils.is_rt(t):
                # NB this avoids implicit mention of retweeted account
                mentions = utils.mentioned_ids_from(utils.get_ot_from_rt(t))
            else:
                mentions = utils.mentioned_ids_from(t)

            for m in mentions:
                if m in self.to_exclude:
                    continue
                m_extract = extract_template.copy()
                m_extract['tgt'] = m
                extractions.append(m_extract)
        # return them
        return extractions


class Comparator:
    def compare(self, x, y):
        # return comparison strength: 0 for different, > 0 for similar (1 for same)
        pass


class ExactMatchComparator(Comparator):
    def compare(self, x, y):
        return 1 if x == y else 0


class CaseInsensitiveComparator(Comparator):
    def compare(self, x, y):
        return 1 if str(x).lower() == str(y).lower() else 0


class TextSimilarityComparator(Comparator):
    def __init__(self, threshold=0.9, min_tokens=5):
        self.threshold = threshold
        self.min_tokens = min_tokens

    def compare(self, str1, str2):
        """
        Method borrowed from https://github.com/QUT-Digital-Observatory/coordination-network-toolkit/blob/main/coordination_network_toolkit/similarity.py
        """
        word_tokeniser = regex.compile(
            # Note this handles 'quoted' words a little weirdly: 'orange' is tokenised
            # as ["orange", "'"] I'd prefer to tokenise this as ["'", "orange", "'"]
            # but the regex library behaves weirdly. So for now phrase search for
            # quoted strings won't work.
            r"\b\p{Word_Break=WSegSpace}*'?",
            flags=regex.WORD | regex.UNICODE | regex.V1,
        )

        def tokenise(text: str, tokenizer: Pattern = word_tokeniser) -> str:
            # words = sorted(set(t for t in tokenizer.split(text.lower()) if t))
            # tokenized = " ".join(words)
            # return tokenized
            return sorted(set(t for t in tokenizer.split(text.lower()) if t))

        set1 = set(tokenise(str1))
        if len(set1) < self.min_tokens:
            return False  # 0

        set2 = set(tokenise(str2))
        if len(set2) < self.min_tokens:
            return False  # 0

        comparison = len(set1 & set2) / len(set1 | set2)
        if comparison > self.threshold:
            return comparison
        else:
            return 0  # treat as _entirely_ dissimilar


class Comparators:
    def get_instance(comparison_strategy, **kwargs):
        if comparison_strategy == 'CASE_INSENSITIVE':
            return CaseInsensitiveComparator()
        elif comparison_strategy == 'TEXT_SIMILARITY':
            return TextSimilarityComparator(
                kwargs['text_similarity_threshold'],
                kwargs['text_similarity_min_tokens']
            )
        else:  # comparison_strategy == 'EXACT_MATCH'
            return ExactMatchComparator()



class BatchManager:
    def __init__(self, config):
        self.cfg = config
        self.csv_mode = self.cfg['raw_data'] == None
        self.comparator = Comparators.get_instance(
            config['comparison_strategy'],
            text_similarity_threshold = config['text_similarity_threshold'],
            text_similarity_min_tokens = config['text_similarity_min_tokens']
        )

    def open_file(self, in_file):
        if in_file[-1].lower() == 'z':  # assumes *.gz
            return gzip.open(in_file, 'rt')
        else:
            return open(in_file, 'r', encoding='utf-8')

    def drop_before(self, batch, cutoff_ts):
        return list(filter(lambda p: p['ts'] >= cutoff_ts, batch))

    def process(self, batch, d1_end_ts, old_g, comparison='EXACT', keep_history=False):
        def check_node(g, n_id):
            if not g.has_node(n_id):
                g.add_node(n_id, label=n_id)

        new_g = old_g.copy() if not (self.cfg['final_g_only'] or self.cfg['dry_run']) else old_g
        for i in range(len(batch) - 1):
            if batch[i]['ts'] >= d1_end_ts:
                break
            for j in range(i+1, len(batch)):
                u = batch[i]
                v = batch[j]
                comparison_strength = self.comparator.compare(u['tgt'], v['tgt'])
                if u['src'] != v['src'] and comparison_strength > 0:
                    check_node(new_g, u['src'])
                    check_node(new_g, v['src'])
                    if not new_g.has_edge(u['src'], v['src']):
                        # 'first' is to track the first co-activity acct
                        # including the timestamp will mean entries can be forgotten
                        new_g.add_edge(
                            u['src'], v['src'],
                            weight = comparison_strength,
                            first_counts = { u['src'] : 1.0, v['src'] : 0.0 }
                        )
                        if keep_history:
                            new_g[u['src']][v['src']]['first'] = [{
                                'src': u['src'], 'ts': u['ts'], 't_id': u['t_id']
                            }]
                            new_g[u['src']][v['src']]['reasons'] = [{
                                'tgt': u['tgt'], 'ts': u['ts'], 'ut_id': u['t_id'], 'vt_id': v['t_id']
                            }]
                    else:
                        new_g[u['src']][v['src']]['weight'] += comparison_strength
                        new_g[u['src']][v['src']]['first_counts'][u['src']] += 1.0
                        if keep_history:
                            new_g[u['src']][v['src']]['first'].append({
                                'src': u['src'], 'ts': u['ts'], 't_id': u['t_id']
                            })
                            new_g[u['src']][v['src']]['reasons'].append({
                                'tgt': u['tgt'], 'ts': u['ts'], 'ut_id': u['t_id'], 'vt_id': v['t_id']
                            })
            # no forgetting at the moment
        return new_g

    def write_g(self, g, fn, dont_write_to_disk, verbose=False, keep_history=False):
        if not dont_write_to_disk:
            tmp_g = g.copy()
            # calc average 'first_count' for each node
            for n, n_id in g.nodes(data='label'):
                # get adjacent edges and their data
                tmp_g.nodes[n]['first_proportion'] = statistics.mean(
                    edge_data['first_counts'][n_id] for _, edge_data in g[n].items()
                )
            # flatten complex structures to JSON
            for u, v, d in g.edges(data=True):
                tmp_g[u][v]['first_counts'] = json.dumps(tmp_g[u][v]['first_counts'])
                if keep_history:
                    tmp_g[u][v]['first'] = json.dumps(g[u][v]['first'])
                    tmp_g[u][v]['reasons'] = json.dumps(g[u][v]['reasons'])
            nx.write_graphml(tmp_g, fn)
            log(f'Wrote g (V={g.number_of_nodes():,},E={g.number_of_edges():,}) to {fn}', verbose)

    def process_batch(self, end_w_ts, queue, g, d1_end_ts=None):
        # set the window over which we're operating
        start_w_ts = end_w_ts - self.cfg['d2']
        if not d1_end_ts:
            d1_end_ts = start_w_ts + self.cfg['d1']

        # what's the time span of the queue?
        log(f'Queue {len(queue)} events in {(queue[-1]["ts"] - queue[0]["ts"]) / (60):.1f} minutes')
        # for e in queue:
        #     log(f'{ts_s(e["ts"])} {e["tgt"]}')

        # only process the current window's worth, because the last event that
        # occurred may have been way beyond the previous events
        queue = self.drop_before(queue, start_w_ts)
        log(f'-> Queue {len(queue)} events in {(queue[-1]["ts"] - queue[0]["ts"]) / (60):.1f} minutes')

        if len(queue) >= 2:  # guard clause
            g = self.process(queue, d1_end_ts, g, self.cfg['comparison_strategy'], self.cfg['keep_history'])
        queue = self.drop_before(queue, start_w_ts)  # drop t0 - d1 now

        return (d1_end_ts, queue, g)

    def filter_edges(self, g, min_ew):
        if min_ew < 0:
            return g

        for u, v, _ in [
            (u, v, w) for u, v, w in g.edges(data='weight') if w < min_ew
        ]:
            g.remove_edge(u, v)

        for n, _ in [(n, d) for n, d in g.degree() if d == 0 ]:
            g.remove_node(n)

        return g


    def mkfn(self, ts, final=False):
        tag = 'FINAL' if final else f'{ts_s(ts)}'
        extract_what = f'-{self.cfg["extract_what"]}' if self.cfg["extract_what"] else ''
        return f'{self.cfg["out_filebase"]}{extract_what}-{tag}.graphml'

    def run(self):
        if self.cfg['raw_data'] == 'TWEETS':
            extractor = TweetExtractor(self.cfg['extract_what'], self.cfg['exclude_targets'])
        else:  # csv input
            params = [self.cfg[k] for k in ['id_col', 'ts_col', 'src_col', 'tgt_col', 'exclude_targets']]
            extractor = CsvExtractor(*params)

        in_f = None
        try:
            in_f = self.open_file(self.cfg['in_file'])
            reader = in_f
            if self.csv_mode:
                reader = csv.DictReader(in_f)

            queue = []
            g = nx.Graph()
            start_w_ts = -1
            line_count = 0
            definitely_no_interaction_column = False  # used to short circuit further tests
            for line in reader:
                line_count = utils.log_row_count(line_count, OVERRIDE)

                extractions = extractor.extract(line)
                if len(extractions) == 0:
                    continue

                if start_w_ts == -1:
                    start_w_ts = extractions[0]['ts']
                    log(f'First timestamp: {ts_s(start_w_ts)}')
                curr_ts = extractions[0]['ts']

                # look for the interaction (i.e. extract_what) if it hasn't been provided
                if self.csv_mode and not definitely_no_interaction_column and not self.cfg['extract_what']:
                    if 'interaction' in line:
                        self.cfg['extract_what'] = line['interaction']  # line is a csv row
                    else:
                        definitely_no_interaction_column = True

                if curr_ts > start_w_ts + self.cfg['d2']: # end of curr window
                    # print('end of window')
                    fn = self.mkfn(start_w_ts)
                    d2_end_ts = start_w_ts + self.cfg['d2']
                    start_w_ts, queue, g = self.process_batch(d2_end_ts, queue, g)
                    self.write_g(g, fn, self.cfg['dry_run'] or self.cfg['final_g_only'], keep_history=self.cfg['keep_history'])

                # add the current extractions
                for e in extractions:
                    queue.append(e)

                log(f'[{ts_s(curr_ts)}] Queue size: {len(queue)}, lines read: {line_count}, extractions: {len(extractions)}')

            log('\n', OVERRIDE)

            fn = self.mkfn(start_w_ts, final=True)
            # use up the entire window, given we're at the end
            last_ts = queue[-1]["ts"]
            log(f'Last timestamp: {ts_s(last_ts)}')
            start_w_ts, queue, g = self.process_batch(start_w_ts + self.cfg['d2'], queue, g, last_ts)
            self.write_g(self.filter_edges(g, self.cfg['final_g_min_edge_weight']), fn, self.cfg['dry_run'], keep_history=self.cfg['keep_history'], verbose=OVERRIDE)

        finally:
            if in_f: in_f.close()


def parse_ts(ts_str):
    return utils.extract_ts_s(ts_str)


def ts_s(epoch_seconds_ts):
    return utils.ts_to_str(epoch_seconds_ts)


def convert_to_secs(t_str):
    if t_str[-1] in 'mM':
        return int(t_str[:-1]) * 60
    elif t_str[-1] in 'hH':
        return int(t_str[:-1]) * 60 * 60
    elif t_str[-1] in 'dD':
        return int(t_str[:-1]) * 60 * 60 * 24
    elif t_str[-1] in 'wW':
        return int(t_str[:-1]) * 60 * 60 * 24 * 7
    else:
        return int(t_str[:-1]) if t_str[-1] in 'sS' else int(t_str) # seconds


DEBUG=False
OVERRIDE=True
def log(msg, override=False):
    if DEBUG or override: utils.eprint('[%s] %s' % (utils.now_str(), msg))


if __name__=='__main__':

    options = Options()
    opts = options.parse(sys.argv[1:])

    DEBUG=opts.verbose

    start_time = time.time()
    log('Starting', OVERRIDE)

    cfg = dict(
        in_file = opts.interactions_file,
        out_filebase = opts.out_filebase,
        d1 = convert_to_secs(opts.d1_arg),
        d2 = convert_to_secs(opts.d2_arg),
        raw_data = opts.raw_data,
        extract_what = opts.extract_what,
        exclude_targets = list(map(lambda s: s.lower(), opts.exclude_targets.split('|'))),
        dry_run = opts.dry_run,
        final_g_only = opts.final_g_only,
        final_g_min_edge_weight = opts.final_g_min_edge_weight,
        id_col = opts.id_column,
        ts_col = opts.timestamp_column,
        src_col = opts.source_column,
        tgt_col = opts.target_column,
        keep_history = opts.keep_history,
        comparison_strategy = opts.comparison_strategy,
        text_similarity_threshold = opts.text_similarity_threshold,
        text_similarity_min_tokens = opts.text_similarity_min_tokens
    )

    # default is for no sliding windows (i.e., adjacent windows)
    if cfg['d2'] == -1:
        cfg['d2'] = cfg['d1']

    if cfg['d1'] > cfg['d2']:
        print(f'Delta 1 ({opts.d1_arg}) cannot be greater than delta 2 ({opts.d2_arg})')
    else:
        mgr = BatchManager(cfg)
        mgr.run()

    duration = time.time() - start_time
    log(f'DONE having taken {duration:.1f} seconds', OVERRIDE)
