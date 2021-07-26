#!/usr/bin/env python3

import networkx as nx
import sys
import utils

from argparse import ArgumentParser


class Options:
    def __init__(self):
        self._init_parser()

    def _init_parser(self):
        usage = 'filter_by_edge_weight.py -i <in.graphml> -o <out.graphml> -mw <weight> [--dry-run]'

        self.parser = ArgumentParser(usage=usage)
        self.parser.add_argument(
            '-i',
            required=True,
            dest='in_file',
            help='A weighted network'
        )
        self.parser.add_argument(
            '-o',
            required=False,
            default=None,
            dest='out_file',
            help='The filtered network (default: modified in file name)'
        )
        self.parser.add_argument(
            '-mw', '--min-weight',
            required=True,
            type=float,
            dest='min_weight',
            help='Minimum edge weight to retain'
        )
        self.parser.add_argument(
            '-p', '--weight-property',
            required=False,
            default='weight',
            dest='weight_property',
            help='Name of weight property (default "weight")'
        )
        self.parser.add_argument(
            '--dry-run',
            dest='dry_run',
            action='store_true',
            default=False,
            help='Dry run - will not write to disk (default: False)'
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


DEBUG=False
OVERRIDE=True
def log(msg, override=False):
    if DEBUG or override: utils.eprint('[%s] %s' % (utils.now_str(), msg))


if __name__=='__main__':

    options = Options()
    opts = options.parse(sys.argv[1:])

    DEBUG=opts.verbose

    STARTING_TIME = utils.now_str()
    log('Starting', OVERRIDE)

    if len(sys.argv) < 2:
        print('Usage: filter_by_edge_weight.py <in.graphml> <out.graphml>')
        sys.exit(1)

    in_gfn = opts.in_file
    out_gfn = opts.out_file
    if not out_gfn:
        out_gfn = f'{in_gfn[:in_gfn.rindex(".")]}-min{opts.min_weight}.graphml'
        # print(out_gfn)
        # sys.exit(0)

    in_g = nx.read_graphml(in_gfn)
    print(f'Min weight: {opts.min_weight}')
    print(f'In file:  {in_gfn}')
    print(f'Out file: {out_gfn}')
    print(f'In:  V={in_g.number_of_nodes():>8,} E={in_g.number_of_edges():>8,}')

    for u, v, _ in [
        (u, v, w) for u, v, w in in_g.edges(data=opts.weight_property) if w < opts.min_weight
    ]:
        in_g.remove_edge(u, v)

    for n, _ in [(n, d) for n, d in in_g.degree() if d == 0 ]:
        in_g.remove_node(n)

    print(f'Out: V={in_g.number_of_nodes():>8,} E={in_g.number_of_edges():>8,}')

    if not opts.dry_run:
        nx.write_graphml(in_g, out_gfn)

    log('DONE having started at %s,' % STARTING_TIME, OVERRIDE)
