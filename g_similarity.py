import networkx as nx
import sys

from argparse import ArgumentParser


class Options:
    def __init__(self):
        self._init_parser()

    def _init_parser(self):
        usage = 'filter_by_edge_weight.py -i <in.graphml> -o <out.graphml> -mw <weight> [--dry-run]'

        self.parser = ArgumentParser(usage=usage)
        self.parser.add_argument(
            '-g1',
            required=True,
            dest='g1_file',
            help='A network (graphml)'
        )
        self.parser.add_argument(
            '-g2',
            required=True,
            dest='g2_file',
            help='A network (graphml)'
        )
        self.parser.add_argument(
            '--header',
            dest='header',
            action='store_true',
            default=False,
            help='Output the column header (default: False)'
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


def jaccard(g1, g2):
    g1_nodes = set(g1.nodes())
    g2_nodes = set(g2.nodes())

    return len(g1_nodes.intersection(g2_nodes)) / len(g1_nodes.union(g2_nodes))


def overlap(g1, g2):
    g1_nodes = set(g1.nodes())
    g2_nodes = set(g2.nodes())

    return len(g1_nodes.intersection(g2_nodes)) / min(len(g1_nodes), len(g2_nodes))


DEBUG=False
OVERRIDE=True
def log(msg, override=False):
    if DEBUG or override: utils.eprint('[%s] %s' % (utils.now_str(), msg))


if __name__=='__main__':

    options = Options()
    opts = options.parse(sys.argv[1:])

    DEBUG=opts.verbose

    g1 = nx.read_graphml(opts.g1_file)
    g2 = nx.read_graphml(opts.g2_file)

    # g1_components = list(nx.connected_components(g1))
    # g1lc = g.subgraph(max(components, key=len))

    g1lc = g1.subgraph(max(nx.connected_components(g1), key=len))
    g2lc = g2.subgraph(max(nx.connected_components(g2), key=len))

    # overlap always ought to be 1
    if opts.header:
        print('G1,G2,G1 nodes,G2 nodes,Jaccard,Overlap,G1 (LC) nodes,G2 (LC) nodes,Jaccard (LC),Overlap (LC)')

    print(','.join([
        f'{opts.g1_file}',
        f'{opts.g2_file}',
        f'{g1.number_of_nodes()}',
        f'{g2.number_of_nodes()}',
        f'{jaccard(g1, g2)}',
        f'{overlap(g1, g2)}',
        f'{g1lc.number_of_nodes()}',
        f'{g2lc.number_of_nodes()}',
        f'{jaccard(g1lc, g2lc)}',
        f'{overlap(g1lc, g2lc)}'
    ]))
