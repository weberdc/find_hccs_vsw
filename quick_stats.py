#!/usr/bin/env python3

import networkx as nx
# import ntpath  # https://stackoverflow.com/a/8384788
import os
import statistics
import sys

from utils import extract_filename


def mew(g):
    ews = [d['weight'] for u,v,d in g.edges(data=True)]
    # using pstdev because we have the whole population of edge weights
    return statistics.mean(ews), statistics.pstdev(ews)


if __name__=='__main__':
    if len(sys.argv) < 2:
        print('Usage: quick_stats.py [--header] <weighted_graph.graphml>')
        sys.exit(1)

    if sys.argv[1] == '--header':
        gfn = sys.argv[2]
        header = True
    else:
        gfn = sys.argv[1]
        header = False
    # print(f'{gfn} MEW: {mew(nx.read_graphml(gfn))}')

    g = nx.read_graphml(gfn)

    if g.number_of_nodes() == 0:
        print('Empty graph')
        sys.exit(1)

    columns = (
        'filename,nodes,edges,edge_weight_mean,edge_weight_stdev,density,components,' +
        'largest_component_nodes,largest_component_edges,' +
        'largest_component_edge_weight_mean,largest_component_edge_weight_stdev,' +
        'largest_component_density'
    ).split(',')

    components = list(nx.connected_components(g))
    lc = g.subgraph(max(components, key=len))

    ew_mean, ew_stdev = mew(g)
    lc_ew_mean, lc_ew_stdev = mew(lc)
    stats = dict(
        filename = extract_filename(gfn),
        nodes = g.number_of_nodes(),
        edges = g.number_of_edges(),
        edge_weight_mean = ew_mean,
        edge_weight_stdev = ew_stdev,
        density = nx.density(g),
        components = len(components),
        largest_component_nodes = lc.number_of_nodes(),
        largest_component_edges = lc.number_of_edges(),
        largest_component_edge_weight_mean = lc_ew_mean,
        largest_component_edge_weight_stdev = lc_ew_stdev,
        largest_component_density = nx.density(lc)
    )

    if header:
        print(','.join(columns))
        # print(f'{gfn},nodes,edges,edge_weight_mean,edge_weight_stdev,density,big_c_nodes,big_c_edges,big_c_ew_mean,big_c_ew_stdev')

    print(','.join([f'{stats[k]}' for k in columns]))
