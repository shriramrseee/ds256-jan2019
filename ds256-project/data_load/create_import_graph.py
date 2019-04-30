import networkx as nx
import time

g = nx.Graph()

edges = []
nodes = []

with open("local_data/yagoLiteralFacts.tsv", "rb") as f:
    i = 0
    start = time.time()
    for l in f:
        line = l.split('\t')
        if len(line) == 5:
            nodes.append((line[1][1:-1].decode('utf-8').encode('ascii', 'ignore'),
                          {line[2][1:-1].decode('utf-8').encode('ascii', 'ignore'): line[3][1:-1].decode('utf-8').encode('ascii', 'ignore')}))
        i += 1
        if i % 100000 == 0:
            end = time.time()
            print i, end - start
            g.add_nodes_from(nodes)
            if i == 100000:
                break
            nodes = []

with open("local_data/yagoFacts.tsv", "rb") as f:
    i = 0
    start = time.time()
    for l in f:
        line = l.split('\t')
        if len(line) == 5:
            edges.append((line[1][1:-1].decode('utf-8').encode('ascii', 'ignore'),
                          line[3][1:-1].decode('utf-8').encode('ascii', 'ignore'),
                          {'label': line[2][1:-1].decode('utf-8').encode('ascii', 'ignore')}))
        i += 1
        if i % 100000 == 0:
            g.add_edges_from(edges)
            end = time.time()
            print i, end - start
            edges = []
            if i == 100000:
                break

g.add_edges_from(edges)
end = time.time()
print i, end - start

nx.write_graphml(g, "test_graph.ml")

print g.number_of_nodes(), g.number_of_edges()



