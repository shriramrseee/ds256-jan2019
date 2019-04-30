import json
from multiprocessing import freeze_support

from gremlin_python import statics
from gremlin_python.driver.client import Client
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __, both, out
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T, ShortestPath
from gremlin_python.process.traversal import Order
from gremlin_python.process.traversal import Cardinality
from gremlin_python.process.traversal import Column
from gremlin_python.process.traversal import Direction
from gremlin_python.process.traversal import Operator
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import Pop
from gremlin_python.process.traversal import Scope
from gremlin_python.process.traversal import Barrier
from gremlin_python.process.traversal import Bindings
from gremlin_python.process.traversal import WithOptions

from input_query import process_input_query

if __name__ == '__main__':
    freeze_support()

    # with open("sample_queries/vertex_search.json", "rb") as f:
    #     query = f.read()
    #     process_input_query(json.loads(query))

    # g = traversal().withRemote(DriverRemoteConnection('ws://35.200.188.1:8182/gremlin', 'g'))
    # print g.V().label().toList()

    # with open("sample_queries/edge_search.json", "rb") as f:
    #     query = f.read()
    #     process_input_query(json.loads(query))

    with open("sample_queries/path_search.json", "rb") as f:
        query = f.read()
        process_input_query(json.loads(query))

    # Get cut vertices
    # g = traversal().withRemote(DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))
    # cutV = []
    # for i in g.V('Donald_Knuth').repeat(out().simplePath()).times(1).path().toList():
    #     cutV.append(i.objects[-1])
    #
    # with open("sample_queries/reachability.json", "rb") as f:
    #     query = f.read()
    #     process_input_query(json.loads(query), cutV[1:10])

    # g = traversal().withRemote(DriverRemoteConnection('ws://35.200.188.1:8182/gremlin', 'g'))
    # g = g.withComputer()
    # print g.V('Mahatma_Gandhi').shortestPath().with_(ShortestPath.target, __.hasId('Iceland')).with_(ShortestPath.includeEdges, True).toSet()


# result = g.V().hasLabel('<India>').shortestPath().with_(
#     ShortestPath.target, __.hasLabel('<50_Cent>')).toList()
# print result[0].objects[0].label
# print result[0].objects[1].label
# print result[0].objects[2].label

# g = traversal().withRemote(DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))
# print g.V().count().toList()
# for i in g.V().hasLabel("<India>").outE().haslabel("<dealsWith>").toList():
#     print g.V(i.inV).label().toList()[0], i.label
# print dir(g.V().hasLabel("<India>").outE().toList()[0].inV)
#
# print g.V().hasLabel("<India>").order().by().toList()

#
# id = g.V().hasLabel("<50_Cent>").toList()[0].id
# print id
# print g.V(id).out().toList()
# print g.E().toList()
# print g.V(id).repeat(__.outE().subgraph('subGraph').outV()).times(1).cap('subGraph').toList()

# g.V().hasLabel('person').has('name', P.eq('marko')).out()

# g.io().write('data').iterate()

# graph = g.V(3).repeat(__.inE().subgraph('subGraph').outV()).times(3).cap('subGraph').toList()[0]
#
# for e in graph['@value']['edges']:
#     id = e.id
#     inV = e.inV
#     outV = e.outV
#     label = g.E(id).label().toList()[0]
#     prop = {}
#     p = g.E(id).propertyMap().toList()[0]
#     print p
#     for i in p:
#         print i, p[i]
#         # prop[i] = p[i][0].value


# from local_graph import fetch_store_local_graph, clear_local_graph
#
# clear_local_graph()
# fetch_store_local_graph('Donald_Knuth', 1)







# print g.V().hasLabel('person').has('name', P.eq('marko')).out().toSet()

# "C:\Users\Beast\Documents\IISc\DS 256\ds256-jan2019\project\apache-tinkerpop-gremlin-server-3.4.1\bin\gremlin-server.bat" ..\gremlin-server.yaml
