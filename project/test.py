from gremlin_python import statics
from gremlin_python.driver.client import Client
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T
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

# with open("sample_queries/vertex_search.json", "rb") as f:
#     query = f.read()
#     process_input_query(query)


# with open("sample_queries/edge_search.json", "rb") as f:
#     query = f.read()
#     process_input_query(query)

# g = traversal().withRemote(DriverRemoteConnection('ws://10.24.24.2:8182/gremlin', 'g'))

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


from local_graph import fetch_store_local_graph

fetch_store_local_graph(3,3)







# print g.V().hasLabel('person').has('name', P.eq('marko')).out().toSet()