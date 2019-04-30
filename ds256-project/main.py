import time

from local_graph import fetch_store_local_graph, clear_local_graph
from cache import cache
import conf

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

from rest_api import create_app

# Initialize local graph database

clear_local_graph()
fetch_store_local_graph(conf.local_entity, conf.hops)

print 'Initialization of Local graph Done!'

# Get Cut vertices

cutv = []
g = traversal().withRemote(DriverRemoteConnection(conf.local_server, 'g'))
for i in g.V(conf.local_entity).repeat(out().simplePath()).times(conf.hops).path().toList():
    cutv.append(i.objects[-1])

print 'Cut Vertices Done!'

# Initialize cache

remote_cache = cache(conf.max_cache_size)

print 'Cache Done!'

# Start a REST API to handle queries

app = create_app(remote_cache, cutv)
app.run(host='localhost')

while True:
    time.sleep(10)

