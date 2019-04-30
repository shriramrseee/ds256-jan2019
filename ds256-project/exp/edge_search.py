import copy
import json
import random
import time

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
from local_graph import fetch_store_local_graph, clear_local_graph
from eprint import eprint

# Clear graph
clear_local_graph()
fetch_store_local_graph('<India>', 2)

# Get randomizing parameters
g = traversal().withRemote(DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))
vlabels = g.V().label().toList()
elabels = g.E().label().toList()


# Run queries for 5 mins

with open("../sample_queries/edge_search.json", "rb") as f:
    query = f.read()
    input_query = json.loads(query)
    st = time.time()
    while True:
        for i in range(10):
            # Construct query
            n = random.randint(1, 1000) % 3
            q = copy.deepcopy(input_query)
            if n==0:
                q['filter']['has_label'] = random.choice(elabels)
            elif n==1:
                q['filter']['from'] = random.choice(vlabels)
            else:
                q['filter']['to'] = random.choice(vlabels)
            # Fire query
            pay = process_input_query(q)
            eprint("Edge Search", n, pay[0], pay[1], pay[2], pay[3], pay[4], sep=',')
        if time.time() - st >= 500:
            break

# Goodbye

