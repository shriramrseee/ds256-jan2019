import json
import networkx as nx
from networkx.readwrite import json_graph

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

from config import remote_ip, local_graph_file


class local_vertex:
    """
    Local vertex class
    """

    def __init__(self, id=None, prop=None):
        self.id = id
        self.prop = prop

    def __dict__(self):
        return {'id': self.id, 'prop': self.prop}


class local_edge:
    """
    Local edge class
    """

    def __init__(self, prop=None, inV=None, outV=None):
        self.prop = prop
        self.inV = inV
        self.outV = outV

    def __dict__(self):
        return {'prop': self.prop, 'inV': self.inV, 'outV': self.outV}


def clear_local_graph():
    """
    Delete all vertices in local graph
    """
    with open(local_graph_file, 'wb') as f:
        f.write('')


def fetch_store_local_graph(source, hops=1):
    """
    Fetch and store subgraph locally
    """
    g = traversal().withRemote(DriverRemoteConnection('ws://' + remote_ip + ':8182/gremlin', 'g'))

    # Fetch remote subgraph
    subgraph = g.V(source).repeat(__.outE().subgraph('subGraph').outV()).times(hops).cap('subGraph').toList()[0]

    # Construct vertex list
    vertices = []
    for v in subgraph['@value']['vertices'][0:10]:
        id = v.id
        prop = {}
        p = g.V(id).propertyMap().toList()[0]
        for i in p:
            prop[i] = p[i][0].value
        vertices.append(local_vertex(id, prop))

    # Construct edge list
    edges = []
    for e in subgraph['@value']['edges'][0:10]:
        inV = e.inV.id
        outV = e.outV.id
        prop = {'id': e.id}
        p = g.E(e.id).propertyMap().toList()[0]
        for i in p:
            prop[i] = p[i].value
        edges.append(local_edge(prop, inV, outV))

    print len(vertices), len(edges)

    # Persist in local NetworkX file
    g = nx.Graph()
    for v in vertices:
        g.add_node(v.id, **v.prop)
    for e in edges:
        g.add_edge(e.inV, e.outV, **e.prop)
    data = json_graph.node_link_data(g)
    with open(local_graph_file, 'wb') as f:
        json.dump(data, f)


def get_local_graph():
    """
    Create local graph instance and return the object
    :return:
    """

    with open(local_graph_file, "rb") as f:
        data = json.load(f)
        g = json_graph.node_link_graph(data)

    return g
