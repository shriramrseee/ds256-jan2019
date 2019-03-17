import json

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


class local_vertex:
    """
    Local vertex class
    """

    def __init__(self, id=None, label=None, prop=None):
        self.id = id
        self.label = label
        self.prop = prop

    def __dict__(self):
        return {'id': self.id, 'label': self.label, 'prop': self.prop}


class local_edge:
    """
    Local edge class
    """

    def __init__(self, id=None, label=None, prop=None, inV=None, outV=None):
        self.id = id
        self.label = label
        self.prop = prop
        self.inV = inV
        self.outV = outV

    def __dict__(self):
        return {'id': self.id, 'label': self.label, 'prop': self.prop, 'inV': self.inV, 'outV': self.outV}


def clear_local_graph():
    """
    Delete all vertices in local graph
    """
    g = traversal().withRemote(DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))
    g.V().drop().iterate()


def fetch_store_local_graph(id, hops=3):
    """
    Fetch and store subgraph locally
    """
    g = traversal().withRemote(DriverRemoteConnection('ws://10.24.24.2:8182/gremlin', 'g'))

    # Fetch remote subgraph
    subgraph = g.V(id).repeat(__.inE().subgraph('subGraph').outV()).times(hops).cap('subGraph').toList()[0]

    # Construct vertex list
    vertices = []
    for v in subgraph['@value']['vertices']:
        id = v.id
        label = g.V(id).label().toList()[0]
        prop = {}
        p = g.V(id).propertyMap().toList()[0]
        for i in p:
            prop[i] = p[i][0].value
        vertices.append(local_vertex(id, label, prop))

    # Construct edge list
    edges = []
    for e in subgraph['@value']['edges']:
        id = e.id
        inV = e.inV.id
        outV = e.outV.id
        label = g.E(id).label().toList()[0]
        prop = {}
        p = g.E(id).propertyMap().toList()[0]
        for i in p:
            prop[i] = p[i].value
        edges.append(local_edge(id, label, prop, inV, outV))

    # Persist in local Tinkergraph

    g = traversal().withRemote(DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))

    new_id = {}

    for v in vertices:
        new_vertex = g.addV(v.label)
        for i in v.prop:
            new_vertex.property(i, v.prop[i])
        new_id[v.id] = new_vertex.id().toList()[0]

    print new_id

    for e in edges:
        s = g.V(new_id[e.outV])
        d = g.V(new_id[e.inV])
        new_edge = g.addE(e.label).from_(s).to(d)
        for i in e.prop:
            new_edge.property(i, e.prop[i])


