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

from config import remote_ip


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

    def load_from_json(self, j):
        self.id = j['id']
        self.label = j['label']
        self.prop = j['prop']


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

    def load_from_json(self, j):
        self.id = j['id']
        self.label = j['label']
        self.prop = j['prop']
        self.inV = j['inV']
        self.outV = j['outV']


def fetch_store_local_graph(id, hops=3):
    """
    Fetch and store subgraph locally
    """
    g = traversal().withRemote(DriverRemoteConnection('ws://' + remote_ip + ':8182/gremlin', 'g'))

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
        vertices.append(local_vertex(id, label, prop).__dict__)

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
        edges.append(local_edge(id, label, prop, inV, outV).__dict__)

    # Persist in JSON format
    vertices = json.dumps(vertices)
    edges = json.dumps(edges)

    with open("local_data/vertices.json", 'wb') as f:
        f.write(vertices)

    with open("local_data/edges.json", 'wb') as f:
        f.write(edges)


def read_local_graph():
    """
    Reads local graph JSON and returns vertex and edge list
    :return:
    """
    vertices = []
    edges = []

    with open("local_data/vertices.json", 'rb') as f:
        data = json.load(f)
        for v in data:
            t = local_vertex()
            t.load_from_json(v)
            vertices.append(t)

    with open("local_data/edges.json", 'rb') as f:
        data = json.load(f)
        for e in data:
            t = local_edge()
            t.load_from_json(e)
            edges.append(t)

    return [vertices, edges]
