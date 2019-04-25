import time

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


def load_fact(location, filepath):
    """
    Load fact data into database
    """

    print "Loading Facts"

    g = traversal().withRemote(DriverRemoteConnection(location, 'g'))

    with open(filepath, "rb") as f:
        i = 0
        start = time.time()
        for l in f:
            edge = l[1:-1].split("\t")
            if len(edge) == 3:
                s = g.V().hasLabel(edge[0]).toList()
                d = g.V().hasLabel(edge[2]).toList()
                if len(s) == 0:
                    s = g.addV(edge[0]).toList()
                if len(d) == 0:
                    d = g.addV(edge[2]).toList()
                new_edge = g.addE(edge[1]).from_(s[0]).to(d[0]).toList()
            i += 1
            if i % 1000 == 0:
                end = time.time()
                print i, end-start
        end = time.time()
        print i, end - start

    print g.V().count().toList()
    print g.E().count().toList()


def load_date(location, filepath):
    """
    Load date data into database
    """

    print "Loading Dates"

    g = traversal().withRemote(DriverRemoteConnection(location, 'g'))

    with open(filepath, "rb") as f:
        i = 0
        start = time.time()
        for l in f:
            prop = l[1:-1].split("\t")
            if len(prop) == 3:
                v = g.V().hasLabel(prop[0]).toList()
                if len(v) == 0:
                    v = g.addV(prop[0]).toList()
                g.V(v[0].id).property(prop[1], prop[2]).toList()
            i += 1
            if i % 1000 == 0:
                end = time.time()
                print i, end - start
        end = time.time()
        print i, end - start


def load_literal(location, filepath):
    """
    Load literal facts into database
    """

    print "Loading Literal facts"

    g = traversal().withRemote(DriverRemoteConnection(location, 'g'))

    with open(filepath, "rb") as f:
        i = 0
        start = time.time()
        for l in f:
            prop = l[1:-1].split("\t")
            if len(prop) == 3:
                v = g.V().hasLabel(prop[0]).toList()
                if len(v) == 0:
                    v = g.addV(prop[0]).toList()
                g.V(v[0].id).property(prop[1], prop[2]).toList()
            i += 1
            if i % 1000 == 0:
                end = time.time()
                print i, end - start
        end = time.time()
        print i, end - start
