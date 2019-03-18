import time

from gremlin_python import statics
from gremlin_python.driver.client import Client
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
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


def query_server(query, location, answer):
    """
    Contact the Gremlin server with given query and get the query result
    :return: query result
    """

    st = time.time()

    ip = {'local': 'ws://localhost:8182/gremlin', 'remote': 'ws://10.24.24.2:8182/gremlin'}

    g = traversal().withRemote(DriverRemoteConnection(ip[location], 'g'))

    filter_pred = {'gt': P.gt, 'gte': P.gte, 'lt': P.lt, 'lte': P.lte, 'eq': P.eq, 'neq': P.neq}

    if query.type == 'vertex_search':
        result = g.V()
        if query.filter['has_label'] is not None:
            result = g.V().hasLabel(query.filter['has_label'])
        if query.filter['has'] is not None:
            attribute = query.filter['has'][0]
            predicate = filter_pred[query.filter['has'][1]](query.filter['has'][2])
            result = result.has(attribute, predicate)
        if query.filter['from'] is not None:
            result = result.hasLabel(query.filter['from']).outE().inV()
        if query.filter['to'] is not None:
            result = result.hasLabel(query.filter['to']).inE().outV()

        # Execute query
        answer.union(result.label().toSet())
        # print location, answer.result, time.time() - st
        answer.time = time.time() - st

    elif query.type == 'edge_search':
        result = g.V()
        if query.filter['from'] is not None:
            result = result.hasLabel(query.filter['from']).outE()
        if query.filter['to'] is not None:
            result = result.hasLabel(query.filter['to']).inE()
        if query.filter['has_label'] is not None:
            result = g.E().hasLabel(query.filter['has_label'])

        # Execute query
        result = result.toSet()
        for e in result:
            answer.union({(e.outV.label, e.label, e.inV.label)})
        # print location, answer.result, time.time() - st
        answer.time = time.time() - st

    elif query.type == 'path_search':
        # Execute query
        g = g.withComputer()
        result = g.V().hasLabel(query.filter['from']).shortestPath().with_(ShortestPath.target, __.hasLabel(query.filter['to'])).with_(ShortestPath.includeEdges, True).toSet()
        # print location, result, time.time() - st
        answer.union(result)
        answer.time = time.time() - st
