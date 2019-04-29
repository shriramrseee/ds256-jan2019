import time
from operator import gt, lt, eq

from gremlin_python import statics
from gremlin_python.driver.client import Client
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T, ShortestPath, gte, lte, neq
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


def query_remote_server(query, answer):
    """
    Contact the Gremlin server with given query and get the query result
    :return: query result
    """

    st = time.time()

    g = traversal().withRemote(DriverRemoteConnection('ws://' + remote_ip + ':8182/gremlin', 'g'))

    filter_pred = {'gt': P.gt, 'gte': P.gte, 'lt': P.lt, 'lte': P.lte, 'eq': P.eq, 'neq': P.neq}

    if query.type == 'vertex_search':
        result = g
        if query.filter['has_label'] is not None:
            result = g.V(query.filter['has_label'])
        if query.filter['from'] is not None:
            result = result.V(query.filter['from']).outE().inV()
        if query.filter['to'] is not None:
            result = result.V(query.filter['to']).inE().outV()
        if query.filter['has'] is not None:
            attribute = query.filter['has'][0]
            predicate = filter_pred[query.filter['has'][1]](query.filter['has'][2])
            result = result.has(attribute, predicate)

        # Execute query
        answer.union(result.id().toSet())
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


def query_local_server(graph, query, answer):
    """
    Query the local NetworkX graph and get the query result
    :return: query result
    """

    st = time.time()

    filter_pred = {'gt': gt, 'gte': gte, 'lt': lt, 'lte': lte, 'eq': eq, 'neq': neq}

    if query.type == 'vertex_search':
        result = set([])
        result1 = set([])
        flag = False
        vertices = graph.nodes(data=True)
        edges = graph.edges.data()
        if query.filter['has_label'] is not None:
            for v in vertices:
                if v[0] == query.filter['has_label']:
                    result.add(v[0])
            flag = True
        if query.filter['from'] is not None:
            for e in edges:
                if e[0] == query.filter['from']:
                    result1.add(e[1])
            if flag:
                result = result1.intersection(result)
            else:
                result = result1
                flag = True
            result1 = set([])
        if query.filter['to'] is not None:
            for e in edges:
                if e[1] == query.filter['to']:
                    result1.add(e[0])
            if flag:
                result = result1.intersection(result)
            else:
                flag = True
                result = result1
            result1 = set([])
        if query.filter['has'] is not None:
            attribute = query.filter['has'][0]
            predicate = filter_pred[query.filter['has'][1]]
            for v in vertices:
                if attribute in v[1]:
                    if predicate(v[1][attribute], query.filter['has'][2]):
                        result1.add(v[0])
            if flag:
                result = result1.intersection(result)
            else:
                flag = True
                result = result1
            result1 = set([])

        # Execute query
        answer.union(result)
        # print location, answer.result, time.time() - st
        answer.time = time.time() - st
