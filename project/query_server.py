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


def query_server(query):
    """
    Contact the Gremlin server with given query and get the query result
    :return: query result
    """

    g = traversal().withRemote(DriverRemoteConnection('ws://10.24.24.2:8182/gremlin', 'g'))

    sort_orders = {'asc': Order.asc, 'desc': Order.desc}
    filter_pred = {'gt': P.gt, 'gte': P.gte, 'lt': P.lt, 'lte': P.lte, 'eq': P.eq, 'neq': P.neq}

    if query.type == 'vertex_search':
        # Get query parameters
        has_label = query.filter.pop('has_label')
        attribute = query.filter['has'][0]
        predicate = filter_pred[query.filter['has'][1]](query.filter['has'][2])
        order_by = query.sort['attribute']
        order = sort_orders[query.sort['order']]

        # Execute query
        result = g.V().hasLabel(has_label).has(attribute, predicate).order().by(order_by, order).toList()
        result = {v.id for v in result}
        return result

    elif query.type == 'edge_search':
        # Get query parameters
        has_label = query.filter.pop('has_label')
        attribute = query.filter['has'][0]
        predicate = filter_pred[query.filter['has'][1]](query.filter['has'][2])
        order_by = query.sort['attribute']
        order = sort_orders[query.sort['order']]

        # Execute query
        result = g.E().hasLabel(has_label).has(attribute, predicate).order().by(order_by, order).toList()
        result = {e.id for e in result}
        return result
