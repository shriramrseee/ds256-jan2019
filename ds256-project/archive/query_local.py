from archive.local_graph import read_local_graph
import operator


def query_local(query):
    """
    Master function to run local query
    """
    vertices, edges = read_local_graph()

    if query.type == 'vertex_search':
        return local_vertex_search(vertices, query)
    elif query.type == 'edge_search':
        return local_edge_search(edges, query)


def local_vertex_search(vertices, query):
    """
    Perform local vertex search
    """

    sort_orders = {'asc': True, 'desc': False}
    filter_pred = {'gt': operator.gt, 'gte': operator.ge, 'lt': operator.lt, 'lte': operator.le, 'eq': operator.eq,
                   'neq': operator.ne}

    # Get query parameters
    print query.filter
    has_label = query.filter.pop('has_label')
    attribute = query.filter['has'][0]
    predicate = filter_pred[query.filter['has'][1]]
    value = query.filter['has'][2]
    order_by = query.sort['attribute']
    order = sort_orders[query.sort['order']]

    # Execute query
    result = set([])
    for v in vertices:
        if v.label == has_label and attribute in v.prop and predicate(v.prop[attribute], value):
            result.add(v.id)

    return result


def local_edge_search(edges, query):
    """
    Perform local edge search
    """

    sort_orders = {'asc': True, 'desc': False}
    filter_pred = {'gt': operator.gt, 'gte': operator.ge, 'lt': operator.lt, 'lte': operator.le, 'eq': operator.eq,
                   'neq': operator.ne}

    # Get query parameters
    has_label = query.filter.pop('has_label')
    attribute = query.filter['has'][0]
    predicate = filter_pred[query.filter['has'][1]]
    value = query.filter['has'][2]
    order_by = query.sort['attribute']
    order = sort_orders[query.sort['order']]

    # Execute query
    result = set([])
    for e in edges:
        if e.label == has_label and attribute in e.prop and predicate(e.prop[attribute], value):
            result.add(e.id)

    return result
