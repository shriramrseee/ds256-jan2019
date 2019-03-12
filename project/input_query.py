import copy
import json

from query_local import query_local
from query_server import query_server


class query:
    """
    Class to capture query parameters
    """

    def __init__(self, type):
        self.type = type
        self.filter = None
        self.sort = None

    def load_vertex_search(self, input_query):
        self.filter = input_query['filter']
        self.sort = input_query['sort']

    def load_edge_search(self, input_query):
        self.filter = input_query['filter']
        self.sort = input_query['sort']


def process_input_query(input_query):
    """
    Processes the given input query and returns the result
    :param input_query: JSON format
    :return: query result
    """

    input_query = json.loads(input_query)

    q = query(input_query['type'])

    if q.type == 'vertex_search':
        q.load_vertex_search(input_query)
    elif q.type == 'edge_search':
        q.load_edge_search(input_query)

    # Execute Remote query
    remote_result = query_server(copy.deepcopy(q))
    # Execute Local query
    local_result = query_local(copy.deepcopy(q))

    print local_result, remote_result
