import json

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

    # Execute Local query

    # Execute Remote query
    remote_result = query_server(q)

    print remote_result



