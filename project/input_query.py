import copy
import json
import threading
from multiprocessing import Process

from query import query_server


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

    def load_edge_search(self, input_query):
        self.filter = input_query['filter']

class answer:
    """
    Class to capture result
    """

    def __init__(self):
        self.result = set([])

    def union(self, r):
        self.result |= r

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

    # Execute Local query
    local_result = answer()
    lt = threading.Thread(target=query_server, args=(copy.deepcopy(q), 'local', local_result))
    lt.start()

    # Execute Remote query
    remote_result = answer()
    rt = threading.Thread(target=query_server, args=(copy.deepcopy(q), 'remote', remote_result))
    rt.start()

    lt.join()
    rt.join()

    print local_result.result.union(remote_result.result)
