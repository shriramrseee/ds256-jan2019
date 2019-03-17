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

    def load_path_search(self, input_query):
        self.filter = input_query['filter']


class answer:
    """
    Class to capture result
    """

    def __init__(self):
        self.result = set([])

    def union(self, r):
        self.result |= r


def process_input_query(input_query, cutV=None):
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
    elif q.type == 'path_search':
        q.load_path_search(input_query)

    if q.type == 'path_search':
        # Execute Local query
        local_result = answer()
        lt = threading.Thread(target=query_server, args=(copy.deepcopy(q), 'local', local_result))
        lt.start()
        lt.join()

        if len(local_result.result) == 0:  # Path is not contained in local

            # Execute Remote query
            remote_result = answer()
            for v in cutV:
                newq = copy.deepcopy(q)
                newq.filter['from'] = v.label
                rt = threading.Thread(target=query_server, args=(copy.deepcopy(newq), 'remote', remote_result))
                rt.start()
                rt.join()

            # Find min
            m = None
            for i in remote_result.result:
                if m is None or len(i.objects) < len(m.objects):
                    m = i

            if m is not None:
                # Complete path
                local_result = answer()
                newq = copy.deepcopy(q)
                newq.filter['to'] = m.objects[0].label
                lt = threading.Thread(target=query_server, args=(copy.deepcopy(newq), 'local', local_result))
                lt.start()
                lt.join()
                for i in local_result.result:
                    for j in i.objects:
                        print j.label,
                    for i in m.objects[1:]:
                        print i.label,
                    print '\n'
            else:
                print None

        else: # Path is contained in local
            for i in local_result.result:
                for j in i.objects:
                    print j.label,
                print '\n'



    else:
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
