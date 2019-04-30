import copy
import json
import threading
import time
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

    def load_reachability(self, input_query):
        self.filter = input_query['filter']

    def load_path_search(self, input_query):
        self.filter = input_query['filter']


class answer:
    """
    Class to capture result
    """

    def __init__(self):
        self.result = set([])
        self.time = 0

    def union(self, r):
        self.result |= r


def process_input_query(input_query, cutV=None):
    """
    Processes the given input query and returns the result
    :param input_query: JSON format
    :return: query result
    """

    q = query(input_query['type'])

    if q.type == 'vertex_search':
        q.load_vertex_search(input_query)
    elif q.type == 'edge_search':
        q.load_edge_search(input_query)
    elif q.type == 'reachability':
        q.load_reachability(input_query)
    elif q.type == 'path_search':
        q.load_path_search(input_query)

    if q.type == 'reachability':

        start = time.time()

        # Execute Local query
        local_result = answer()
        lt = threading.Thread(target=query_server, args=(copy.deepcopy(q), 'local', local_result))
        lt.start()
        lt.join()

        local_time = local_result.time
        remote_time = 0
        local_length = len(local_result.result)
        remote_length = 0

        if len(local_result.result) == 0:  # Path is not contained in local

            # Execute Remote query
            remote_result = answer()
            for v in cutV:
                newq = copy.deepcopy(q)
                newq.filter['from'] = v.label
                rt = threading.Thread(target=query_server, args=(copy.deepcopy(newq), 'remote', remote_result))
                rt.start()
                rt.join()
                remote_time += remote_result.time

            remote_length += len(remote_result.result)

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
                local_time += local_result.time
                local_length += len(local_result.result)
                for i in local_result.result:
                    for j in i.objects:
                        print j.label,
                    for i in m.objects[1:]:
                        print i.label,
                    print '\n'
            else:
                # Path might be contained entirely in remote            
                rt = threading.Thread(target=query_server, args=(copy.deepcopy(q), 'remote', remote_result))
                rt.start()
                rt.join()
                remote_time += remote_result.time
                if len(remote_result.result) == 0:
                    # No path in local or remote
                    print None
                else:
                    for i in remote_result.result:
                        for j in i.objects:
                            print j.label,
                        print '\n'

        else:  # Path is contained in local
            for i in local_result.result:
                for j in i.objects:
                    print j.label,
                print '\n'

        payload = [local_length, remote_length, local_time, remote_time, time.time() - start]

        return payload

    elif q.type == 'path_search':

        start = time.time()

        # Execute Remote query
        remote_result = answer()
        rt = threading.Thread(target=query_server, args=(copy.deepcopy(q), 'remote', remote_result))
        rt.start()

        rt.join()

        end = time.time()

        # print remote_result.result

    else:
        start = time.time()
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

        end = time.time()

        # payload = [len(local_result.result), len(remote_result.result)]

        print local_result.result
        print remote_result.result

        local_result.result.union(remote_result.result)

        # payload = payload + [len(local_result.result), local_result.time, remote_result.time]

        # return payload
