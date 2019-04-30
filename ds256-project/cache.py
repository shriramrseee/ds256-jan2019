import sys


class cache:
    """
    Class to implement cache logic
    """

    def __init__(self, limit):
        self.result = {}
        self.order = []
        self.limit = limit

    def lookup(self, query):
        if query in self.result:
            return self.result[query]
        else:
            return -1

    def add(self, query, answer):
        self.result[query] = answer
        self.order.append(query)
        while self.get_size() > self.limit and len(self.order) > 0:
            self.pop()

    def get_size(self):
        return sys.getsizeof(self.result)

    def pop(self):
        del self.result[self.order[0]]
        self.order.pop(0)

    def clean(self):
        self.result = {}
        self.order = []
