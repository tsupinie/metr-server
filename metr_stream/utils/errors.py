
class StaleDataError(Exception):
    def __init__(self, handler):
        self.handler = handler


class NoNewDataError(Exception):
    def __init__(self, handler):
        self.handler = handler
