
class StaleDataError(Exception):
    def __init__(self, handler):
        self.handler = handler

