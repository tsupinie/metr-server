
def get_data_handler(name):
    from .level2radar import Level2Handler

    handler_dict = {
        'level2radar': Level2Handler,
    }

    return handler_dict[name]


class DataHandler(object):
    async def fetch(self):
        raise NotImplementedError(f"fetch() is not implemented for {self.__class__.__name___}")

    def post_fetch(self):
        pass
