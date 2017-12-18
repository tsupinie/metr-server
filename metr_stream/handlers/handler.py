
def get_data_handler(name):
    from .level2radar import Level2Handler
    from .shapefile import ShapefileHandler
    from .obs import ObsHandler
    from .static import StaticHandler

    handler_dict = {
        'level2radar': Level2Handler,
        'shapefile': ShapefileHandler,
        'obs': ObsHandler,
        'gui': StaticHandler,
    }

    return handler_dict[name]


class DataHandler(object):
    async def fetch(self):
        raise NotImplementedError(f"fetch() is not implemented for {self.__class__.__name___}")

    def post_fetch(self):
        pass

    def data_check_intv(self):
        return 30 * 24 * 3600
