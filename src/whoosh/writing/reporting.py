import logging


logger = logging.getLogger(__name__)


class Reporter(object):
    def __init__(self, logger_obj=None):
        self.logger = logger_obj or logger

    def start_indexing(self, ixdir):
        pass

    def finish_indexing(self):
        pass


default_reporter = Reporter



