import logging

logger = logging.getLogger('wh_parser.logger')


class IgnoreErrorFilter(logging.Filter):
    def filter(self, record):
        if record.levelno == logging.ERROR:
            return 0
        else:
            return 1


# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# FileHandler
file_handler = logging.FileHandler('./error.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(level=logging.ERROR)
logger.addHandler(file_handler)

# StreamHandler
stream_handler = logging.StreamHandler()
stream_handler.setLevel(level=logging.INFO)
stream_filter = IgnoreErrorFilter("stream_error_filter")
stream_handler.addFilter(stream_filter)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
