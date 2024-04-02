import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_handler = logging.StreamHandler()
log_formatter = logging.Formatter('%(levelname)s:%(name)s:%(asctime)s:%(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)