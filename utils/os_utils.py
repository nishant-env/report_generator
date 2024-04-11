import os
from .log_utils import logger

def check_if_folder_exists(path):
    return os.path.exists(path)


def create_folder(path):
    logger.debug("Checking if folder already exists")
    try:
        if_exists = check_if_folder_exists(path)
        logger.debug(f"Folder existance: {if_exists}")
        if not if_exists:
            os.mkdir(path)
            logger.info(f"Folder Creation Successful for {path}")

        else:
            logger.debug('Folder already exists, skipping')
    except Exception as e:
        logger.exception("Folder creation failed with ", e)