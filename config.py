import configparser
import os

dirname = os.path.dirname(os.path.abspath(__file__))
config_file_path = os.path.join(dirname, '.conf.ini')
config = configparser.RawConfigParser()
config.read(config_file_path)



## db connection
def db_connection(connection_string: str) -> str:
    """
    Returns db connection string corresponding to db connection parameter as specified in config file
    """
    return config.get('DATABASES', connection_string)


## mail config
SMTP_HOST = config.get('MAIL_CONFIG', 'SMTP_HOST')
SMTP_PORT = config.get('MAIL_CONFIG', 'SMTP_PORT')
MAIL_FROM = config.get('MAIL_CONFIG', 'MAIL_FROM')
MAIL_PASSWORD = config.get('MAIL_CONFIG', 'MAIL_PASSWORD')