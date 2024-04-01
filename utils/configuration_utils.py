from config import bootstrap_server
producer_conf = {'bootstrap.servers': bootstrap_server, 'client.id': 'producer_1'}


consumer_conf = {'bootstrap.servers': bootstrap_server,
                     'group.id': "group_1_1",
                     'auto.offset.reset': "smallest"}