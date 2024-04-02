from config import bootstrap_server, consumer_group

producer_conf = {'bootstrap.servers': bootstrap_server, 'client.id': 'producer_ams'}


consumer_conf = {'bootstrap.servers': bootstrap_server,
                     'group.id': consumer_group,
                     'auto.offset.reset': "smallest", "client.id": 'client_1'}