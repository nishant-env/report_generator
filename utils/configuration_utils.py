from config import bootstrap_server, consumer_group, consumer_max_poll_timeout_ms

producer_conf = {'bootstrap.servers': bootstrap_server, 'client.id': 'producer_ams'}


consumer_conf = {'bootstrap.servers': bootstrap_server,
                     'group.id': consumer_group,
                     'max.poll.interval.ms': consumer_max_poll_timeout_ms,
                     'auto.offset.reset': "smallest", "client.id": 'client_1'}