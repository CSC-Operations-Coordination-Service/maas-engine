"""AMQP related components"""

import logging
from urllib.parse import urlparse, urlunparse

import kombu


class AMQPSettings:
    """AMQPSettings store exchanges, queues and event mapping and provides the logic
    to build them from a dictionnary typically loaded from a json file
    """

    def __init__(self, url, heartbeat=None):

        self.logger = logging.getLogger(self.__class__.__name__)

        self.url = url

        # AMQP heartbeat timeout in seconds; None or 0 disables heartbeats
        # (kombu's default). See --amqp-heartbeat in maas_engine.cli.args.
        self.heartbeat = heartbeat

        self.connection = None

        self.exchanges = {}

        self.queues = {}

        self.event_mapping = {}

    def build_queues(self, config_dict, max_priority):
        """Create exchanges and queues, populate the event mapping from a data
        dictionnary

        Args:
            config_dict (dict): configuration dictionnary with an amqp key
        """
        for exchange_dict in config_dict["amqp"]:

            # create exchange
            exchange_name = exchange_dict["name"]

            exchange = kombu.Exchange(exchange_name, type="topic", durable=True)

            # register exchange in internal dict
            self.exchanges[exchange_name] = exchange

            for queue_dict in exchange_dict["queues"]:

                queue_name = queue_dict["name"]

                # create queue
                queue = kombu.Queue(
                    queue_name,
                    exchange,
                    routing_key=queue_dict["routing_key"],
                    durable=True,
                    exclusive=False,
                    max_priority=max_priority,
                )

                # register queues in internal dict
                self.queues[queue_name] = queue

                # populate event mapping
                self.event_mapping[queue_dict["routing_key"]] = list(
                    queue_dict["events"]
                )

    def get_consumers(self, Consumer, channel, on_message):
        """create consumer list, grouped by exchange"""
        return [
            Consumer(
                [
                    queue
                    for queue in self.queues.values()
                    if queue.exchange.name == exchange_name
                ],
                # TODO make a configuration parameter
                prefetch_count=1,
                callbacks=[on_message],
            )
            for exchange_name in self.exchanges
        ]

    @staticmethod
    def mask_amqp_password(amqp_url):
        # Parse the URL
        parsed_url = urlparse(amqp_url)

        # Replace the password with '**'
        if parsed_url.password:
            masked_netloc = f"{parsed_url.username}:**@{parsed_url.hostname}"
            if parsed_url.port:
                masked_netloc += f":{parsed_url.port}"
        else:
            masked_netloc = parsed_url.netloc

        # Reconstruct the URL with the masked password
        masked_url = urlunparse(
            (
                parsed_url.scheme,
                masked_netloc,
                parsed_url.path,
                parsed_url.params,
                parsed_url.query,
                parsed_url.fragment,
            )
        )

        return masked_url

    def connect(self) -> kombu.BrokerConnection:
        """initiate the connection to AMQP service

        Returns:
            kombu.BrokerConnection: working connection
        """
        # The heartbeat lives on this connection, and kombu's ConsumerMixin
        # clones it for every (re)connection of the consume loop, which also
        # calls heartbeat_check() between messages. Producers publish with
        # retry=True, which re-establishes the connection if the broker dropped
        # it for missed heartbeats.
        self.connection = kombu.BrokerConnection(
            self.url, heartbeat=self.heartbeat or 0
        )
        self.logger.info(
            "AMQP: connected to %s (heartbeat: %s)",
            AMQPSettings.mask_amqp_password(self.url),
            f"{self.heartbeat}s" if self.heartbeat else "disabled",
        )
        return self.connection
