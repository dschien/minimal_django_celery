import amqpstorm
from django.conf import settings


class IODICUS_AMQP_Controller():
    abstract = True
    _channel = None
    _connection = None

    @property
    def channel(self):
        if self._channel is None:
            self._connection = amqpstorm.Connection(settings.IODICUS_MESSAGING_HOST,
                                                    settings.IODICUS_MESSAGING_USER,
                                                    settings.IODICUS_MESSAGING_PASSWORD,
                                                    port=settings.IODICUS_MESSAGING_PORT,
                                                    ssl=settings.IODICUS_MESSAGING_SSL)

            self._channel = self._connection.channel()

            self._channel.exchange.declare(exchange=settings.IODICUS_MESSAGING_EXCHANGE_NAME, exchange_type='fanout')
        return self._channel

    def send_msg(self, json_payload=None):
        self.channel.basic.publish(body=json_payload,
                                   # body
                                   routing_key='',  # routing key
                                   exchange=settings.IODICUS_MESSAGING_EXCHANGE_NAME,
                                   properties={
                                       'delivery_mode': 2,  # make message persistent
                                       'content_type': 'application/json',

                                   })
