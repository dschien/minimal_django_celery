import amqpstorm
from django.conf import settings


class AMQP_Controller():
    abstract = True
    _channel = None
    _connection = None

    @property
    def channel(self):
        if self._channel is None:
            self._connection = amqpstorm.Connection(settings.MESSAGING_HOST,
                                                    settings.MESSAGING_USER,
                                                    settings.MESSAGING_PASSWORD,
                                                    port=settings.MESSAGING_PORT,
                                                    ssl=settings.MESSAGING_SSL)

            self._channel = self._connection.channel()

            self._channel.exchange.declare(exchange=settings.MESSAGING_EXCHANGE_NAME, exchange_type='fanout')
        return self._channel

    def send_msg(self, json_payload=None):
        self.channel.basic.publish(body=json_payload,
                                   # body
                                   routing_key='',  # routing key
                                   exchange=settings.MESSAGING_EXCHANGE_NAME,
                                   properties={
                                       'delivery_mode': 2,  # make message persistent
                                       'content_type': 'application/json',

                                   })
