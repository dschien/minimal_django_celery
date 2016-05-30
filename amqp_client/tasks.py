import logging

import amqpstorm
from celery import Task
from celery import shared_task
from django.conf import settings

__author__ = 'schien'

logger = logging.getLogger(__name__)


class ErrorLoggingTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error("Task failed: %s" % einfo)


class AMQP_Task(Task):
    abstract = True
    _channel = None
    _connection = None

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error("AMQP_Task failed")
        logger.error(einfo)

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


@shared_task(base=AMQP_Task, bind=True)
def send_msg(self, json_payload):
    logger.debug('Publishing new message %s' % json_payload)
    print('sending')
    self.channel.basic.publish(body=json_payload,
                               # body
                               routing_key='',  # routing key
                               exchange=settings.MESSAGING_EXCHANGE_NAME,
                               properties={
                                   'delivery_mode': 2,  # make message persistent
                                   'content_type': 'application/json',

                               })
