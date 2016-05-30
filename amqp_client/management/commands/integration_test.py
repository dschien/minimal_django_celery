import logging

import simplejson as json
from django.conf import settings
from django.core.management import BaseCommand

from amqp_client import tasks
from amqp_client.controller import AMQP_Controller

__author__ = 'schien'

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Test'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        print(settings.MESSAGING_HOST)
        # AMQP_Controller().send_msg(json.dumps("test"))

        tasks.send_msg.delay(json.dumps("test"))
