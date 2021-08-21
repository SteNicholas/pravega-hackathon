import csv
import logging
import os
import time
from subprocess import Popen, PIPE, STDOUT
from typing import Dict
from uuid import uuid1

import pandas as pd
import yaml
from ai_flow.model_center.entity.model_version_stage import ModelVersionEventType
from notification_service.client import NotificationClient
from notification_service.base_notification import EventWatcher, BaseEvent
from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from typing import List
import sys, getopt


class PravegaWatcher(EventWatcher):
    def __init__(self):
        super().__init__()

    def process(self, events: List[BaseEvent]):
        bash_command = ['java', '-jar',
                        '/Users/nicholas/Downloads/pravega_hackathon/pravega_writer/target/pravega_writer-1.0.0-jar-with-dependencies.jar']
        print('Submits the process with the bash command: {}'.format(' '.join(bash_command)))
        with open('stdout.log', 'a') as out, open('stderr.log', 'a') as err:
            sub_process = Popen(bash_command, stdout=out, stderr=err)
            sub_process.wait()


if __name__ == '__main__':
    notification_client = NotificationClient('localhost:50051', default_namespace='default')
    notification_client.start_listen_event(key='pravega.pravega_model', watcher=PravegaWatcher(), namespace='default',
                                           version=0,
                                           event_type=ModelVersionEventType.MODEL_GENERATED,
                                           start_time=0)
