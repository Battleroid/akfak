import json
import logging
import os
import socket
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait
from functools import partial
from multiprocessing import cpu_count
from queue import Queue
from threading import Thread
from typing import TypeVar
from uuid import uuid4
from copy import deepcopy, copy
from contextlib import contextmanager

import click
import structlog
from pykafka import KafkaClient
from pyzabbix import ZabbixMetric, ZabbixSender

from .util import load_config, sha256sum
from .log import setup_logging
from .enums import Level

StringType = TypeVar('StringType', str, bytes)

# our own logger (for JSON output), null out the pykafka logs
setup_logging()
pykafka_log = logging.getLogger('pykafka')
pykafka_log.setLevel(100)


# TODO: create class for logging using partials to set error/warn/info
@contextmanager
def timeout(t=5.0):
    org_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(t)
    yield
    socket.setdefaulttimeout(org_timeout)


class AkfakClient:
    """
    Manages the fetching & sending of offset info for a single client (cluster)
    """

    def __init__(self, config: dict):

        # config & settings
        self.uuid = str(uuid4())
        self.name = config['name']
        self.config = config

        # partial for logger
        # TODO: replace with log.bind and personal loggers for each
        self.log = structlog.get_logger('akfak.client', client=self)

        # connect to brokers
        try:
            self.kafka = KafkaClient(config['brokers'])
        except Exception as e:
            self.log.error('pykafka_broker_error', exc_info=e)

        # Build list of notifiers for sending
        self.notifiers = []
        for item in ('graphite', 'zabbix'):
            if item in self.config['settings']:
                self.notifiers.append(getattr(self, f'send_to_{item}'))

    def send_to_graphite(self, data: dict):
        """
        Send results to graphite

        Args:
            data (dict): results dict
        """
        fixed_data = data[next(iter(data))]
        prefix = self.config['settings']['graphite']['prefix']

        payload = []
        for topic, consumers in fixed_data.items():
            for consumer, lag_data in consumers.items():
                epoch = lag_data['epoch']
                total = lag_data['total']
                payload.append(
                    f'{prefix}.{self.name}.{topic}.{consumer}.total {total} {epoch}'
                )
                for part, part_lag_data in lag_data['parts'].items():
                    part_lag = part_lag_data['lag']
                    payload.append(
                        f'{prefix}.{self.name}.{topic}.{consumer}.{part} {part_lag} {epoch}'
                    )

        graphite_url = self.config['settings']['graphite']['url']
        graphite_port = int(self.config['settings']['graphite']['port'])
        chunk_size = 50
        chunks = [
            payload[x:x + chunk_size]
            for x in range(0, len(payload), chunk_size)
        ]
        for i, chunk in enumerate(chunks, 1):
            message = '\n'.join(chunk)
            message += '\n'
            try:
                s = socket.socket()
                s.settimeout(5.0)
                s.connect((graphite_url, graphite_port))
                s.sendall(message.encode('utf-8'))
            except Exception as e:
                self.log.error(
                    'graphite_send_error',
                    chunk=i,
                    total_chunks=len(chunks),
                    chunk_payload=chunk,
                    exc_info=e
                )
            finally:
                s.close()

    def send_to_zabbix(self, data: dict):
        """
        Send results (evaluated with rules) to zabbix

        Args:
            data (dict): results dict
        """
        zabbix_name = self.config['settings']['zabbix'].get('name', self.name)
        zabbix_url = self.config['settings']['zabbix']['url']
        zabbix_port = int(self.config['settings']['zabbix']['port'])
        zabbix_key = self.config['settings']['zabbix']['key']
        fixed_data = data[next(iter(data))]

        payload = []
        for topic, consumers in fixed_data.items():
            for consumer, lag_data in consumers.items():
                level = lag_data['zabbix']['level']
                epoch = lag_data['epoch']
                metric = ZabbixMetric(
                    zabbix_name,
                    f'{zabbix_key}[{topic},{consumer}]',
                    level.value,
                    clock=epoch
                )
                payload.append(metric)

        try:
            with timeout():
                zabbix_sender = ZabbixSender(zabbix_url, zabbix_port)
                zabbix_sender.send(payload)
        except Exception as e:
            self.log.error('zabbix_send_error', exc_info=e)

    def send_to_notifiers(self, data: dict):
        """
        Send to applicable notifiers

        Args:
            data (dict): results dict
        """
        for sender in self.notifiers:
            sender(data)

    def fetch(
        self, topic: StringType, consumer: StringType, consumer_config: dict
    ) -> dict:
        """
        Fetch individual topic:consumer pair offset information

        Args:
            topic (StringType): topic name
            consumer (StringType): consumer name
            consumer_config (dict): config for topic:consumer pair

        Returns:
            dict of offset information for partitions, total, and evaluation
            of zabbix rules inserted at the top level
        """

        # verify we're sending bytes, not raw str
        tname = bytes(topic, 'utf8') if isinstance(topic, str) else topic
        cname = bytes(consumer,
                      'utf8') if isinstance(consumer, str) else consumer

        # client + consumer
        conn = self.kafka.topics[tname]

        # fetch lag info & calc lag, offsets retry is set to 10ms
        # Why? Default is too high and this bit of brute forcing fixes
        # high fetch time
        latest_offsets = conn.latest_available_offsets()
        current_offsets = conn.get_simple_consumer(
            consumer_group=cname,
            auto_start=False,
            offsets_channel_backoff_ms=10
        ).fetch_offsets()

        # build results
        results = {'parts': {}}
        for p, v in current_offsets:
            # If the partition is -1, it's inactive, so set to 0 to avoid
            # subtracting REALLY_HIGH_OFFSET - -1 (e.g. add 1 to it lol)
            current = v.offset
            latest = latest_offsets[p].offset[0]
            lag = 0 if current == -1 else max(latest - current, 0)
            results['parts'][p] = {
                'lag': lag,
                'latest': latest,
                'current': current,
            }

        # some minor stats
        results['total'] = sum([v['lag'] for v in results['parts'].values()])
        results['latest'] = sorted([
            v['latest'] for v in results['parts'].values()
        ])[-1]
        results['current'] = sorted([
            v['current'] for v in results['parts'].values()
        ])[-1]
        results['epoch'] = int(time.time())

        # evaluate lag (for zabbix & human readable api if it exists)
        if self.config['settings'].get('zabbix', None):
            alerts = [(Level[name], value)
                      for name, value in consumer_config['alerts'].items()]
            evaluations = [(results['total'] >= threshold, level)
                           for level, threshold in alerts]
            max_level = list(
                filter(
                    lambda x: x[0],
                    sorted(evaluations, key=lambda x: x[1], reverse=True)
                )
            )
            if not max_level:
                max_level = Level.normal
            else:
                max_level = max_level[0][1]
            results['zabbix'] = {'level': max_level, 'name': max_level.name}

        return results

    def fetch_all(self) -> dict:
        """
        Fetch all given topic:consumer pair offset information

        Returns:
            returns complete result dict of all topic:consumer pairs
        """

        results = defaultdict(lambda: defaultdict(dict))
        cluster_name = self.config['name']

        for tname, topic_config in self.config['topics'].items():
            for cname, consumer_config in topic_config['consumers'].items():
                results[cluster_name][tname][cname] = self.fetch(
                    tname,
                    cname,
                    consumer_config,
                )

        return {'uuid': self.uuid, 'results': results}

    def __repr__(self):
        return f'<AkfakClient {self.uuid}:{self.name}>'


class AkfakServer:
    """
    Manages several Akfak clients from the given config dict
    """

    # TODO: cons needs to go away!
    def __init__(self, config: str, cons: bool = False):

        # config dict
        self.path = config
        self.config = load_config(config)
        self.org_sha = sha256sum(self.path)

        self.server_output = os.path.join(
            self.config['settings']['server_output'], 'server.json'
        )
        self.server_discovery = os.path.join(
            self.config['settings']['server_output'], 'discovery.json'
        )
        self.server_interval = self.config['settings']['server_interval']
        self.server_timeout = self.config['settings']['server_timeout']
        self.cons = cons

        # logging
        # TODO: set logger, set logging output to null if console output
        # TODO: need a better way to tell this to be quiet if set to be
        # console only output
        self.log = structlog.get_logger('akfak.server', name=self)

        # clients
        self.clients = {}
        try:
            for cluster_config in self.config['clusters']:
                client = AkfakClient(cluster_config)
                self.clients[client.uuid] = client
        except Exception as e:
            self.log.error('broker_setup_failure', exc_info=e)
            self.stop()
            raise click.Abort()

        # sending queue & thread
        self.send_queue = Queue(100)
        self.watch_thread = Thread(target=self._watch)
        self.watch_thread.daemon = True

        # signal for stopping
        self.done = False

        self.log.info('finished_setup', config_sha=self.org_sha)

    def _reload_config(self, new_sha):
        """
        Reload config with new values, do not apply anything if an exception 
        occurs while loading the new clients.
        """
        self.log.info('reload_config_started')

        # make a deepcopy so we don't just reassign a reference
        _org_config = deepcopy(self.config)
        _org_server_interval = deepcopy(self.server_interval)
        _org_server_timeout = deepcopy(self.server_timeout)
        _org_clients = copy(self.clients)
        _org_sha = copy(self.org_sha)

        try:
            new_config = load_config(self.path)
            server_interval = new_config['settings']['server_interval']
            server_timeout = new_config['settings']['server_timeout']

            # clients
            clients = {}
            for cluster_config in new_config['clusters']:
                client = AkfakClient(cluster_config)
                clients[client.uuid] = client

            # we need the clients to finish sending
            self.log.info(
                'joining_task_queue',
                queue_size=self.send_queue.qsize()
            )
            self.send_queue.join()

            # reassign config info
            self.config = new_config
            self.server_interval = server_interval
            self.server_timeout = server_timeout
            self.clients = clients
            self.org_sha = new_sha

            # build discovery playlist
            if self.config['settings'].get('zabbix'):
                self.build_discovery_playlist()
            else:
                with open(self.server_discovery, 'w') as fo:
                    json.dump({}, fo)
        except Exception as e:
            self.log.warn('reload_config_error', exc_info=e)

            # roll it back
            self.config = _org_config
            self.server_interval = _org_server_interval
            self.server_timeout = _org_server_timeout
            self.clients = _org_clients
            self.org_sha = _org_sha
        else:
            self.log.info('reload_config_successful')
        finally:
            self.log.info('reload_config_finished')


    def _watch(self):
        """
        Target watch process for send thread. Watches and blocks for incoming
        tasks in queue. Uses client :func:`AkfakClient.send_to_notifiers` func
        to send to all relevant notifiers per client.
        """
        # TODO: sending should be per client, so individual clients can send immediately
        while not self.done:
            results, epoch = self.send_queue.get(block=True)
            start_time = time.time()
            for uuid, result in results.items():
                client = self.clients[uuid]
                client.send_to_notifiers(result)
            self.send_queue.task_done()
            self.log.info(
                'sending_finished',
                task_epoch=epoch,
                duration=time.time() - start_time
            )

    def build_discovery_playlist(self):
        """
        Build Zabbix auto discovery playlist and dump to given path
        """
        payload = {}
        for cluster in self.config['clusters']:
            clstr_zbx = cluster['settings'].get('zabbix', {})
            name = clstr_zbx.get('name', cluster['name'])
            payload.setdefault(name, {'data': []})
            cluster_payload = payload[name]['data']
            for topic, topic_config in cluster['topics'].items():
                for consumer in topic_config['consumers'].keys():
                    cluster_payload.append({
                        '{#TOPIC}': topic,
                        '{#CONSUMER}': consumer
                    })

        if not self.cons:
            with open(self.server_discovery, 'w') as fo:
                json.dump(payload, fo, indent=2, sort_keys=True)
        else:
            print(json.dumps(payload, indent=2, sort_keys=True))

        self.log.info(
            'built_discovery_playlist', disc_output=self.server_discovery
        )

    def start(self):
        """
        Start fetching, notifying and updating
        """

        # build discovery playlist before we start
        if self.config['settings'].get('zabbix'):
            self.build_discovery_playlist()
        else:
            # TODO: dummy discovery file, api shouldn't require this if not needed
            with open(self.server_discovery, 'w') as fo:
                json.dump({}, fo)

        # Start watching send queue
        self.watch_thread.start()

        while not self.done:

            # TODO: check if config has been modified, reload if so
            cur_sha = sha256sum(self.path)
            if cur_sha != self.org_sha:
                self._reload_config(cur_sha)

            pe = ThreadPoolExecutor(
                max_workers=max([cpu_count(), len(self.clients)])
            )

            # send = for zabbix/graphite, api = for web (uuid not needed)
            api_results = defaultdict(lambda: defaultdict(dict))
            send_results = {}
            start_time = time.time()

            # with pe as exc:
            tasks = [
                pe.submit(client.fetch_all) for client in self.clients.values()
            ]

            done, pending = wait(tasks, timeout=self.server_timeout)

            # force shutdown so the remaining tasks no longer block
            pe.shutdown(wait=False)

            if pending:
                self.log.warn('tasks_exceeded_timeout', exceeded_tasks=pending)

            for task in pending:
                task.cancel()

            for task in done:
                client_result = task.result()
                uuid = client_result['uuid']
                payload = client_result['results']

                # update results per client
                for client_name, client_payload in payload.items():
                    api_results[client_name].update(client_payload)
                send_results[uuid] = payload

            with open(self.server_output, 'w') as fo:
                fo.write(json.dumps(api_results, indent=2))

            # TODO: unlikely the queue will be full, but need to catch this
            epoch = time.time()
            self.send_queue.put_nowait((send_results, int(epoch)))

            self.log.info(
                'offset_fetch_finished',
                task_epoch=int(epoch),
                duration=time.time() - start_time
            )
            time.sleep(self.server_interval)

    def stop(self):
        """
        Stop fetching & sending processes
        """
        self.done = True
        self.send_queue.join()
        if self.watch_thread.is_alive():
            self.log.info('shutting_off_send_thread')
            self.watch_thread.join(timeout=5)
        self.log.info('shutdown_finished')

    def __del__(self):
        self.log.info('received_shutdown_signal')
        self.stop()

    def __repr__(self):
        clients = ", ".join([
            f'{name}:{c.name}' for name, c in self.clients.items()
        ])
        return f'<AkfakServer ({clients})>'
