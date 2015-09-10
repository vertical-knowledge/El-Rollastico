#!/usr/bin/env python
import logging.config

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s| %(name)s/%(processName)s[%(process)d]-%(threadName)s: %(message)s @%(funcName)s:%(lineno)d #%(levelname)s',
        }
    },
    'handlers': {
        'console': {
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        # 'logfile': {
        #     'formatter': 'standard',
        #     'class': 'logging.FileHandler',
        #     'filename': 'rollastic.log',
        # },
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO',
    },
    'loggers': {
        __name__: dict(level='DEBUG'),

        # These are super noisy
        'elasticsearch': dict(level='WARNING'),
        'requests': dict(level='WARNING'),
        'urllib3': dict(level='WARNING'),
    }
}

logging.config.dictConfig(LOGGING)

import logging

_LOG = logging.getLogger(__name__)

from distutils.version import LooseVersion, StrictVersion
from datetime import timedelta
import elasticsearch
import time
import click
import sys
import types
import re

try:
    import salt.client
except ImportError:
    pass
HAS_SALT = 'salt.client' in sys.modules


class Cluster(object):
    '''
    Represents an ES cluster.
    '''

    def __init__(self, hosts, timeout=None, sniff=False, connect_to_all_masters=True):
        '''
        Init

        :param hosts: A comma-separated string or an list of hosts to connect to
        :type hosts: str or list
        :param timeout: Client timeout
        :type timeout: int
        :param sniff: Enable ES sniffer (not recommended as it doesn't work that well)
        :type sniff: bool
        :param connect_to_all_masters: Once connected, get a list of all master nodes and connect to all of them.
        :type connect_to_all_masters: bool
        '''
        if isinstance(hosts, types.StringTypes):
            hosts = hosts.split(',')
        self.hosts = hosts

        es_opts = dict(
            timeout=timeout,
            retry_on_timeout=True,
        )
        if sniff:
            es_opts.update(dict(
                sniff_on_start=True,
                sniff_on_connection_fail=True,
            ))

        self.es = elasticsearch.Elasticsearch(self.hosts, **es_opts)

        if connect_to_all_masters:
            _LOG.info('Connecting to all master nodes')
            master_hosts = list()
            for node in self.iter_nodes():
                if node.is_master:
                    master_hosts.append(node.publish_host)
            _LOG.debug('master_hosts=%s', master_hosts)
            self.es = elasticsearch.Elasticsearch(master_hosts, **es_opts)

    def put_settings(self, settings, persistent=True):
        '''
        Push settings to cluster.

        :param settings: Dictionary of values to set.
        :type settings: dict
        :param persistent: If true, set persistently, else transiently
        :type persistent: bool
        :return: Success
        :rtype: bool
        '''
        cat = persistent and 'persistent' or 'transient'
        ret = self.es.cluster.put_settings({cat: settings})
        return ret['acknowledged'] is True

    def disable_allocation(self):
        _LOG.info('Disabling allocation')
        return self.put_settings({
            'cluster.routing.allocation.disable_allocation': 'true',
            # 'cluster.routing.allocation.node': 'none',
        })

    def enable_allocation(self):
        _LOG.info('Enabling allocation')
        return self.put_settings({
            'cluster.routing.allocation.disable_allocation': 'false',
            # 'cluster.routing.allocation.node': 'all',
        })

    def status(self):
        '''
        Get cluster health

        :return: Cluster health
        :rtype: str
        '''
        health = self.es.cluster.health()
        return health['status']

    def wait_until_green(self):
        '''
        Loops around until cluster health is green

        :return: Success (always True)
        :rtype: bool
        '''
        _LOG.info('Waiting until cluster is green')
        while True:
            status = self.status()
            if status == 'green':
                return True
            time.sleep(10)

    def node_ips(self):
        '''
        Return a list of IPs for all nodes in cluster.

        :return: List of node IPs
        :rtype: list
        '''
        raw = self.es.cat.nodes(h='ip')
        ret = []
        for line in raw.splitlines():
            line = line.rstrip('\n').strip()
            ret.append(line)
        return ret

    def has_node_ip(self, ip):
        '''
        Checks if IP is in cluster

        :param ip: IP
        :type ip: str
        :return: bool if in cluster
        :rtype: bool
        '''
        return ip in self.node_ips()

    def wait_until_node_joins(self, name, freshness_window=120):
        '''
        Loops around waiting until a node with the specified name joins the cluster with an uptime within
        freshness_window.

        :param name: Node name
        :type name: str
        :param freshness_window: How recent (in secs) the join must be to pass
        :type freshness_window: int
        :return: Node on Success
        :rtype: Node
        '''
        # TODO This could just check old uptime vs new uptime instead of having a freshness_window. This would alleviate issues with a recently restarted node (ie within window)
        _LOG.info('Waiting until node %s joins with a freshness_window of %d secs', name, freshness_window)
        while True:
            for n in self.iter_nodes():
                if n.name == name:
                    uptime = n.uptime
                    if not uptime:
                        _LOG.warn('Found node %s but uptime=%s?', n, uptime)
                        continue
                    if freshness_window and uptime.total_seconds() > freshness_window:
                        _LOG.debug('Found node %s but uptime=%s was under freshness_window=%ds',
                                   name, uptime.total_seconds(), freshness_window)
                        continue
                    _LOG.info('Found node %s with uptime=%s was within freshness_window=%ds',
                              name, uptime.total_seconds(), freshness_window)
                    return n
            time.sleep(10)

    def iter_nodes(self):
        '''
        Iters through all nodes in cluster.

        :return: Generator of nodes
        :rtype: generator
        '''
        return Node.iter_nodes(self)

    def rolling_helper(self, callback, node_filter=lambda self, node: True,
                       master=False, data=True,
                       initial_wait_until_green=True, wait_until_green=True, disable_allocation=True):
        '''
        Generic helper to perform rolling actions.

        :param callback: Callback to call per node that matches node_filter
        :type callback: function
        :param node_filter: Filter to call per node to see if we should run on it
        :type node_filter: function
        :param master: Include master nodes in this roll
        :type master: bool
        :param data: Include data nodes in this roll
        :type data: bool
        :param initial_wait_until_green: Wait until cluster is green before rolling
        :type initial_wait_until_green: bool
        :param wait_until_green: Wait until cluster is green after each callback
        :type wait_until_green: bool
        :param disable_allocation: Disable allocation before callback, enable afterwards
        :type disable_allocation: bool
        '''
        _LOG.info('Rolling through nodes on %s', self)

        nodes = list(self.iter_nodes())
        master_nodes = [n for n in nodes if n.is_master]
        data_nodes = [n for n in nodes if n.is_data]
        _LOG.info('Nodes: %d master, %d data', len(master_nodes), len(data_nodes))
        _LOG.debug('nodes=%s', nodes)

        if initial_wait_until_green:
            self.wait_until_green()

        roll_nodes = []
        if master:
            roll_nodes.extend(master_nodes)
        if data:
            roll_nodes.extend(data_nodes)
        _LOG.debug('roll_nodes=%s', roll_nodes)

        for node in roll_nodes:
            _LOG.debug('Node: %s', node)
            if node_filter(self, node):
                _LOG.info('Node matched filter: %s', node)
                if disable_allocation:
                    self.disable_allocation()

                # ready to run callback at this point
                callback(self, node)

                if disable_allocation:
                    self.enable_allocation()
                if wait_until_green:
                    self.wait_until_green()

    def rolling_restart(self, master=False, data=True, initial_wait_until_green=True,
                        heap_used_percent_threshold=85):
        '''
        Rolling restart.

        :param master: Include master nodes in this roll
        :type master: bool
        :param data: Include data nodes in this roll
        :type data: bool
        :param initial_wait_until_green: Wait until cluster is green before rolling
        :type initial_wait_until_green: bool
        :param heap_used_percent_threshold: Threshold of heap used (percentage) to initiate a roll. Use -1 to do all.
        :type heap_used_percent_threshold: int
        '''
        _LOG.info('Performing rolling restart on %s', self)
        # TODO Allow this to be ran without Salt again
        assert HAS_SALT

        saltcli = salt.client.LocalClient()

        def restart(self, node):
            _LOG.info('Found node with heap above threshold=%d: %s', heap_used_percent_threshold, node)

            nso = NodeSaltOps(saltcli, node)

            ''' Prep '''

            _LOG.info('Verifying I can ping node=%s through Salt', node)
            assert nso.ping()

            ''' Shutdown '''

            assert nso.ensure_elasticsearch_is_dead()

            ''' Start '''

            assert nso.service_start('elasticsearch')
            time.sleep(15)
            assert nso.wait_for_service_status('elasticsearch', True)

            ''' Wait until node joins '''

            self.wait_until_node_joins(node.name)

        node_filter = lambda self, node: node.heap_used_percent > heap_used_percent_threshold

        return self.rolling_helper(
            restart, node_filter,
            master=master, data=data,
            initial_wait_until_green=initial_wait_until_green,
        )

    def rolling_upgrade(self, minimum_version=None, master=False, data=True, initial_wait_until_green=True):
        '''
        Rolling upgrade.

        :param minimum_version: If node version is below this, then perform upgrade on it.
        :type minimum_version: str
        :param master: Include master nodes in this roll
        :type master: bool
        :param data: Include data nodes in this roll
        :type data: bool
        :param initial_wait_until_green: Wait until cluster is green before rolling
        :type initial_wait_until_green: bool
        '''
        _LOG.info('Performing rolling upgrade on %s', self)
        assert HAS_SALT

        saltcli = salt.client.LocalClient()

        def node_filter(self, node):
            if not minimum_version:
                return True
            return LooseVersion(node.version) < LooseVersion(minimum_version)

        def upgrade(self, node):
            nso = NodeSaltOps(saltcli, node)
            wait_for_rejoin = False

            ''' Prep '''

            _LOG.info('Verifying I can ping node=%s through Salt', node)
            assert nso.ping()

            ''' Highstate '''

            _LOG.info('Blazing it up (lighting a highstate) on node=%s', node)
            ret = nso.cmd('state.highstate', quiet=True)

            # Check for changes in the elasticsearch service from highstate run
            svc_changes = ret['service_|-elasticsearch_|-elasticsearch_|-running']['changes']
            if svc_changes:
                wait_for_rejoin = True
                _LOG.info('Salt elasticsearch service changes: %s', svc_changes)
            else:
                _LOG.info('Salt reported that no changes were performed on the elasticsearch service.')

            ''' HACK Work around broken pkg.latest in Salt '''

            upgradable = nso.cmd('pkg.available_version', ['elasticsearch'])
            if upgradable:
                _LOG.info('Working around broken pkg.latest in Salt')

                # We force a stop here because elasticsearch upgrades can make
                # service stop no longer work, leaving a zombie ES process that
                # sysvinit cannot control
                assert nso.ensure_elasticsearch_is_dead()
                wait_for_rejoin = True

                ret = nso.cmd('pkg.install', ['elasticsearch'])
                if ret.get('elasticsearch'):
                    wait_for_rejoin = True

            ''' Wait for node to rejoin (if applicable) '''

            if wait_for_rejoin:
                _LOG.info('Waiting for node=%s to rejoin', node)

                if not nso.service_status('elasticsearch'):
                    assert nso.service_start('elasticsearch')
                    time.sleep(15)
                    assert nso.wait_for_service_status('elasticsearch', True)
                self.wait_until_node_joins(node.name)

        return self.rolling_helper(
            upgrade, node_filter,
            master=master, data=data,
            initial_wait_until_green=initial_wait_until_green,
        )


class Node(dict):
    '''
    Represents a cluster node.
    '''

    def __init__(self, cluster, node_id=None):
        '''
        Init

        :param cluster: Cluster instance
        :type cluster: Cluster
        :param node_id: Node unique identifier
        :type node_id: str
        '''
        self.cluster = cluster
        self.node_id = node_id
        if self.node_id:
            self.populate()

    @classmethod
    def iter_nodes(cls, cluster):
        '''
        Iterates through all nodes.

        :param cluster: Cluster instance
        :type cluster: Cluster
        :return: Generator for all nodes
        :rtype: generator
        '''
        info = cluster.es.nodes.info(metric='settings')
        for node_id, node in info['nodes'].items():
            yield cls(cluster, node_id)

    def populate(self):
        '''
        Clears self and populates information from cluster.
        '''
        self.clear()

        info = self.cluster.es.nodes.info(self.node_id)['nodes'].get(self.node_id, {})
        if not info:
            _LOG.warning('Bad result for node info. node_id=%s info=%s', self.node_id, info)
        self.update(info)

        stats = self.cluster.es.nodes.stats(self.node_id)['nodes'].get(self.node_id, {})
        if not stats:
            _LOG.warning('Bad result for node stats. node_id=%s stats=%s', self.node_id, stats)
        self.update(stats)

    @property
    def name(self):
        '''
        :rtype: str
        '''
        return self.get('name')

    @property
    def version(self):
        '''
        :rtype: str
        '''
        return self['version']

    def __repr__(self):
        return '<{0.__class__.__name__} {0.name} master={0.is_master} data={0.is_data}>'.format(self)

    @property
    def is_master(self):
        '''
        :rtype: bool
        '''
        return self['settings']['node']['master'] == 'true'

    @property
    def is_data(self):
        '''
        :rtype: bool
        '''
        return self['settings']['node']['data'] == 'true'

    @property
    def heap_used_percent(self):
        '''
        :rtype: int
        '''
        return self['jvm']['mem']['heap_used_percent']

    @property
    def uptime(self):
        '''
        :rtype: int
        '''
        ms = self['jvm'].get('uptime_in_millis')
        if not ms:
            return
        seconds = self['jvm']['uptime_in_millis'] / 1000
        return timedelta(seconds=seconds)

    @property
    def publish_host(self):
        '''
        :return: Node published host address (just the address)
        :rtype: str
        '''
        http_addr = self['http_address']
        m = re.match(r'^inet\[(?P<publish_host>[^/]*)/(?P<publish_ip>[^\]]+)]$', http_addr)
        if not m:
            raise Exception('Could not match http_address: %s' % http_addr)
        for v in m.groups():
            if v:
                return v

    def shutdown(self):
        '''
        Shutdown node through ES API

        :return:
        :rtype:
        '''
        _LOG.info('Shutting down node=%s through elasticsearch API', self)
        assert self.name
        ret = self.cluster.es.nodes.shutdown(self.name)
        _LOG.debug(ret)
        return ret


class NodeSaltOps(object):
    '''
    Contains Salt operations on a Node.
    '''

    def __init__(self, saltcli, node):
        '''
        Init

        :param saltcli: Salt client instance
        :type saltcli: salt.client.LocalClient
        :param node: Node instance
        :type node: Node
        '''
        self.node = node
        self.s = saltcli

    def cmd(self, fun, arg=(), kwarg=None, quiet=False):
        '''
        Perform Salt command on Node.

        :param fun: Salt function
        :type fun: str
        :param arg: Args for function
        :type arg: list
        :param kwarg: Kwargs for function
        :type kwarg: dict
        :param quiet: If True, does not log return value to debug level
        :type quiet: bool
        :return: Results
        '''
        ret = self.s.cmd(self.node.name, fun, arg=arg, kwarg=kwarg)
        assert len(ret) == 1
        assert self.node.name in ret
        ret = ret.get(self.node.name, {})
        if not quiet:
            _LOG.debug('salt: %s(%s %s)=%s', fun, arg, kwarg, ret)
        return ret

    def ping(self):
        _LOG.info('Pinging node=%s', self.node)
        return bool(self.cmd('test.ping'))

    def service_status(self, name):
        '''
        :param name: Service name
        :type name: str
        :return: Service status
        :rtype: bool
        '''
        return self.cmd('service.status', [name])

    def service_start(self, name):
        '''
        :param str name: Service name
        :return: Bool if service was started
        :rtype: bool
        '''
        _LOG.info('Starting service=%s', name)
        return bool(self.cmd('service.start', [name]))

    def wait_for_service_status(self, name, status, check_every=10, timeout_iterations=6):
        '''
        Waits for service status with specified timeout.

        :param name: Service name
        :type name: str
        :param status: Status to wait for
        :type status: bool
        :param check_every: Seconds in between checks
        :type check_every: int
        :param timeout_iterations: Iterations of check_every secs before timing out
        :type timeout_iterations: int
        :return: True on success, False on timeout
        :rtype: bool
        '''
        _LOG.info('Waiting for service=%s status to be %s on node=%s', name, status, self.node)
        x = 0
        while True:
            ret = self.service_status(name)
            # This is "is status" to exclude the case of no response
            if ret is status:
                return True

            if timeout_iterations and x == timeout_iterations:
                return False

            time.sleep(check_every)
            x += 1

    def ensure_elasticsearch_is_dead(self, kill_on_shutdown_timeout=True):
        '''
        Stops Elasticsearch service and ensures it's dead. If kill_on_shutdown_timeout, if process does not die within
        120s then run a naive killall java on the box and wait until it's shown as dead.

        :param kill_on_shutdown_timeout: If we should attempt a killall java on the box if a shutdown does not work.
        :type kill_on_shutdown_timeout: bool
        :raises Exception: if we could not ensure ES is dead
        :return: True on success
        :rtype: bool
        '''
        _LOG.info('Ensuring elasticsearch is dead on node=%s', self.node)

        self.node.shutdown()

        # This will wait for up to one minute
        dead = self.wait_for_service_status('elasticsearch', False)
        if not dead:
            _LOG.warn('Timeout waiting for service=elasticsearch to die on node=%s', self.node)

            if kill_on_shutdown_timeout:
                _LOG.warn('Killing java on node=%s', self.node)
                # TODO retval on this?
                self.cmd('cmd.run', ['killall java'])
                time.sleep(15)

            # This will wait for up to another minute
            dead = self.wait_for_service_status('elasticsearch', False)
            if not dead:
                raise Exception("Could not stop service=elasticsearch on node=%s" % self.node)
        return dead


@click.group()
def cli():
    pass


@cli.command()
@click.argument('hosts', nargs=1)
@click.option('--sniff', default=False, help='Enable elastic sniffer [false]', is_flag=True)
@click.option('--masters', default=False, help='Restart master nodes as well [false]', is_flag=True)
@click.option('--datas', default=True, help='Restart data nodes [true]', is_flag=True)
@click.option('--kill-at-heap', default=85, help='Heap used percentage threshold to restart that node [85]',
              type=click.INT)
def restart(hosts, sniff, kill_at_heap, masters, datas):
    _LOG.info('Rolling restart with hosts=%s kill_at_heap=%s', hosts, kill_at_heap)

    cluster = Cluster(hosts, sniff=sniff)
    _LOG.info('Cluster status: %s', cluster.status())
    cluster.rolling_restart(master=masters, data=datas, heap_used_percent_threshold=kill_at_heap)


@cli.command()
@click.argument('hosts', nargs=1)
@click.option('--sniff', default=False, help='Enable elastic sniffer [false]', is_flag=True)
@click.option('--masters', default=False, help='Restart master nodes as well [false]', is_flag=True)
@click.option('--datas', default=True, help='Restart data nodes [true]', is_flag=True)
@click.option('--minimum-version', default='1.7.1', help='Minimum version to upgrade to [1.7.1]')
def upgrade(hosts, sniff, masters, datas, minimum_version):
    _LOG.info('Rolling upgrade with hosts=%s and minimum_version=%s', hosts, minimum_version)

    cluster = Cluster(hosts, sniff=sniff)
    _LOG.info('Cluster status: %s', cluster.status())
    cluster.rolling_upgrade(master=masters, data=datas, minimum_version=minimum_version)


if __name__ == '__main__':
    cli(auto_envvar_prefix='ROLLASTIC')
