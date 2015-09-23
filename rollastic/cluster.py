from rollastic.log import get_logger

_LOG = get_logger()

from rollastic.node import Node, NodeSaltOps, HAS_SALT

from distutils.version import LooseVersion
import elasticsearch
import time
import types


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

    def wait_until_green(self, check_every=5):
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
            time.sleep(check_every)

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

    def wait_until_node_joins(self, name, uptime_less_than=None, freshness_window=120, check_every=5):
        '''
        Loops around waiting until a node with the specified name joins the cluster with an uptime within
        freshness_window.

        :param name: Node name
        :type name: str
        :param uptime_less_than: If specified, uptime must be less than this. Meant to handle where a node was already restarted within freshness_window.
        :type uptime_less_than: int
        :param freshness_window: How recent (in secs) the join must be to pass
        :type freshness_window: int
        :return: Node on Success
        :rtype: Node
        '''
        _LOG.info('Waiting until node %s joins with a freshness_window of %d secs and uptime_less_than=%d', name, freshness_window, uptime_less_than)
        while True:
            for n in self.iter_nodes():
                if n.name == name:
                    if not n.uptime:
                        _LOG.warn('Found node %s but it was lacking uptime?', n)
                        continue
                    uptime = n.uptime.total_seconds()
                    if not uptime:
                        _LOG.warn('Found node %s but uptime=%s?', n, uptime)
                        continue
                    if freshness_window and uptime > freshness_window:
                        _LOG.debug('Found node %s but uptime=%s was under freshness_window=%ds',
                                   name, uptime, freshness_window)
                        continue
                    if uptime_less_than and uptime > uptime_less_than:
                        _LOG.debug('Found node %s but uptime=%s was above uptime_less_than=%s',
                                   name, uptime, uptime_less_than)
                        continue
                    _LOG.info('Found node %s with uptime=%s was within freshness_window=%ds and uptime_less_than=%s',
                              name, uptime, freshness_window, uptime_less_than)
                    return n
            time.sleep(check_every)

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

        # TODO Allow this to be ran without Salt again (states must restart ES on their own, eg swap to upstart)
        if not HAS_SALT:
            raise Exception("Salt is currently needed to restart the Elasticsearch service on each node.")

        def restart(self, node):
            _LOG.info('Found node with heap above threshold=%d: %s', heap_used_percent_threshold, node)

            nso = NodeSaltOps(node)

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

            self.wait_until_node_joins(node.name, uptime_less_than=node.uptime.total_seconds())

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

        if not HAS_SALT:
            raise Exception("Salt is required to perform a rolling upgrade.")

        def node_filter(self, node):
            if not minimum_version:
                return True
            return LooseVersion(node.version) < LooseVersion(minimum_version)

        def upgrade(self, node):
            nso = NodeSaltOps(node)
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
                self.wait_until_node_joins(node.name, uptime_less_than=node.uptime.total_seconds())

        return self.rolling_helper(
            upgrade, node_filter,
            master=master, data=data,
            initial_wait_until_green=initial_wait_until_green,
        )
