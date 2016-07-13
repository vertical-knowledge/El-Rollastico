from el_rollastico.log import get_logger

_LOG = get_logger()

from datetime import timedelta
import time
import re
import sys

try:
    import salt.client
except ImportError:
    pass
HAS_SALT = 'salt.client' in sys.modules


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


class NodeSaltOps(object):
    '''
    Contains Salt operations on a Node.
    '''

    def __init__(self, node, saltcli=None):
        '''
        Init

        :param node: Node instance
        :type node: Node
        :param saltcli: Salt client instance
        :type saltcli: salt.client.LocalClient
        '''
        assert HAS_SALT

        self.node = node
        if not saltcli:
            saltcli = salt.client.LocalClient()
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

    def service_stop(self, name):
        '''
        :param str name: Service name
        :return: Bool if service was stopped
        :rtype: bool
        '''
        _LOG.info('Stopping service=%s', name)
        return bool(self.cmd('service.stop', [name]))

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
        self.service_stop('elasticsearch')

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
