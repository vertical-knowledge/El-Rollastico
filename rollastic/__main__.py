from rollastic.log import get_logger

_LOG = get_logger()

from rollastic.cluster import Cluster
import click


@click.group()
def cli():
    pass


@cli.command()
@click.argument('master_node', nargs=1)
@click.option('--masters/--no-masters', default=False, help='Restart master nodes as well [false]')
@click.option('--datas/--no-datas', default=True, help='Restart data nodes [true]')
@click.option('--kill-at-heap', default=85, help='Heap used percentage threshold to restart that node [85]',
              type=click.INT)
@click.option('--highstate/--no-highstate', default=False,
              help='Run a highstate on each node prior to rolling. ES restart from a highstate is taken into account.')
def restart(master_node, kill_at_heap, masters, datas, highstate):
    '''
    Rolling restart of cluster.

    MASTER_NODE is the initial node to query to get the list of master nodes to connect to. Rollastic will connect to all master nodes to avoid relying on one to be up for the roll procedure.

    \b
    This will:
      - Collect and order the nodes to roll.
        If you opted to include master nodes, they are always done first.
      - Wait until cluster is in green health
      - For each node from #1 above
        If node's heap used percentage is over kill-at-heap:
        * Disable cluster allocation
        * Ping node through Salt to verify connectivity
        * If --highstate was specified, run a highstate:
          If the highstate fails, fail Rollastic.
        * If ES service was not restarted during a highstate:
          * Shutdown node
          * Wait for ES to die for 2m.
            If it's not dead, run a killall java and wait another 2m.
            If it's still not dead, fail.
          * Start elasticsearch service through Salt
        * Wait until node joins cluster with an uptime within 120s.
        * Enable allocation
        * Wait until cluster is in green health
    '''
    _LOG.info('Rolling restart with master_node=%s kill_at_heap=%s', master_node, kill_at_heap)

    cluster = Cluster(master_node)
    _LOG.info('Cluster status: %s', cluster.status())
    cluster.rolling_restart(master=masters, data=datas, heap_used_percent_threshold=kill_at_heap, highstate=highstate)


@cli.command()
@click.argument('master_node', nargs=1)
@click.option('--masters/--no-masters', default=False, help='Restart master nodes as well [false]')
@click.option('--datas/--no-datas', default=True, help='Restart data nodes [true]')
@click.option('--minimum-version', default='1.7.1', help='Minimum version to upgrade to [1.7.1]')
@click.option('--hold', is_flag=True, default=False, help='Override ''held'' elasticsearch package mark, and re-mark as ''held'' once upgraded. '
              'Cannot be combined with the --unhold flag. This works on Debian based systems only.')
@click.option('--unhold', is_flag=True, default=False, help='Override ''held'' elasticsearch package mark, and ''unhold'' package once upgraded. '
              'Cannot be combined with the --hold flag. This works on Debian based systems only.')
def upgrade(master_node, masters, datas, minimum_version, hold, unhold):
    '''
    Rolling upgrade of cluster.

    MASTER_NODE is the initial node to query to get the list of master nodes to connect to. Rollastic will connect to all master nodes to avoid relying on one to be up for the roll procedure.

    \b
    This will:
      - Collect and order the nodes to roll.
        If you opted to include master nodes, they are always done first.
      - Wait until cluster is in green health
      - For each node from #1 above
        If node's ES version is under minimum_version:
        * Disable cluster allocation
        * Ping node through Salt to verify connectivity
        * Run a Salt highstate
        * Check for an available upgrade on the Elasticsearch package, if so:
          - Shutdown node
          - Wait for ES to die for 2m.
            If it's not dead, run a killall java and wait another 2m.
            If it's still not dead, fail.
        * If ES was stopped at any point in this:
          - Start elasticsearch service if it's not already started
          - Wait until node joins cluster with an uptime within 120s.
        * Enable allocation
        * Wait until cluster is in green health
    '''
    # Assert that incompatible arguments are not specified, and determine hold policy
    assert not (hold and unhold)
    hold_package = None
    if hold:
        hold_package = True
    elif unhold:
        hold_package = False
    
    _LOG.info('Rolling upgrade with master_node=%s and minimum_version=%s, hold_package=%s', master_node, minimum_version, hold_package)
    
    cluster = Cluster(master_node)
    _LOG.info('Cluster status: %s', cluster.status())
    cluster.rolling_upgrade(master=masters, data=datas, minimum_version=minimum_version, hold_package=hold_package)

    
if __name__ == '__main__':
    cli(auto_envvar_prefix='ROLLASTIC')
