Rollastic
=========

[![Join the chat at https://gitter.im/vertical-knowledge/rollastic](https://badges.gitter.im/vertical-knowledge/rollastic.svg)](https://gitter.im/vertical-knowledge/rollastic?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Elasticsearch cluster management, namely rolling restart and upgrades.
Your cluster should be deployed/managed via SaltStack; this is meant to be ran on your Salt master.

While currently not a *hard* requirement (as far as states applied on an upgrade, a highstate is performed), we
recommend to use our fork of the elasticsearch Salt formula: https://github.com/vkgit/saltstack-elasticsearch-formula,
as further integration will happen in the future.


Install
-------

```
pip install rollastic
```

Usage
-----

### Restart

```
Usage: rollastic restart [OPTIONS] MASTER_NODE

  Rolling restart of cluster.

  MASTER_NODE is the initial node to query to get the list of master nodes
  to connect to. Rollastic will connect to all master nodes to avoid relying
  on one to be up for the roll procedure.

  This will:
    - Collect and order the nodes to roll.
      If you opted to include master nodes, they are always done first.
    - Wait until cluster is in green health
    - For each node from #1 above
      If node's heap used percentage is over kill-at-heap:
      * Disable cluster allocation
      * Ping node through Salt to verify connectivity
      * Shutdown node
      * Wait for ES to die for 2m.
        If it's not dead, run a killall java and wait another 2m.
        If it's still not dead, fail.
      * If --highstate was specified, run a highstate:
        If the highstate fails, fail Rollastic.
      * Start elasticsearch service through Salt
      * Wait until node joins cluster with an uptime within 120s.
      * Enable allocation
      * Wait until cluster is in green health
Options:
  --masters / --no-masters   Restart master nodes as well [false]
  --datas / --no-datas       Restart data nodes [true]
  --kill-at-heap INTEGER     Heap used percentage threshold to restart that
                             node [85]
  --highstate/--no-highstate Run a highstate on each node prior to rolling.
			     ES restart from a highstate is taken into account. [false]
  --help                     Show this message and exit.
```

### Upgrade

```
Usage: rollastic upgrade [OPTIONS] MASTER_NODE

  Rolling upgrade of cluster.

  MASTER_NODE is the initial node to query to get the list of master nodes
  to connect to. Rollastic will connect to all master nodes to avoid relying
  on one to be up for the roll procedure.

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

Options:
  --masters / --no-masters  Restart master nodes as well [false]
  --datas / --no-datas      Restart data nodes [true]
  --minimum-version TEXT    Minimum version to upgrade to [1.7.1]
  --hold                    Override held elasticsearch package mark, and re-
                            mark as held once upgraded. Cannot be combined
                            with the --unhold flag. This works on Debian
                            based systems only.
  --unhold                  Override held elasticsearch package mark, and
                            unhold package once upgraded. Cannot be combined
                            with the --hold flag. This works on Debian based
                            systems only.
  --help                    Show this message and exit.
```
