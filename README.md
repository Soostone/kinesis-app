

## TODOs

### Immediate Needs

- Add verbosity to config and disable majority of stdout messages
- Needs an automated test suite
- Need to test/debug multi-node operation
- Need to test/debug shard splits and merges

### Later Investigation
- Add physical node awareness to balancing (for multiple processes per
  machine)
- How to handle shard splits and merges
    - Take a look at PivotCloud repository
- Simple web GUI to monitor cluster and basically elaborate
  ClusterState
    - Spin up a local webserver via snap/warp/etc. to periodically
      poll for cluster state and show some metrics.

