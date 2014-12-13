

## TODOs

### Immediate Needs
- Needs an automated test suite
- Modify node so multiple processes from same machine is safe
- Need to test/debug multi-node operation
- Need to test/debug shard splits and merges
- Need to test config locking

### Later Investigation
- How to handle shard splits and merges
    - Take a look at PivotCloud repository
- Simple web GUI to monitor cluster and basically elaborate
  ClusterState
    - Spin up a local webserver via snap/warp/etc. to periodically
      poll for cluster state and show some metrics.
