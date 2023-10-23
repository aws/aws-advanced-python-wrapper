# Failover Configuration Guide

## Tips to Keep in Mind

### Failover Time Profiles
A failover time profile refers to a specific combination of failover parameters that determine the time in which failover should be completed and define the aggressiveness of failover. Some failover parameters include `failover_timeout_sec` and `failover_reader_connect_timeout_sec`. Failover should be completed within 5 minutes by default. If the connection is not re-established during this time, then the failover process times out and fails. Users can configure the failover parameters to adjust the aggressiveness of the failover and fulfill the needs of their specific application. For example, a user could take a more aggressive approach and shorten the time limit on failover to promote a fail-fast approach for an application that does not tolerate database outages. Examples of normal and aggressive failover time profiles are shown below. 
<br><br>
**:warning:Note**: Aggressive failover does come with its side effects. Since the time limit on failover is shorter, it becomes more likely that a problem is caused not by a failure, but rather because of a timeout.
<br><br>
#### Example of the configuration for a normal failover time profile:
| Parameter                                    | Value |
|----------------------------------------------|-------|
| `failover_timeout_sec`                       | `300` |
| `failover_writer_reconnect_interval_sec`     | `2`   |
| `failover_reader_connect_timeout_sec`        | `30`  |
| `failover_cluster_topology_refresh_rate_sec` | `2`   |

#### Example of the configuration for an aggressive failover time profile:
| Parameter                                    | Value |
|----------------------------------------------|-------|
| `failover_timeout_sec`                       | `30`  |
| `failover_writer_reconnect_interval_sec`     | `2`   |
| `failover_reader_connect_timeout_sec`        | `10`  |
| `failover_cluster_topology_refresh_rate_sec` | `2`   |

### Writer Cluster Endpoints After Failover
Connecting to a writer cluster endpoint after failover can result in a faulty connection because there can be a delay before the endpoint is updated to point to the new writer. On the AWS DNS server, this change is usually updated after 15-20 seconds, but the other DNS servers sitting between the application and the AWS DNS server may take longer to update. Using the stale DNS data will most likely cause problems for users, so it is important to keep this in mind.
### 2-Host Clusters
Using failover with a 2-host cluster is not beneficial because during the failover process involving one writer host and one reader host, the two hosts simply switch roles; the reader becomes the writer and the writer becomes the reader. If failover is triggered because one of the hosts has a problem, this problem will persist because there aren't any extra hosts to take the responsibility of the one that is broken. Three or more database hosts are recommended to improve the stability of the cluster.

### Host Availability
A common misconception about failover is the expectation that only one host will be unavailable during the failover process; this is actually not true. When failover is triggered, all hosts become unavailable for a short time. This is because the control plane, which orchestrates the failover process, first shuts down all hosts, then starts the writer host, and finally starts and connects the remaining hosts to the writer. In short, failover requires each host to be reconfigured and thus, all hosts become unavailable for a short period of time. With this in mind, please note that aggressive failover configurations may cause failover to fail because some hosts may still be unavailable when your failover timeout setting is reached.

### Monitor Failures and Investigate
If you are experiencing difficulties with the failover plugin, try the following:
- Enable [logging](/docs/using-the-python-driver/UsingThePythonDriver.md#logging) to find the cause of the failure. If it is a timeout, review the [failover time profiles](#failover-time-profiles) section and adjust the timeout values.
- For additional assistance, visit the [getting help page](../../README.md#getting-help-and-opening-issues).
