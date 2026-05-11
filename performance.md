# Performance

Performance tests are often conducted in very controlled, steady state conditions. That's important when making comparisons. But it can also be revealing to measure how performance changes over time when systems vary dynamically.

This plot was created using data collected during a connection pool test for an application that caches data. As the workload begins and the cache fills, performance improves for all configurations. But the *default* (red) configuration performs best because it is not constrained by the limits of a connection pool.

![](images/conn-pool.png)

This plot uses the same data set as above, but instead of a time series, the data is compared using box plots which clearly show how performance varies for each configuration. These box plots have long tails since workload performance improves over time.

![](images/conn-pool-2.png)

## 

This plot shows how application throughput changes as the number of workload threads increase for two different database drivers.

![](images/vcn-tps.png)

Based on the same performance test above, this plot shows average transaction response times for different database drivers and transaction types. In this case query transactions (blue) perform best followed by delete (red) and then insert/update (green) transaction types.

![](images/vcn-response.png)

These box plots show how a workload executing on each node (instance) of a distributed system perform over a long duration load test. The nodes are ordered from left to right based on the median TPS measurement for each node.

![](images/grid-tps.png)
