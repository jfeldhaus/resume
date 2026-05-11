# Distributed Systems

The complex behavior of distributed systems is fascinating. The network graph below was generated using data from a distributed database during a load test. Each of the blue circles is a workload process connected to one of the database nodes. And each database node has a corresponding replica node to ensure fault tolerance. This is a healthy system where the workload is evenly distributed and every node is available.

![](images/nodes-1.png)

The graph below depicts a system experiencing severe problems where 8 of 10 nodes have failed (grey). This scenario occurred during a long running stress test. Database nodes were purposefully disabled to verify that client applications (red) automatically failed over to surviving nodes.

![](images/nodes-2.png)

The graphs below indicate which nodes (x axis) are associated with particular transactions (y axis). The left hand graph depicts a serious problem where transactions unnecessarily and inefficiently access all nodes in the system. The right hand graph depicts the same scenario after the problem was fixed. Only the nodes required to fulfill the request are accessed, resulting in an order of magnitude improvement in performance.

![](images/global-txn.png)

These graphs depict the same problem, but at the individual transaction level. On the left hand graph, when an application executes the UPDATE statement then all nodes in the system are unnecessarily accessed. The right hand graph shows how the application transaction can be modified using a SELECT statement to avoid the problem and improve performance. This optimization was eventually implemented in the database so that applications only need to execute the previously inefficient UPDATE statement.

![](images/global-idx.png)

This animation shows the row level resources acquired by various queries across the nodes of a distributed database. The location of the acquired rows depends on the node where the query is executed and whether any remote nodes have failed (grey columns).

![](images/grid-queries.gif)
