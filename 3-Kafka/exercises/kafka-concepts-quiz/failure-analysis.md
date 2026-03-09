# Failure Analysis

## Scenario

Your Kafka cluster has 3 brokers. The "orders" topic has 3 partitions with replication factor 2:

| Partition | Leader | Follower |
|-----------|--------|----------|
| Partition 0 | Broker 1 | Broker 2 |
| Partition 1 | Broker 2 | Broker 3 |
| Partition 2 | Broker 3 | Broker 1 |

**Event: Broker 2 suddenly crashes.**

---

## Analysis Questions

### 1. Which partitions are affected?

Partition 1 is most effected because Broker 2 had its lead partition. It will need to make Broker 3 its new leader. It can no longer recover from additional failures.


### 2. What happens to Partition 0?

*Consider: Broker 2 was the follower for Partition 0. What is the impact?*

Since broker 2 was the follower for partition 0, its failure means that partition 0 no longer has fault tolerance and if broker 1 fails, the partition will be unavailable.


### 3. What happens to Partition 1?

*Consider: Broker 2 was the leader for Partition 1. Who becomes the new leader?*

The new leader for partition 1 will be broker 3.


### 4. Can producers still send messages to all partitions? Why or why not?

Yes. Since broker 3 became the new leader for partition 1, partition 0's leader is available on broker 1, and partition 3's leader is available on broker 3, all partitions can still be written to.


### 5. What is the cluster's replication status after the failure?

*Consider: How many replicas does each partition have now? Is the cluster at risk?*

The cluster is at risk of a severe failure. Partitions 0 and 1 both have a replication factor of 1, an additional broker failure will render one of those partitions inaccessible.


---

## Bonus Question

If Broker 3 also fails immediately after Broker 2, what happens to Partition 1?

If broker 3 also fails immediately after broker 2, Partition 1 is unavailable.
