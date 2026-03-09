# Kafka Architecture Concepts - Answers

## Part 1: Concept Questions

### 1. What is the difference between a topic and a partition?

The key difference is that a partition is a component of a topic. A topic is a component of a broker which producers write to and consumers read from. The data on a topic is split among different partitions based on a partition key. 


### 2. Why might you choose to have multiple partitions for a single topic?

You may choose to have multiple partitions for a single topic if you want to group messages based on a key value. Partitions also allow for parallelism since multiple consumers can read from different partitions at the same time.


### 3. What is the role of ZooKeeper in a Kafka cluster? What is KRaft and how is it different?

ZooKeeper is a coordinator in a Kafka cluster. It elects new leader replicas when the current leader goes down. KRaft is another coordinator, but it is more modern. It is built in to Kafka and performs better at scale.


### 4. Explain the difference between a leader replica and a follower replica.

The leader replica is the one that producers write to and it maintains the source of truth for the replicas. All of the follower replicas copy changes from the leader. The followers are able to be read from to increase the number of concurrent reads and also they are on standby to become the new leader if the current leader system fails.


### 5. What does ISR stand for, and why is it important for fault tolerance?

ISR stands for In-Sync Replica. This refers to the set of follower replicas that are up to date with the leader replica. It is important for fault-tolerance because the set of ISRs are the replicas that are eligible to become the new leader if the current leader goes down. 


### 6. How does Kafka differ from traditional message queues like RabbitMQ?

Messaging queues have a message written to a queue by a producer and then the data is read from the queue once. Kafka is different in that it uses pub/sub messaging instead of queue messaging. This means that the publisher writes data to a channel, and 0 or more consumers can read from the channel, each getting their own copy of the message. 


### 7. What is the publish-subscribe pattern, and how does Kafka implement it?

The publish-subscribe pattern is a messaging pattern in which one publisher writes messages to a channel, and multiple consumers subscribe to that channel to receive the messages. It allows one published message to be viewed by many consumers. Kafka implements it using by having Broker servers manage writes and reads from Topics. The topics are analogous to the channel.


### 8. What happens when a Kafka broker fails? How does the cluster recover?

When a Kafka broker fails, there should be one or more other brokers which store replicas of the topics and their partitions. This allows KRaft to elect new leader partitions for any leader partitions that were in the failed broker from the remaining brokers. 