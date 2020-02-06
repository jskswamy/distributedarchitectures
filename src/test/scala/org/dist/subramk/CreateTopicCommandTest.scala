package org.dist.subramk

import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks


class CreateTopicCommandTest extends ZookeeperTestHarness {
  test("should create persistent path for topic with topic partition assignments in zookeeper") {
    val config = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient = new ZookeeperClientImpl(config)

    zookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    zookeeperClient.registerBroker(Broker(1, "10.10.10.11", 8001))
    zookeeperClient.registerBroker(Broker(2, "10.10.10.12", 8002))
    zookeeperClient.registerBroker(Broker(3, "10.10.10.11", 8003))
    zookeeperClient.registerBroker(Broker(4, "10.10.10.12", 8004))
    zookeeperClient.registerBroker(Broker(5, "10.10.10.12", 8005))

    val createCommandTest = new CreateTopicCommand(zookeeperClient)
    createCommandTest.createTopic("topic1", 2, 3)

    val topics = zookeeperClient.getAllTopics()
    assert(topics.size == 1)

    val partitionAssignments = topics("topic1")
    assert(partitionAssignments.size == 2)
    partitionAssignments.foreach(p => assert(p.brokerIds.size == 3))
  }
}
