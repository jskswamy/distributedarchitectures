package org.dist.subramk

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.util.Networks

class TopicChangeHandlerTest extends ZookeeperTestHarness {

  test("should register for topic change and get replica assignments") {
    val config = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient = new ZookeeperClientImpl(config)
    var actualReplicas: Seq[PartitionReplicas] = List()
    zookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    zookeeperClient.registerBroker(Broker(1, "10.10.10.11", 8001))
    zookeeperClient.registerBroker(Broker(2, "10.10.10.12", 8002))

    val createTopicCommand = new CreateTopicCommand(zookeeperClient)
    val topicChangeListener = new TopicChangeHandler(zookeeperClient, (_, replicas) => {
      actualReplicas = replicas
    })
    zookeeperClient.subscribeTopicChangeListener(topicChangeListener)
    createTopicCommand.createTopic("topic1", 2, 2)

    TestUtils.waitUntilTrue(() => {
      actualReplicas.nonEmpty
    }, "Waiting for topic metadata", 1000)
  }
}
