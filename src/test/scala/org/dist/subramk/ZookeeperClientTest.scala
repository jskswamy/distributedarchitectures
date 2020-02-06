package org.dist.subramk

import org.dist.queue.common.KafkaZookeeperClient
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks


class ZookeeperClientTest extends ZookeeperTestHarness {
  test("should send LeaderAndFollower requests to all leader and follower brokers for given topicandpartition") {
    val host = new Networks().hostname()
    val port = TestUtils.choosePort()
    val config = Config(1, host, port, zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config)
    val actualClient = KafkaZookeeperClient.getZookeeperClient(config, config => config.zkConnect)

    zookeeperClient.registerSelf()

    val allBrokers = ZkUtils.getAllBrokersInCluster(actualClient)

    assertResult(1)(allBrokers.size)
    assertResult(Broker(1, host, port))(allBrokers.head)
  }
}
