package org.dist.subramk

import com.google.common.annotations.VisibleForTesting
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.I0Itec.zkclient.exception.{ZkMarshallingError, ZkNoNodeException}
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.dist.kvstore.JsonSerDes
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker

import scala.jdk.CollectionConverters._

trait ZookeeperClient {
  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]]

  def registerSelf()
}

class ZookeeperClientImpl(config: Config) extends ZookeeperClient {
  val BrokerIdsPath = "/brokers/ids"
  private val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)

  override def registerSelf(): Unit = {
    val broker = Broker(config.brokerId, config.hostName, config.port)
    registerBroker(broker)
  }

  @VisibleForTesting
  def registerBroker(broker: Broker): Unit = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }

  def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case _: ZkNoNodeException =>
        createParentPath(client, path)
        client.createEphemeral(path, data)
    }
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  private def getBrokerPath(id: Int) = {
    BrokerIdsPath + "/" + id
  }

  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }
}

object ZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data: Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes: Array[Byte]): Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}
