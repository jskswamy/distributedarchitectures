package org.dist.subramk

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging

class BrokerChangeListener(zookeeperClient: ZookeeperClient) extends IZkChildListener with Logging {
  var count = 0

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    count = currentChilds.size()
  }
}
