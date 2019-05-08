package utils

import org.apache.log4j.Logger

import scala.collection.mutable

/**
  * Created by 80374643 on 2018/03/03.
  * 保证在整个Streaming应用运行过程中，只须为每个Executor创建一个Kafka连接
  */

object KafkaFactory {
  private val logger = Logger.getLogger(getClass)
  private val threadLocal = new ThreadLocal[mutable.Map[String, StreamWriter]]

  def getOrCreateProducer(brokers: String): StreamWriter = {
    var producerMap = threadLocal.get()
    if (producerMap == null) {
      producerMap = mutable.Map[String, StreamWriter]()
      threadLocal.set(producerMap)
    }
    producerMap.getOrElseUpdate(brokers, {
      logger.info(s"Create Kafka producer , brokers: $brokers")
      val kafkaClient = new StreamWriter(brokers)

      kafkaClient
    })
  }
}
