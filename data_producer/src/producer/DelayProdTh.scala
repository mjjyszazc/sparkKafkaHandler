package producer

import java.io.{FileInputStream, File}
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}

/**
  * Created by kfwb29 on 2018/9/7.
  */
object DelayProdTh {
  def main(args: Array[String]) {

    val config =  ConfigFactory.parseFile(
      new File(System.getProperty("user.dir")+"/application.conf"))
    val conf = ConfigFactory.load(config)
    val prop = new Properties()
    prop.put("bootstrap.servers",conf.getString("kafka.bootstrap.servers"))
    prop.put("client.id", conf.getString("kafka.delayClient.id"))
    prop.put("key.serializer",classOf[StringSerializer])
    prop.put("value.serializer",classOf[StringSerializer])
    val kafkaProducer = new KafkaProducer[String,String](prop)

    val topic = conf.getString("kafka.DelayTopic")
    //单次发送消息的时间间隔
    val intervalTime = conf.getInt("kafka.delayIntervalTime")
    var i = 0
    while(true){
      val startTime = System.currentTimeMillis()
      //val message = (startTime+3+"#"+((startTime)%26+97).toChar.toString)
      val message = (i+"#"+((startTime)%26+97).toChar.toString)
      i = i+1
      val producerRecord = new ProducerRecord[String,String](topic,i.toString,message)
      kafkaProducer.send(producerRecord)
      println(i+"---"+message)
      Thread.sleep(intervalTime)
    }
  }
}
