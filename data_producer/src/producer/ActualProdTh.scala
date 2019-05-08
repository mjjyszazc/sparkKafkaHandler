package producer

import java.io.File
import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable


/**
  * Created by kfwb29 on 2018/9/7.
  */
object ActualProdTh {
  def main(args: Array[String]) {

    val conf =  ConfigFactory.parseFile(
      new File(System.getProperty("user.dir")+"/application.conf"))
    val prop = new Properties()
    prop.put("bootstrap.servers",conf.getString("kafka.bootstrap.servers"))
    prop.put("client.id", conf.getString("kafka.actualClient.id"))
    prop.put("key.serializer",classOf[StringSerializer])
    prop.put("value.serializer",classOf[StringSerializer])
    val kafkaProducer = new KafkaProducer[String,String](prop)

    val topic = conf.getString("kafka.actualTopic")
    //单次发送消息的时间间隔
    val intervalTime = conf.getInt("kafka.actualIntervalTime")
    var i = 0
    while(true){
      val startTime = System.currentTimeMillis()
      val message = (i+"#"+ (startTime%26+97).toChar.toString)
      i = i+1
      //val message = (startTime+"#"+ (startTime%26+97).toChar.toString)
      val producerRecord = new ProducerRecord[String,String](topic,i.toString,message)
      kafkaProducer.send(producerRecord)
      println(i+"---"+message)
      Thread.sleep(intervalTime)
    }
  }
}
