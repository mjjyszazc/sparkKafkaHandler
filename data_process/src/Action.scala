import java.io.FileInputStream
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import utils.KafkaFactory

/**
  * Created by kfwb29 on 2018/9/7.
  */
object Action {

  def main(args: Array[String]) {

    val properties = new Properties()
    properties.load(new FileInputStream("application.properties"))

    val conf = new SparkConf().setAppName(properties.getProperty("spark.appName"))
      .set("spark.default.parallelism", properties.getProperty("spark.default.parallelism"))                             //spark 任务并行数
      .set("spark.driver.allowMultipleContexts", properties.getProperty("spark.driver.allowMultipleContexts"))
      .set("spark.speculation",properties.getProperty("spark.speculation"))                                     //关闭慢任务推测
      .set("spark.streaming.blokerInteval", properties.getProperty("spark.streaming.blokerInteval"))
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.driver.writeAheadLog.allowBatching","false") //解决程序被强行停止时WAL缓存问题
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Durations.seconds(properties.getProperty("spark.streaming.durationSeconds").toLong))

    ssc.sparkContext.setLogLevel(properties.getProperty("spark.context.logLevel"))
    ssc.checkpoint(properties.getProperty("spark.checkPoint.Path"))

    val delayTopic = Set[String](properties.getProperty("kafka.delay.topic"))
    val actualTopic = Set[String](properties.getProperty("kafka.actual.topic"))

    //direct方式从kafka拉取的数据
    val actualStream:InputDStream[ConsumerRecord[String, String]] =  KafkaUtils.createDirectStream[String,String](ssc,
      PreferConsistent,Subscribe[String,String](actualTopic,getKafkaParams(
        properties.getProperty("kafka.bootstrap.servers"),properties.getProperty("kafka.actualConsumer.groupId"),properties.getProperty("kafka.auto.offset.reset"))))
    val delayStream:InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](ssc,
      PreferConsistent,Subscribe[String,String](delayTopic,getKafkaParams(
        properties.getProperty("kafka.bootstrap.servers"),properties.getProperty("kafka.delayConsumer.groupId"),properties.getProperty("kafka.auto.offset.reset"))))

    submitOffsetToKafka(actualStream)
    submitOffsetToKafka(delayStream)

     val actWindow :DStream[(String, String)]= actualStream.map(record=>(record.key(),record.value().split("#")))
                    .filter(record=>record._2.length==2)
                    .map(record=>(record._2(0),record._2(1)))

     val delWindow:DStream[(String, String)]= delayStream.map(record=>(record.key(),record.value().split("#")))
                    .filter(record=>record._2.length==2)
                    .map(record=>(record._2(0),record._2(1)))

     val joinedStream :DStream[(String, (String, String))]= delWindow.window(Seconds(properties.getProperty("sparkStreaming.joinWindow.seconds").toInt),
      Seconds(properties.getProperty("sparkStreaming.slideWindow.seconds").toInt))
      .join(actWindow.window(Seconds(properties.getProperty("sparkStreaming.joinWindow.seconds").toInt),Seconds(properties.getProperty("sparkStreaming.slideWindow.seconds").toInt)))

    //join后的DStream写入kafka
    joinedStream.foreachRDD(rdd=>{
      println("============================")
      rdd.foreachPartition(partition=>{
        val kafkaProducer = KafkaFactory.getOrCreateProducer(properties.getProperty("kafka.bootstrap.servers"))
        partition.foreach(record=>{
          kafkaProducer.send( properties.getProperty("kafka.joinDStream.topic"),record._1,record._2._1+record._2._2)
          println(record.toString())
      })})
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /*
    获取kafkaConsumer参数
   */
  def getKafkaParams(server:String,groupID:String,offsetRest:String): Map[String,Object] ={
    val kafkaParams = Map[String,Object](
      "bootstrap.servers"->server,
      "group.id"->groupID,
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "enable.auto.commit"->"false",
      "auto.offset.reset"->offsetRest
    )
    kafkaParams
  }

  /*
  异步提交方式在kafka topic中维护offset  异步提交一般要等很久 所以二次启动会出现消息重复消费
   */
  def submitOffsetToKafka(dstream:InputDStream[ConsumerRecord[String, String]]): Unit = {
    dstream.foreachRDD(rdd => {
      val offsetRangs = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRangs(TaskContext.get().partitionId())
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangs)
    })
  }
}
