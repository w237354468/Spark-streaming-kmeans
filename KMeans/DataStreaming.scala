package KMeans

import java.util.Properties
import java.util.concurrent._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{DoubleDeserializer, DoubleSerializer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object DataStreaming{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("" +
        "实时KMeans")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(2))

    val topics = Array("km")
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "192.168.11.128:9092,192.168.11.129:9092",
      "key.deserializer" -> classOf[DoubleDeserializer],
      "value.deserializer" -> classOf[DoubleDeserializer],
      "group.id" -> "KM",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[Double,Double](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[Double,Double](topics,kafkaParams)
    )

    stream.foreachRDD(rdd =>{
      val testData = rdd.map(data => (data.key(),data.value()))
      val rddForKMeans = new KMeans
      rddForKMeans.initialCenters(testData)
    })

    ssc.start()

    //反序列化做KMeans
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.11.128:9092,192.168.11.129:9092")
    properties.setProperty("key.serializer",classOf[DoubleSerializer].getName)
    properties.setProperty("value.serializer",classOf[DoubleSerializer].getName)
    properties.setProperty("acks","0")
    val kafkaProducer = new KafkaProducer[Double,Double](properties)

    //多线程写数据
    val numThread = 5
    val pool = Executors.newFixedThreadPool(numThread)
//    {
//      new ThreadPoolExecutor(numThread,numThread,
//        0L,TimeUnit.MILLISECONDS,
//        new LinkedBlockingDeque[Runnable]())
//    }
    pool.submit( new Runnable {
      override def run(): Unit = {
        while(true) {
          val record = new ProducerRecord[Double, Double](topics(0),
            Random.nextInt(100).toDouble,
            Random.nextInt(100).toDouble)
          kafkaProducer.send(record)
          Thread.sleep(300)
        }
      }
    }
    )
    ssc.awaitTermination()
  }
}