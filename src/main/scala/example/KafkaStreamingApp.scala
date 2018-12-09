package example

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author YaboSun
  *
  * SparkStreaming对接Kafka的方式一 基于Receiver实现WordCount
  */
object KafkaStreamingApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Usage: KafkaStreamingApp <zkQuorum> <group> <topic> <numThreads>")
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf()
      .setAppName("KafkaReceiverWordCount")
      .setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    // SparkStreaming如何对接kafka
    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)

    // 这里为什么要取第二个
    messages.map(_._2).count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}
