package project

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author YaboSun
  *
  * 使用SparkStreaming处理Kafka的数据
  */
object StatStreamingAPP {
  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("Usage: StatStreamingAPP <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf()
      .setAppName("StatStreamingAPP")
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    // 测试步骤一： 测试数据接收
    messages.map(_._2).count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}
