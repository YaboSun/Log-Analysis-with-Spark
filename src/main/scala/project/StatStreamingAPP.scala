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
    //messages.map(_._2).count().print()

    // 测试步骤二： 数据清洗
    val logs = messages.map(_._2)
    val cleanData = logs.map(line => {
      // 主要是解析类似这样格式的
      // "GET /class/128.html HTTP/1.1"
      val infos = line.split("\t")
      // infos(2) = "GET /class/128.html HTTP/1.1"
      // url = /class/128.html
      val url = infos(2).split(" ")(1)


      // 进行筛选 只选取以class开头的课程编号
      if (url.startsWith("/class")) {
        val courseIdHtml = url.split("/")(2)
        val courseId = courseIdHtml.substring(0, courseIdHtml.lastIndexOf(".")).toInt
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
