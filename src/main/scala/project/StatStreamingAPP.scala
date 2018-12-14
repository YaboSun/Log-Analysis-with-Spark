package project

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import project.utils.DateUtils

import scala.collection.mutable.ListBuffer

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
      var courseId = 0
      // 进行筛选 只选取以class开头的课程编号
      if (url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0 )

    // cleanData.print()

    // 测试步骤三： 统计今天到现在为止实战课程的访问量
    cleanData.map(x => {
      // HBase rowkey 设计：20181111_88
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })

    // 测试步骤四： 统计今天到现在为止从搜索引擎引流的实战课程的访问量
    cleanData.map(x => {
      /** 实现从这样的 “http://www.baidu.com/s?wd=Storm实战” 一个字符串中得到www.baidu.com */
      // 转化为“http:/www.baidu.com/s?wd=Storm实战”
      val referer = x.referer.replaceAll("//", "/")
      // 按照“/”进行拆分
      val splits = referer.split("/")

      var host = ""
      if (splits.length > 2) {
        host = splits(1)
      }

      (host, x.courseId, x.time)
    }).filter(_._1 != "").map(x => {
      (x._3.substring(0,8) + "_" + x._1 + "_" + x._2, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseSearchClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })

        CourseSearchClickCountDAO.save(list)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
