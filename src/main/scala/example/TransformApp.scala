package example

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author YaboSun
  * 黑名单过滤
  */
object TransformApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")

    /**
      * 创建StreamingContext需要两个参数：SparkConf和batch interval
      */
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    /**
      * 构建黑名单
      */
    val blacks = List("zs", "ls")
    // 将集合转换为RDD
    val blacksRDD = ssc.sparkContext.parallelize(blacks)
    val blacksRDDMap = blacksRDD.map(x => (x, true))

    // 传入的测试数据
    val lines = ssc.socketTextStream("localhost", 6789)

    val clickLog = lines.map(x => (x.split(",")(1), x)).transform(rdd => { // 将传入的数据转化为一定格式
      rdd.leftOuterJoin(blacksRDDMap) // 执行左连接操作
        .filter(x => x._2._2.getOrElse(false) != true) // 过滤结果得到最终黑名单过滤以后的
        .map(x => x._2._1) // 输出最终想要的结果
    })

    clickLog.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
