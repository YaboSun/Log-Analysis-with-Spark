package example

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成统计
  * 并将结果写入到MySQL数据库中
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 如果使用了stateful的算子，必须要设置checkpoint
    // 在生产环境中，建议把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    // 将统计结果输出到控制台
    // state.print()

    //result.foreachRDD(rdd =>{
    //  val connection = createConnection()  // executed at the driver
    //  rdd.foreach { record =>
    //    val sql = "insert into wordcount(word, wordcount) values ('"+record._1 + "', " + record._2 +")"
    //    connection.createStatement().execute(sql)
    //  }
    //})

    result.print()
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount(word, wordcount) values ('"+record._1 + "', " + record._2 +")"

          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * get mysql connection
    * @return
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "root")
  }
}
