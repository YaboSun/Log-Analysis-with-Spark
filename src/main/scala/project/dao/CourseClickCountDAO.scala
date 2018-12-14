package project.dao

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import project.domain.CourseClickCount
import project.utils.HBaseUtils

import scala.collection.mutable.ListBuffer

/**
  * @author YaboSun
  *
  * 实战课程点击数实体类数据访问层
  */
object CourseClickCountDAO {

  // 定义表名
  val table_name = "course_clickcount"
  // 定义列族 column family
  val cf = "log_info"
  // 定义列名
  val qualifier = "click_count"

  /**
    * 保存数据到HBase
    * @param list CourseClickCount集合
    */
  def save(list: ListBuffer[CourseClickCount]): Unit ={
    val table = HBaseUtils.getInstance().getTable(table_name)

    for(ele <- list) {
      // 这个函数可以直接将已有的直接相加
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifier),
        ele.click_count)
    }
  }

  /**
    * 根据rowkey查询值
    * @param day_course
    * @return
    */
  def count(day_course:String): Long = {
    val table = HBaseUtils.getInstance().getTable(table_name)

    val get = new Get(Bytes.toBytes(day_course))

    val value = table.get(get).getValue(cf.getBytes(), qualifier.getBytes())

    // scala中==与java不同
    if(value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20181111_8", 8))
    list.append(CourseClickCount("20181111_9", 9))
    list.append(CourseClickCount("20181111_1", 100))

    save(list)
    print(count("20181111_8") + count("20181111_9") + count("20181111_1"))
  }
}
