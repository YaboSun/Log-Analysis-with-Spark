package project.dao

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import project.domain.{CourseClickCount, CourseSearchClickCount}
import project.utils.HBaseUtils

import scala.collection.mutable.ListBuffer

/**
  * @author YaboSun
  *
  * 从搜索引擎引流实战课程点击数实体类数据访问层
  */
object CourseSearchClickCountDAO {

  // 定义表名
  val table_name = "course_search_clickcount"
  // 定义列族 column family
  val cf = "log_info"
  // 定义列名
  val qualifier = "click_count"

  /**
    * 保存数据到HBase
    *
    * @param list CourseSearchClickCount集合
    */
  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {
    val table = HBaseUtils.getInstance().getTable(table_name)

    for (ele <- list) {
      // 这个函数可以直接将已有的直接相加
      table.incrementColumnValue(Bytes.toBytes(ele.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifier),
        ele.click_count)
    }
  }

  /**
    * 根据rowkey查询值
    *
    * @param day_search_course
    * @return
    */
  def count(day_search_course: String): Long = {
    val table = HBaseUtils.getInstance().getTable(table_name)

    val get = new Get(Bytes.toBytes(day_search_course))

    val value = table.get(get).getValue(cf.getBytes(), qualifier.getBytes())

    // scala中==与java不同
    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("20181111_www.baidu.com_8", 8))
    list.append(CourseSearchClickCount("20181111_cn.bing.com_9", 9))

    save(list)
    print(count("20181111_www.baidu.com_8") + count("20181111_cn.bing.com_9"))
  }
}
