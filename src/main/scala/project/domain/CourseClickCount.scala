package project.domain

/**
  * @author YaboSun
  *
  * 实战课程点击数实体类
  */

/**
  * 实战课程点击数
  * @param day_course 对应HBase中的rowkety：20181111_1
  * @param click_count 对应20181111_1的访问总数
  */
case class CourseClickCount(day_course:String, click_count:Long)
