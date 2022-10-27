import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter


object CodingGame {

  def dateIntervalle(beginDate : ZonedDateTime, endDate : ZonedDateTime) : Unit = {

  }

  def main(args: Array[String]): Unit = {
    //println(Constant.connectDBPostgres("titanic", "postgres", "djibson93").show(numRows = 200, truncate = false))
    //Constant.writeDataForPostgres("titanic", "postgres", "djibson93")
    //val data = Constant.connectDBPostgres("titanic", "postgres", "djibson93")
    //println(Constant.getAllData().show(50, false))
    //println(Constant.getDataByKey(data, "Sex").show(false))
    /*
    val dataF = Constant.getAllData()
    val dFR = dataF.repartition(4)
    val dFC = dataF.coalesce(1)
    dFC.persist(StorageLevel.MEMORY_AND_DISK_2)
    println("Repartition : "+dFR.rdd.partitions.length)
    println("Coalesce : "+dFC.rdd.partitions.length)

    */
    val beginDateSt = "2022-10-01".concat("T00:00Z")
    val endDateSt = "2022-10-03".concat("T00:00Z")
    val beginDate = ZonedDateTime.parse(beginDateSt, DateTimeFormatter.ISO_DATE_TIME)
    val endDate = ZonedDateTime.parse(endDateSt, DateTimeFormatter.ISO_DATE_TIME)
    println(Constant.getDateIntervalle(beginDate, endDate, "Monthly"))
  }

}
