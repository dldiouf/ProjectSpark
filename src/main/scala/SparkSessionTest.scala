import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.current_date
import sys.process._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,lit,sqrt,lpad}
object SparkSessionTest {
    def main(args: Array[String]): Unit = {
      //val sc = new SparkContext("local[*]","SparkSessionTest")
      val sparkSession = SparkSession.
        builder.master("local[*]").
        config("spark.ui.port", "0").
        config("spark.sql.warehouse.dir", "/user/djibril/warehouse").
        enableHiveSupport().
        appName("SparkSessionTest").
        getOrCreate()
      val username = System.getProperty("user.name")
      val data = sparkSession.
        read.
        csv("/Users/djibril/Desktop/BigData/data-master/retail_db/orders/part-00000")
        println(data.select(current_date().alias("Date en cours")).show)
  }
}
