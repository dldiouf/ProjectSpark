import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SPARK_VERSION}
import org.apache.spark.io
object Test {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]","Test")
    val orderItems = sc.textFile("/Users/djibril/Desktop/BigData/data-master/retail_db/order_items/part-00000")
    val orderItemsFilter = orderItems.filter(orderItems => orderItems.split(",")(1).toInt == 2)
    val orderItemsMap = orderItemsFilter.map(orderItemsFilter => orderItemsFilter.split(",")(4).toFloat)
    val orderItemsReduce = orderItemsMap.reduce((total, subtotal) => total+subtotal).toFloat
    println(orderItemsReduce)

    val src = sc.textFile("/Users/djibril/Desktop/BigData/SparkLL/fichiers_d_exercice_essentiel_apache_spark/Chapitre_01/table_ronde/*.txt")
    val src1 = src.distinct()
    val datajoin = src
    val srcMap = src.map(l => l.length).reduce((a,b) => a+b)
    val srcMapO = src.map(_.length).reduce(_ + _)

    // Declaration d'une constante globale
    val number = sc.broadcast(390)

    // Declaration d'un accumulateur (Modifiable)
    val acc = sc.longAccumulator("nombre")
    acc.value
    src1.persist()
    print(src.count())
    print(src1.count())


  }
}
