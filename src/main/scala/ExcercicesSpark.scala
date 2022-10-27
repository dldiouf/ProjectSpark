import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.internal._
object ExcercicesSpark {
  //spark.catalog.createExternalTable("")
  //spark.catalog.createTable("employees", schema=employeesSchema)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName("Excercices Spark").
      config("spark.ui.port","0").
      config("spark.sql.warehouse.dir", "/Users/djibril/warehouse").
      enableHiveSupport().
      master("local[*]").
      getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    val username = System.getProperty("user.name")
    spark.sql(s"create database if not exists ${username}_db")
    spark.catalog.setCurrentDatabase(s"${username}_db")
    val schemaOrderItem = StructType(Array(StructField("item_id", IntegerType, true),
      StructField("order_item_id", IntegerType, true), StructField("order_item_qty", IntegerType, true),
      StructField("product_id", IntegerType, true), StructField("amount", DoubleType, true),
      StructField("amount_total", DoubleType, true)
    ))
    val schemaOrder = StructType(Array(StructField("order_id", IntegerType, true),
      StructField("order_date", TimestampType, true),
      StructField("order_customer_id", IntegerType, true),
      StructField("order_status", StringType, true)
    ))
    val orders = spark.read.schema(schemaOrder).
      csv("/Users/djibril/Desktop/BigData/data-master/retail_db/orders")
    orders.createOrReplaceTempView("orders")
    val order_items = spark.read.schema(schemaOrderItem).
      csv("/Users/djibril/Desktop/BigData/data-master/retail_db/order_items")
    order_items.createOrReplaceTempView("order_items")

    // create tables

    //println(spark.sql("select * from orders").show(5, false))
    //println("******************************************************************************")
    //println(spark.sql("select * from order_items").show(5, true))
    //println(orders.
    //select("order_id", "order_date", "order_customer_id", "order_status").
    //filter("order_status like 'CLOSED' ").
    //show(5, false))

    //println(order_items.
    //  groupBy(col("product_id")).
    //  agg(count(col("product_id")).alias("Nombre")).
      //filter("order_status like 'CLOSED' or order_status like 'COMPLETE'").
      //where("order_id in (1, 2, 3, 4, 5, 6)").
      //orderBy(desc("order_id")).
    //  show(10, false)
    //println(orders.join(order_items, order_items("order_item_id") === orders("order_id")).
      //groupBy("order_status").
      //agg(sum("amount").alias("Montant total")).
    //  select(col("order_date"), col("order_status"), col("order_item_qty"), col("amount"))
    //  .show(false))
    //println(spark.catalog.listDatabases().show(false))
    //println(spark.sql("select * from orders").show(5, false))

    //println(spark.catalog.listDatabases().show(false))
    //println(spark.catalog.listTables().show(false))
    //println(spark.catalog.listColumns("orders").show(false))
  }
}
