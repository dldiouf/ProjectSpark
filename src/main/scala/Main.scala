import org.apache.spark.sql.functions.{broadcast, col, upper}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

object Main {
  def main(args: Array[String]): Unit = {
    val beginDateSt = "2022-10-01".concat("T00:00Z")
    val endDateSt = "2022-10-31".concat("T00:00Z")
    val beginDate = ZonedDateTime.parse(beginDateSt, DateTimeFormatter.ISO_DATE_TIME)
    val endDate = ZonedDateTime.parse(endDateSt, DateTimeFormatter.ISO_DATE_TIME)
    val orderPath = "/Users/djibril/Documents/RETAIL_DB_DATASET/retail_db/orders"
    val orderItemPath = "/Users/djibril/Documents/RETAIL_DB_DATASET/retail_db/order_items"
    val productPath = "/Users/djibril/Documents/RETAIL_DB_DATASET/retail_db/orders_items"
    val t0 = System.nanoTime()
    val order = Data.getDataOrder(beginDate, endDate, orderPath)
    val orderItem = Data.getDataOrderItem(beginDate, endDate, orderItemPath)
    println(
      (order.join(broadcast(orderItem), order.col("orderId") === orderItem.col("orderItemId"))
      .filter(
        upper(col("orderStatus"))
        .equalTo("PENDING_PAYMENT".toUpperCase())
      )
      ).show(200, false))

    //println(Data.getDateInterval(beginDate, endDate))

    val t1 = System.nanoTime()
    println("Durée d'execution : "+(t1 - t0)/10e8 + "s")
  }
}
