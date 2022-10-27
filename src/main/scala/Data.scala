import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.joda.time.Period

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

object Data {
  def getData(path : String, schema : StructType, delimiter : String, header : String, inferSchema : String) : DataFrame = {
    Constant.initSpark().read
      .schema(schema)
      .option("inferSchema", inferSchema)
      .option("delimiter", delimiter)
      .option("header", header)
      .csv(path)
  }
  // return ((yyyy && MM && yyyyMMdd) || (yyyy && MM && yyyyMMdd))
  // 2022-10-01 ------ 2022-10-31
  def getDateInterval(beginDate : ZonedDateTime, endDate : ZonedDateTime) : Unit = {
    var year, month, day = ""
    var dayIncremented = beginDate
    while (beginDate.isBefore(endDate)){
      year = DateTimeFormatter.ofPattern("yyyy").format(dayIncremented)
      month = DateTimeFormatter.ofPattern("MM").format(dayIncremented)
      day = DateTimeFormatter.ofPattern("yyyyMMdd").format(dayIncremented)
    }
  }

  def getDataOrder(beginDate : ZonedDateTime, endDate : ZonedDateTime, orderPath : String) : DataFrame = {
    val schema = StructType(
      Array(
        StructField("orderId", IntegerType, true),
        StructField("orderDate", TimestampType, true),
        StructField("orderCustomerDd", IntegerType, true),
        StructField("orderStatus", StringType, true)
      )
    )
    return getData(orderPath, schema, ",", "false", "false")
  }

  def getDataOrderItem(beginDate : ZonedDateTime, endDate : ZonedDateTime, orderItemPath : String) : DataFrame = {
    val schema = StructType(
      Array(
        StructField("itemId", IntegerType, true),
        StructField("orderItemId", IntegerType, true),
        StructField("orderItemQty", IntegerType, true),
        StructField("productId", IntegerType, true),
        StructField("amount", DoubleType, true),
        StructField("amountTotal", DoubleType, true)
    ))
    return getData(orderItemPath, schema, ",", "false", "false")
  }

  def getDataProduct(beginDate : ZonedDateTime, endDate : ZonedDateTime, productPath : String) : DataFrame = {
    val schema = StructType(
      Array(
        StructField("itemId", IntegerType, true),
        StructField("orderItemId", IntegerType, true),
        StructField("orderItemQty", IntegerType, true),
        StructField("productId", IntegerType, true),
        StructField("amount", DoubleType, true),
        StructField("amountTotal", DoubleType, true)
      ))
    return getData(productPath, schema, ",", "false", "false")
  }

}
