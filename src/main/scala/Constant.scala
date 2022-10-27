  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.spark_project.jetty.util.security.Password

  import java.text.SimpleDateFormat
  import java.time.format.DateTimeFormatter
  import java.time.temporal.TemporalAdjusters
  import java.time.{Period, ZoneId, ZonedDateTime}
  import java.util.Date

  object Constant {
    def initSpark(): SparkSession ={
      SparkSession.builder().
        appName("Coding Game").
        config("spark.ui.port","0").
        enableHiveSupport().
        master("local[*]").
        getOrCreate()
    }

    def initSparkStreaming() : Unit = {
      val spark = initSpark()
      val initSpSt = (spark
          .readStream
          .format("rate")
          .option("rowsPerSecond", 1)
          .load()
      )
      println(initSpSt.isStreaming)
    }

    def getAllData() : DataFrame = {
      val spark = Constant.initSpark()
      val dataTitanic = spark.read
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .option("header", "true")
        .csv("/Users/djibril/Downloads/datasets/titanic.csv")
      dataTitanic
    }

    def connectDBPostgres(table : String, user : String, password :String) : DataFrame = {
      initSpark().read.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/titanic")
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .load()
    }

    def writeDataForPostgres(table : String, user : String, password :String) : Unit = {
      getAllData
        .write
        .mode("append")
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/titanic")
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .save()
    }

    def addDateToQuery(startDate: String, endDate: String): String = {
      val format = new SimpleDateFormat("yyyy-MM-dd")
      var starD = format.parse(startDate)
      val endD = format.parse(endDate)
      val endNextD = new SimpleDateFormat("yyyy-MM-dd").format(new Date(endD.getTime + (1000 * 60 * 60 * 24)))
      val endNextDToDate = format.parse(endNextD)
      var partition_condition: String =  "("
      while (starD.before(endNextDToDate)) {
        val year: String = new SimpleDateFormat("yyyy").format(starD)
        val month: String = new SimpleDateFormat("MM").format(starD)
        val day: String = new SimpleDateFormat("yyyyMMdd").format(starD)
        partition_condition += "(" + "year='" + year + "' AND " + "month='" + month + "' AND " + "day='" + day + "')"
        starD = new Date(starD.getTime + (1000 * 60 * 60 * 24))
        if (starD.before(endNextDToDate)) partition_condition += " OR "
        else partition_condition += ")"
      }
      partition_condition
    }

    def getDateIntervalle(startDate : ZonedDateTime, endDate : ZonedDateTime, period: String) : String = {
      var sDate = startDate
      var eDate = endDate.plus(Period.parse("P1D"))
      var condition: String = "("
      if (period.equalsIgnoreCase("Daily") || period.equalsIgnoreCase("Weekly"))
        eDate = eDate
      else {
        if (endDate.getDayOfMonth != endDate.toLocalDate.`with`(TemporalAdjusters.lastDayOfMonth()))
          eDate = endDate.toLocalDate.`with`(TemporalAdjusters.lastDayOfMonth()).atStartOfDay(ZoneId.of("America/New_York"))
        else
          eDate = eDate
      }
      while (sDate.isBefore(eDate)){
        val year: String = DateTimeFormatter.ofPattern("yyyy").format(sDate)
        val month: String = DateTimeFormatter.ofPattern("MM").format(sDate)
        val day: String = DateTimeFormatter.ofPattern("yyyyMMdd").format(sDate)
        condition += "(year="+year+" AND month="+month+" AND day="+day+")"
        sDate = sDate.plus(Period.parse(("P1D")))
        if (sDate.isBefore(eDate)) condition += " OR "
        else condition += ")"
      }
      condition
    }


  }
