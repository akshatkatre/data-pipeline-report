package com.dsti.report

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class AccessLog(
  ip: String,
  ident: String,
  user: String,
  datetime: String,
  request: String,
  status: String,
  size: String,
  referer: String,
  userAgent: String,
  unk: String)

object CreateReport {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CreateReport")
      .getOrCreate()

    val sc = spark.sparkContext
    generateReport(args(0), args(1), spark)
  }

  /*
    Method: generateReport
      Read a web server log file, convert to a data frame
      Identify all the dates that have greater than 20,000 hits for each of these dates
        Get count by IP for the date and store in data frame
        Get count by URI for the date and store in data frame
        Get count of traffic 10 days prior and 10 days after and store in data frame
      Combine the data frames into a single data frame.
      Write contents to a JSON file
    Parameters:
      inputFilePath : (String)
        path of log file that needs to be processed.
      reportExportPath : (String)
        path of report export directory.
      spark : (SparkSession)
        SparkSession object.
    Return Value:
      Unit
   */
  def generateReport(
    inputFilePath: String,
    reportExportPath: String,
    spark: SparkSession): Unit = {
    println("### Start Process ###")

    //read log file
    val logs = spark.read.text(inputFilePath)
    assert(logs.count > 0)

    import spark.implicits._
    val logAsString = logs.map(_.getString(0))

    AccessLog.apply _
    val R =
      """^(?<ip>[0-9.]+) (?<identd>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*) (?<size>[^ ]*) \"(?<referer>[^\"]*)\" \"(?<useragent>[^\"]*)\" \"(?<unk>[^\"]*)\"""".r

    //convert to data frame
    val dsParsed = logAsString.flatMap(x => R.unapplySeq(x))

    //Pattern match to create new DF with columns
    val ds = dsParsed.map(params => {
      AccessLog(
        params(0),
        params(1),
        params(2),
        params(3),
        params(4),
        params(5),
        params(6),
        params(7),
        params(8),
        params(9))
    })

    //create data frame with new datetime column
    val dsWithTime = ds.withColumn(
      "datetime",
      to_timestamp(ds("datetime"), "dd/MMM/yyyy:HH:mm:ss X"))

    //Split request column and split into method, uri and http
    val REQ_EX = "([^ ]+)[ ]+([^ ]+)[ ]+([^ ]+)".r

    val dsExtended = dsWithTime
      .withColumn(
        "method",
        regexp_extract(dsWithTime("request"), REQ_EX.toString, 1))
      .withColumn(
        "uri",
        regexp_extract(dsWithTime("request"), REQ_EX.toString, 2))
      .withColumn(
        "http",
        regexp_extract(dsWithTime("request"), REQ_EX.toString, 3))
      .drop("request")

    //cache dsExtended and create a Temp View
    dsExtended.cache
    dsExtended.createOrReplaceTempView("ExAccessLog")

    //create data frame with dates where number of records is greater than 20000
    val dsHighCount = spark.sql(
      "select cast(datetime as date) as date, count(*) as count from ExAccessLog group by date having count > 20000 order by count desc limit 10")

    //cache dsHighCount and create a Temp View
    dsHighCount.cache
    dsHighCount.createOrReplaceTempView("HighCountLog")

    println("### Processing Report ###")
    //for each date in data frame dsHighCount invoke the method returnReportRow
    //store the contents in an array
    val rep_array = dsHighCount
      .select("date")
      .collect
      .map(x => x.toString.slice(1, x.toString.length - 1))
      .map(dateStr => {
        // get total count of records for the input date
        val dt_ct = spark.sql(
          "select date, count  from HighCountLog where date = '" + dateStr + "'")

        // get count by ipaddress for the input date
        val ip_df = spark.sql(
          "select ip, count(*) as count  from ExAccessLog  where cast(datetime as date) = '" + dateStr + "' group by ip Order by 2 desc")

        // get count by uri for input date
        val ip_uri = spark.sql(
          "select uri, count(*) as count  from ExAccessLog  where cast(datetime as date) = '" + dateStr + "' group by uri Order by 2 desc")

        // get count by date range
        val date_range_df = spark.sql(
          "select cast(datetime as date) as date, count(*) as count from ExAccessLog where cast(datetime as date) between date_sub('" + dateStr + "',10) and date_add('" + dateStr + "',10) group by date order by date")

        //Create a consolidated data frame with contents of above 4 data frames.
        val ret_df = dt_ct
          .withColumn(
            "ip",
            typedLit(
              ip_df.collect
                .map(x => Map(x.get(0).toString -> x.get(1).toString))
                .toList))
          .withColumn(
            "uri",
            typedLit(
              ip_uri.collect
                .map(x => Map(x.get(0).toString -> x.get(1).toString))
                .toList))
          .withColumn(
            "date_range",
            typedLit(
              date_range_df.collect
                .map(x => Map(x.get(0).toString -> x.get(1).toString))
                .toList))

        ret_df
      })

    assert(rep_array.length > 0)

    //merge the array contents into a data frame
    val report_df = rep_array.reduceLeft(_.union(_))
    report_df.write.json(reportExportPath)
    //assert(report_df.count > 0)

    println("### Exporting report ###")

    //write the contents of the data frame to a json file

    println("### Export report complete ###")
    spark.stop()
  }
}
