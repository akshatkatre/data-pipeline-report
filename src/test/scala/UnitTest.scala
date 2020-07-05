import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest._
import com.dsti.report.CreateReport
import scala.reflect.io.Directory
import java.io.File

import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

class UnitTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {
  def createSession: SparkSession =
    SparkSession.builder().master("local[*]").getOrCreate()

  /*
  Method: createReport
      Delete the report export directory
      Invoke the method CreateReport.generateReport()
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
  def createReport(
    logPath: String,
    outputPath: String)(implicit spark: SparkSession): Unit = {
    val directory = new Directory(new File(outputPath))
    directory.deleteRecursively()
    import spark.implicits._
    CreateReport.generateReport(logPath, outputPath, spark)
  }

  info("As business user")
  info("I want to be able to validate the web traffic report")

  feature("web traffic report") {
    scenario("Validate success file exists") {
      implicit val spark: SparkSession = createSession

      Given("I have a test dataset and an output path")
      val csvPath = "src/test/resources/sample.log"
      val outputPath = "target/test/report"

      When("I create a report in output path")
      createReport(csvPath, outputPath)

      Then("There should be a success file created in the output path")
      assert(new java.io.File(outputPath + "//_SUCCESS").exists)
    }

    scenario("Validate file count") {
      implicit val spark: SparkSession = createSession
      Given("I have a test dataset and a success file created")
      val csvPath = "src/test/resources/sample.log"
      val outputPath = "target/test/report"
      val expectedNumberOfGeneratedFiles: Int = 1

      When("I count the number of json files in the output path")
      val numberOfJsonFiles: Int = new java.io.File(outputPath).listFiles
        .filter(_.isFile)
        .toList
        .filter { file =>
          file.getName.endsWith("json")
        }
        .length

      Then("There should one json file in the output path")
      assert(numberOfJsonFiles == expectedNumberOfGeneratedFiles)
    }

    scenario("Validate file contents") {
      implicit val spark: SparkSession = createSession
      Given("I have a test dataset and an output path a json file created")
      val outputPath = "target/test/report"
      val validColumnsInReport =
        Array("date", "count", "ip", "uri", "date_range")
      val expectedCount = 20000
      val validDates = List("2017-02-08", "2017-09-10")

      When("I open the json files and read the file contents")
      val filesInReportDirectory =
        new java.io.File(outputPath).listFiles.filter(_.isFile).toList.filter {
          file =>
            file.getName.endsWith("json")
        }
      val filesToBeValidated =
        filesInReportDirectory.map(x => outputPath + "//" + x.getName)

      Then(
        "The columns should match the expected column values. \n The count of records should be greater than 20,000 and dates should be valid.\n The date should be as per the expected list")
      filesToBeValidated.map(fileName => {
        val file = Source.fromFile(fileName)
        val lines = file.getLines.toList
        file.close
        val mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        lines.map(line => {
          val parsedJson = mapper.readValue[Map[String, Object]](line)
          import spark.implicits._
          val df = spark.read.json(Seq(line).toDS)
          //Validate if the columns in the report are the same as expected
          assert(df.schema.names.diff(validColumnsInReport).isEmpty)

          //Validate that the count in the file is greater than as expected
          assert(parsedJson("count").toString.toInt > expectedCount)

          //Validate the date in the file is as per the dateds in the expected list
          assert(validDates.contains(parsedJson("date").toString))
        })
      })
    }
  }
}
