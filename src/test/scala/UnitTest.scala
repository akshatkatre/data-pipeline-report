import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest._
import com.dsti.report.CreateReport
import scala.reflect.io.Directory
import java.io.File

class UnitTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {
  def createSession: SparkSession =
    SparkSession.builder().master("local[*]").getOrCreate()

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
      Given("I have a test dataset and an output path")
      val csvPath = "src/test/resources/sample.log"
      val outputPath = "target/test/report"
      val expectedNumberOfGeneratedFiles: Int = 2

      When("I count the number of json files in the output path")
      val numberOfJsonFiles: Int = new java.io.File(outputPath).listFiles
        .filter(_.isFile)
        .toList
        .filter { file =>
          file.getName.endsWith("json")
        }
        .length

      Then("There should be a success file created in the output path")
      assert(numberOfJsonFiles == expectedNumberOfGeneratedFiles)
    }

    scenario("Check header count record within a file") {
      implicit val spark: SparkSession = createSession

      Given("I have a test dataset and an output path and reports created")
      val outputPath = "target/test/report"
      val validColumnsInReport =
        Array("date", "count", "ip", "uri", "date_range")
      val expectedCount = 20000
      val validDates = List("2017-09-10", "2018-06-19")

      When("I open the json files and read the count and date records")
      val filesInReportDirectory =
        new java.io.File(outputPath).listFiles.filter(_.isFile).toList.filter {
          file =>
            file.getName.endsWith("json")
        }
      val filesToBeValidated =
        filesInReportDirectory.map(x => outputPath + "//" + x.getName)

      Then(
        "The count of records should be greater than 20,000 and dates should be valid")
      filesToBeValidated.map(file => {
        println(file)
        val df = spark.read.option("multiline", "true").json(file)
        //Validate if the columns in the report are the same as expected
        assert(df.schema.names.diff(validColumnsInReport).isEmpty)
        //Validate if count record greater than 20000
        assert(
          df.select("count")
            .collectAsList
            .get(0)
            .toString
            .replace("[", "")
            .replace("]", "")
            .toLong > expectedCount)
        //validate if dates in report as expected
        assert(
          validDates.contains(
            df.select("date").head.toString.replace("[", "").replace("]", "")))
      })
    }
  }
}
