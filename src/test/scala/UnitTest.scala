import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}
import org.scalatest._
import com.dsti.report.CreateReport

import scala.reflect.io.Directory
import java.io.File

import scala.util.parsing.json.JSON

class UnitTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {
  def createSession: SparkSession =
    SparkSession.builder().master("local[*]").getOrCreate()

  // sample implementation only

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
      When("When I create a report in output path")
      createReport(csvPath, outputPath)
      Then("There should be a success file created in the output path")
      assert(new java.io.File(outputPath + "//_SUCCESS").exists == true)
    }
    scenario("Validate file count") {
      implicit val spark: SparkSession = createSession
      Given("I have a test dataset and an output path")
      val csvPath = "src/test/resources/sample.log"
      val outputPath = "target/test/report"
      val expectedNumberOfGeneratedFiles: Int = 2
      When("When I count the number of json files in the output path")
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
      Given("I have a test dataset and an output path")
      val outputPath = "target/test/report"
      val expectedDateCount = Map("2017-09-10" -> 57032, "2018-06-19" -> 47064)

      When(
        "When I open the first json file and read the count record for the date")
      val numberOfJsonFiles: Int = new java.io.File(outputPath).listFiles
        .filter(_.isFile)
        .toList
        .filter { file =>
          file.getName.endsWith("json")
        }
        .length
      val fname = new java.io.File(outputPath).listFiles
        .filter(_.isFile)
        .toList
        .filter { file =>
          file.getName.endsWith("json")
        }(0)
        .getName

      val input_file = outputPath + "//" + fname

      val json_content = scala.io.Source.fromFile(input_file).mkString

      val json_data =
        JSON.parseFull(json_content).get.asInstanceOf[Map[String, Any]]
      Then("The count record should match the Map objects count record")
      assert(
        json_data("count").toString
          .replace(".0", "")
          .toInt == expectedDateCount(json_data("date").toString))
    }
  }
}
