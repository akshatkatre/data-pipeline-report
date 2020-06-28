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
//case class Report(date: String, accessCount: Long)

class MyTest extends FunSuite with SharedSparkContext {

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

  test("Success File Generated") {

    implicit val spark: SparkSession = createSession

    // Given I have a test dataset and an output path
    val csvPath = "src/test/resources/sample.log"
    val outputPath = "target/test/report"

    // When I create a report in output path
    createReport(csvPath, outputPath)

    // Then the report have more than 0 records
    assert(new java.io.File(outputPath + "\\_SUCCESS").exists == true)

  }

  test("File count") {

    implicit val spark: SparkSession = createSession

    // Given I have a test dataset and an output path
    val outputPath = "target/test/report"
    val expectedNumberOfGeneratedFiles: Int = 2
    // When I create a report in output path
    //createReport(csvPath, outputPath)

    // Then the report have more than 0 records
    assert(
      new java.io.File(outputPath).listFiles
        .filter(_.isFile)
        .toList
        .filter { file =>
          file.getName.endsWith("json")
        }
        .length == expectedNumberOfGeneratedFiles)

  }

  test("Validate count") {
    implicit val spark: SparkSession = createSession

    // Given I have a test dataset and an output path
    val outputPath = "target/test/report"
    val expectedDateCount = Map("2017-09-10" -> 57032, "2018-06-19" -> 47064)
    val fname = new java.io.File(outputPath).listFiles
      .filter(_.isFile)
      .toList
      .filter { file =>
        file.getName.endsWith("json")
      }(0)
      .getName

    val input_file = outputPath + "\\" + fname

    val json_content = scala.io.Source.fromFile(input_file).mkString

    val json_data =
      JSON.parseFull(json_content).get.asInstanceOf[Map[String, Any]]

    assert(
      json_data("count").toString.replace(".0", "").toInt == expectedDateCount(
        json_data("date").toString))
  }
}
