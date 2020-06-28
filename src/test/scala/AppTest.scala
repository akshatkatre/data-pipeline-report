package dsti

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._

class AppTest extends FunSuite with SharedSparkContext {
  test("test initializing spark context") {
    val list: List[Int] = List(1, 2, 3, 4)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
  }
}