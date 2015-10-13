package ua.softserve.spark.integration

import com.github.tlrx.elasticsearch.test.EsSetup
import com.github.tlrx.elasticsearch.test.EsSetup._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Created by dbort on 09.10.2015.
 */
class ItgSuite extends FunSuite with BeforeAndAfterAll {

//  val sparkConf = new SparkConf().setAppName("testContext").setMaster("local")
//  lazy val sc = new StreamingContext(sparkConf, Seconds(1))

  val initialJson = """{"name": "Silvesters's Italian restaurant", "description": "Great food, great atmosphere!", "address": { "street": "46 W 46th street", "city": "San Hose", "state": "CA", "zip": "10036" }, "location": [40.75, -73.97], "tags": ["italian", "spagetti", "pasta"], "rating": "3.5" }"""

  val esSetup = new EsSetup();
  //esSetup.execute(deleteAll(), createIndex("places").withMapping("restaurant", initialJson))


//  def path(file: String) = getClass.getResource("/" + file).getFile

  test("consumeAndPutJsonDataSuccessfully") {
    esSetup.execute(index("places", "type1", "1").withSource("""{"field1" : "value1" }"""));
    esSetup.execute().
    println(esSetup.countAll())
    esSetup.execute(deleteAll())
//    val output = sc.combineTextFiles(path("testinput"), 128, 128, "\n", "true").collect.sorted
//    assert(output.deep == Array("1", "2", "3", "4").deep)
  }

  override def afterAll() {
    esSetup.terminate()
  }

}
