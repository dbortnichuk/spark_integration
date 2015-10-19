package com.cisco.mantl.integration

import com.cisco.mantl.integration.ItgDriver._
import com.github.tlrx.elasticsearch.test.EsSetup
import com.github.tlrx.elasticsearch.test.EsSetup._
import kafka.producer.KeyedMessage
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Created by dbort on 09.10.2015.
 */
class ItgSuite extends FunSuite with BeforeAndAfterAll {

  val kafkaUnitServer: KafkaUnit = new KafkaUnit(5000, 5001)
  kafkaUnitServer.startup

  val sparkConf = new SparkConf().setAppName("testContext").setMaster("local[4]")
  val ssc = new StreamingContext(sparkConf, Seconds(1))

  val esHost = "localhost"
  val esPort = "9200"
  val esIndex = "someindex"
  val esType = "sometype"

  val initialJson = """{"field111" : "value111" }"""
  //val initialJson = """{"name": "Silvesters's Italian restaurant", "description": "Great food, great atmosphere!", "address": { "street": "46 W 46th street", "city": "San Hose", "state": "CA", "zip": "10036" }, "location": [40.75, -73.97], "tags": ["italian", "spagetti", "pasta"], "rating": "3.5" }"""

  val esSetup = new EsSetup();
  //esSetup.execute(deleteAll(), createIndex("places").withMapping("restaurant", initialJson))

  test("consumeAndPutJsonDataSuccessfully") {

    val testTopic = "TestTopic"
    kafkaUnitServer.createTopic(testTopic)
    val keyedMessage: KeyedMessage[String, String] = new KeyedMessage[String, String](testTopic, initialJson, initialJson)

    kafkaUnitServer.sendMessages(keyedMessage)

    //    val messages: util.List[String] = kafkaUnitServer.readMessages(testTopic, 1)
    //    messages.foreach(println)
    //    assert(util.Arrays.asList(initialJson) == messages)

    ssc.putJson(ssc.consume(kafkaUnitServer.getBrokerString, testTopic), esHost, esPort, esIndex, esType)

    //clean Elasticsearch indexes before starting Spark job
    esSetup.execute(deleteAll())

    ssc.start()
    //ssc.awaitTermination()
    Thread.sleep(10000)

    val esClient = esSetup.client()
    val response = esClient.prepareSearch(esIndex)
      .setQuery(QueryBuilders.matchAllQuery())
      .setPostFilter(FilterBuilders.termFilter("field111", "value111"))
      .execute()
      .actionGet();

    assert(response.getHits().totalHits() == 1);
    println("INDEX EXISTS: " + esSetup.exists("someindex"))
    //    println("RECORDS: " + esSetup.countAll())

    ssc.stop(true, true)

  }

  override def afterAll() {
    kafkaUnitServer.shutdown
    esSetup.terminate
  }

}
