package ua.softserve.spark.integration

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark


/**
 * Created by dbort on 09.10.2015.
 */
object ItgDriver {

  def main(args: Array[String]) {

    //turn on debug - $ export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005

    //Thread.sleep(5 * 1000)

    val parser = new scopt.OptionParser[Config](MsgUsage) {
      head("Spark Integration", "1.0")
      opt[String]('b', "brokers") required() valueName ("<brokers>") action { (x, c) =>
        c.copy(brokers = x)
      } text (MsgBrokers)
      opt[String]('t', "topics") required() valueName ("<topics>") action { (x, c) =>
        c.copy(topics = x)
      } text (MsgTopics)
      opt[String]('i', "index") required() valueName ("<esIndex>") action { (x, c) =>
        c.copy(esIndex = x)
      } text (MsgIndex)
      opt[String]('s', "type") required() valueName ("<esType>") action { (x, c) =>
        c.copy(esType = x)
      } text (MsgType)
      opt[String]('m', "master") valueName ("<masterURI>") action { (x, c) =>
        c.copy(master = x)
      } text (MsgMaster)
      opt[String]('n', "name") valueName ("<appName>") action { (x, c) =>
        c.copy(name = x)
      } text (MsgName)
      opt[Long]('c', "batchint") valueName ("<batchInterval>") action { (x, c) =>
        c.copy(batchInterval = x)
      } text (MsgBatchInterval)
      help("help") text (MsgHelp)
      note(MsgNote)
    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val sparkConf = new SparkConf()
        if(!config.name.isEmpty)sparkConf.setAppName(config.name)
        if(!config.master.isEmpty)sparkConf.setMaster(config.master)

        val ssc = new StreamingContext(sparkConf, Seconds(config.batchInterval))
        ssc.putJson(ssc.consume(config.brokers, config.topics), config.esIndex, config.esType)

        // Start the computation
        ssc.start()
        ssc.awaitTermination()
      case None => println("ERROR: bad argument set provided")
    }
  }

  implicit class IntegrationUtils(val origin: StreamingContext) {

    def consume(brokers: String, topics: String): InputDStream[(String, String)] = {
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](origin, kafkaParams, topicsSet)
    }

    def putJson(messages: InputDStream[(String, String)], esIndex: String, esType: String): Unit = {
      val lines = messages.map(_._2)
      lines.print()
      lines.foreachRDD(rdd => EsSpark.saveJsonToEs(rdd, esIndex + "/" + esType))
    }
  }

  case class Config(brokers: String = "",
                    topics: String = "",
                    esIndex: String = "",
                    esType: String = "",
                    master: String = "",
                    name: String = "Spark Integration",
                    batchInterval: Long = 1)

}
