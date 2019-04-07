package com.companyname

import com.mongodb.spark.MongoSpark
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._

object sparkscalakafkastreaming {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection").getOrCreate();
    val sc=spark.sparkContext
    val ssc=new StreamingContext(sc,Seconds(10))
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topicName1 = List("employee-topic").toSet
    val stream1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicName1)
    val lines1 = stream1.map(_._2)
    lines1.foreachRDD {
      rdd => {
        val employeeDataFrame = spark.read.json(rdd)
        employeeDataFrame.show()
        MongoSpark.save(employeeDataFrame.write
          .option("collection","employee")
          .mode("overwrite"))

      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
