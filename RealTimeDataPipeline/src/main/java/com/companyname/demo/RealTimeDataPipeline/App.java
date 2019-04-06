package com.companyname.demo.RealTimeDataPipeline;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.mongodb.spark.MongoSpark;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws InterruptedException {
		// System.out.println( "Hello World!" );
		SparkSession sparkSession = SparkSession.builder().master("local")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection").getOrCreate();
		SparkContext sparkContext = sparkSession.sparkContext();
		JavaSparkContext sc = new JavaSparkContext(sparkContext);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(180000));
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		// kafkaParams.put("key.deserializer", StringDeserializer.class);
		// kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "employeeconsumergroup");
		kafkaParams.put("auto.offset.reset", "smallest"); // from-beginning?
		// kafkaParams.put("enable.auto.commit", false);

		Set<String> topics = Collections.singleton("employee-topic");
		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		JavaDStream<String> json = directKafkaStream.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> message) throws Exception {
				System.out.println(message._2());
				return message._2();
			};
		});
		json.foreachRDD(rdd -> {
			rdd.foreach(record -> System.out.println(record));
		});
		json.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			@Override
			public void call(JavaRDD<String> rdd) {
				if (!rdd.isEmpty()) {
					Dataset<Row> data = sparkSession.read().json(rdd);
					data.printSchema();
					data.show(false);
					/*
					 * //DF in table Dataset<Row> df = data.select(
					 * org.apache.spark.sql.functions.explode(org.apache.spark.sql.functions.col(
					 * "sensors"))).toDF("sensors").select("sensors.s","sensors.d"); df.show(false);
					 */
					MongoSpark.save(data.write().option("collection", "myCollection").mode("append"));
				}
			}
		});
		ssc.start();
		ssc.awaitTermination();
	}
}
