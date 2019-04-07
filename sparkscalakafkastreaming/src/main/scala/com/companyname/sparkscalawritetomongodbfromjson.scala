package com.companyname

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{SaveMode, SparkSession}

object sparkscalawritetomongodbfromjson {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection").getOrCreate();

    val employeeJsonDataFrame=spark.read.format("json").load("src/main/resources/employee.json");
    MongoSpark.save(employeeJsonDataFrame
        .write
        .option("collection","empjson")
        .mode(SaveMode.Append)
    )
  }
}
