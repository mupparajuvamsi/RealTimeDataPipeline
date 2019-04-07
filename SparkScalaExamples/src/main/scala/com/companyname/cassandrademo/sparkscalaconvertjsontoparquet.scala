package com.companyname.cassandrademo

import org.apache.spark.sql.SparkSession

object sparkscalaconvertjsontoparquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sparkscalaconvertjsontoparquet")
      .getOrCreate()
    val employeeDataFrame = spark.read.format("json").option("header", "true").load("src/main/resources/test.json")
    employeeDataFrame.show(false)
    employeeDataFrame.printSchema()
    //employeeDataFrame.write.parquet("C:\\Users\\Tecmax\\Documents\\parquetavrotest")
    val employeeDataFrameParquet=spark.read.parquet("C:\\Users\\Tecmax\\Documents\\parquetavrotest")
    employeeDataFrameParquet.show(false)
    employeeDataFrameParquet.printSchema()
  }
}
