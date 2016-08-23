package org.spark.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.Properties
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions.propertiesAsScalaMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import scala.io.Source
import java.io.PrintWriter
import java.io.File

object DataCreator {
  //creates base unpartitioned table for some raw data creation --> sample_db.data_unpartitioned / 100000 rows
  //uses data in unpartitioned table to insert into partitioned table --> sample_db.data_partitioned
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkSampleUpdate")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new HiveContext(sc)

    sqlContext.setConf("hive.metastore.warehouse.dir", "/Users/ninad/tmp/hive/warehouse")

    sqlContext.sql("create database if not exists sample_db")
    sqlContext.sql("drop table if exists sample_db.data_partitioned")
    sqlContext.sql("drop table if exists sample_db.data_unpartitioned")
    sqlContext.sql("create external table sample_db.data_partitioned (name string, age int, gender string, address string, phone_num bigint) partitioned by"
      + "(as_of_date string) STORED AS PARQUET location '/Users/ninad/tmp/hive/warehouse/sample_partitioned'")
    sqlContext.sql("create external table sample_db.data_unpartitioned (name string, age int, gender string, address string, phone_num bigint)"
      + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location '/Users/ninad/tmp/output' "
      + "")

    val values = Array.ofDim[String](1000, 5)
    val long = System.currentTimeMillis();
    for (i <- 0 until 1000) {
      values(i)(0) = "john" + i
      values(i)(1) = "28"
      values(i)(2) = "male"
      values(i)(3) = "123 random street"
      values(i)(4) = "1234567"
    }

    val dir = new File("/Users/ninad/tmp/output/");
    for (d <- subdirs(dir)) {
      d.delete();
    }
    for (j <- 1 to 100) {
      val file = new PrintWriter("/Users/ninad/tmp/output/data" + j + ".csv")
      for (i <- 0 until 1000) {
        val str = values(i)(0) + "," + values(i)(1) + "," + values(i)(2) + "," + values(i)(3) + "," + values(i)(4)
        file.println(str)
      }
      file.close()
    }

    val data = sqlContext.sql("select count(*) as count from sample_db.data_unpartitioned")
    println("Unpartitioned data size : " + data.collect()(0).get(0))

    sqlContext.sql("insert into table sample_db.data_partitioned partition (as_of_date = '" + long + "' ) select name, age, gender, address, phone_num from sample_db.data_unpartitioned")

    val data2 = sqlContext.sql("select count(*) as count from sample_db.data_partitioned")
    println("Partitioned data size : " + data2.collect()(0).get(0))
  }

  def subdirs(dir: File): Iterator[File] = {
    val children = dir.listFiles.filter(_.isDirectory)
    children.toIterator ++ children.toIterator.flatMap(subdirs _)
  }

}