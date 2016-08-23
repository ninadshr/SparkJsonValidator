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
import scala.collection.mutable.ArrayBuffer
import java.io.File
import java.io.PrintWriter

object SparkUpdateDriver {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkSampleUpdate")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: HiveContext = new HiveContext(sc)
    sqlContext.setConf("hive.metastore.warehouse.dir", "/Users/ninad/tmp/hive/warehouse")
    
     val timestamp = System.currentTimeMillis();
    
    //create small table used to add rows into base table --> sample_db.small_unpartitioned
    createSmallTable(sqlContext)
    
    //add rows from small table into base table. In real usecase the small table would be 
    //incoming addon data from ETL pipeline
    addFewRows(sqlContext, timestamp)
    
    //create small update. table used to add rows into base table --> sample_db.small_unpartitioned
    //In real usecase the small table would be updated/transformational data from ETL pipeline
    createSmallUpdateTable(sqlContext)
    
    //Use left join/overwrite logic to update data in base table
    updateFewRows(sqlContext,timestamp)

  }

  def addFewRows(sqlContext: HiveContext, timestamp: Long) = {
   
    createSmallTable(sqlContext);
    val maxVersionSql = "select max(as_of_date) from sample_db.data_partitioned"

    val maxVersion = sqlContext.sql(maxVersionSql);
    val sql = "insert into table sample_db.data_partitioned partition (as_of_date = '" + timestamp + "' )" +
      " select name, age, gender, address, phone_num from sample_db.small_unpartitioned union all " +
      " select name, age, gender, address, phone_num from sample_db.data_partitioned where as_of_date =" + maxVersion.collect()(0)(0)

    sqlContext.sql(sql)

    val data = sqlContext.sql("select count(*) as count from sample_db.sample_partitioned")
    println(data.collect()(0).get(0))

  }

  def updateFewRows(sqlContext: HiveContext, version: Long) = {
    val timestamp = System.currentTimeMillis();
    val sql = "insert overwrite table sample_db.data_partitioned partition (as_of_date = '" + timestamp + "' )" +
      " select coalesce(up.name,orig.name) as name, coalesce(up.age,orig.age) as age, coalesce(up.gender,orig.gender) as gender, coalesce(up.address,orig.address) as address, coalesce(up.phone_num,orig.phone_num) as phone_num from sample_db.update_unpartitioned as up left join " +
      " sample_db.data_partitioned orig on up.name = orig.name and up.as_of_date = " + version 
    sqlContext.sql(sql)

    val data = sqlContext.sql("select count(*) as count from sample_db.sample_partitioned")
    println(data.collect()(0).get(0))

  }

  def createSmallTable(sqlContext: HiveContext) = {

    sqlContext.sql("drop table if exists sample_db.small_unpartitioned")
    sqlContext.sql("create external table sample_db.small_unpartitioned (name string, age int, gender string, address string, phone_num bigint) partitioned by"
      + "(as_of_date string) STORED AS PARQUET location '/Users/ninad/tmp/output2'")

    val values = Array.ofDim[String](1000, 5)
    val long = System.currentTimeMillis();
    for (i <- 0 until 1000) {
      values(i)(0) = "john" + i
      values(i)(1) = "28"
      values(i)(2) = "male"
      values(i)(3) = "123 random street"
      values(i)(4) = "1234567"
    }
  }

  def createSmallUpdateTable(sqlContext: HiveContext) = {

    sqlContext.sql("drop table if exists sample_db.update_unpartitioned")
    sqlContext.sql("create external table sample_db.update_unpartitioned (name string, age int, gender string, address string, phone_num bigint) partitioned by"
      + "(as_of_date string) STORED AS PARQUET location '/Users/ninad/tmp/output2'")

    val values = Array.ofDim[String](1000, 5)
    val long = System.currentTimeMillis();
    for (i <- 0 until 1000) {
      values(i)(0) = "john" + i
      values(i)(1) = "28"
      values(i)(2) = "male"
      values(i)(3) = "123 update street"
      values(i)(4) = "1234567"
    }

    val dir = new File("/Users/ninad/tmp/output2");
    for (d <- subdirs(dir)) {
      d.delete();
    }
    val file = new PrintWriter("/Users/ninad/tmp/output2/data" + ".csv")
    for (i <- 0 until 1000) {
      val str = values(i)(0) + "," + values(i)(1) + "," + values(i)(2) + "," + values(i)(3) + "," + values(i)(4)
      file.println(str)
    }
    file.close()

    val data = sqlContext.sql("select count(*) as count from sample_db.small_unpartitioned")
    println("Small unpartitioned data size : " + data.collect()(0).get(0))

  }

  def subdirs(dir: File): Iterator[File] = {
    val children = dir.listFiles.filter(_.isDirectory)
    children.toIterator ++ children.toIterator.flatMap(subdirs _)
  }

}