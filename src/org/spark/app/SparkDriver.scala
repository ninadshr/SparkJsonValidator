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

object SparkDriver {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkSampleUpdate")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new HiveContext(sc)
//    val prop = new Properties();
//    val in = getClass().getResourceAsStream("application.properties")
    
    sqlContext.setConf("hive.metastore.warehouse.dir", "/Users/ninad/tmp/hive/warehouse")

//    prop.load(in);
//    in.close();
//    val props: scala.collection.Map[String, String] = prop
//
//    //create partitioned table with timestamp as one partition
//    val derivedTable: DataFrame = sqlContext.sql(props.get("query.first") + args(0))
//    derivedTable.write.mode(SaveMode.Append).saveAsTable(args(1))

    val words = new ArrayBuffer[String]()
    //read data from a file
    val lines = Source.fromFile("/Users/ninad/tmp/data.csv").getLines()

    while (lines.hasNext) {
      val line = lines.next().split(",")
      sqlContext.sql("insert into table sample_db.sample_partitioned partition (as_of_date = '" + System.currentTimeMillis() + "' ) Select  '" + line(0) + "', " + line(1) + ", '" + line(2) + "'")
    }
    //instead of append create another partition with current timestamp
    val data = sqlContext.sql("select count(*) as count from sample_db.sample_partitioned")
    
    println(data.collect()(0).get(0))
    //insert data into that table

    //give an option of insert override and create a function for insert override
  }

  def updateWithTimestamp() {

  }

  def insertOverride() {

  }
}