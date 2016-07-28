package org.spark.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.Properties
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions.propertiesAsScalaMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object SparkDriver {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkSampleUpdate")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new HiveContext(sc)

    val prop = new Properties();
    val in = getClass().getResourceAsStream("application.properties");
    prop.load(in);
    in.close();
    val props: scala.collection.Map[String, String] = prop

    val derivedTable: DataFrame = sqlContext.sql(props.get("query.first") + args(0))
    derivedTable.write.mode(SaveMode.Append).saveAsTable(args(1))
  }
}