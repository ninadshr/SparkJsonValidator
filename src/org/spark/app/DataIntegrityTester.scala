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
import org.apache.spark.rdd.RDD
import org.spark.app.DIJSONParser.ParserException

object DataIntegrityTester {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DataIntegrityTester")
    val sc: SparkContext = new SparkContext(conf)

    val textFile = sc.wholeTextFiles("/Users/ninad/tmp/output/")
    
    val json = """{ "feedType": "Delimited",  "feedFormat": "Text",  "feedLocation": "/hdfs/usr/spark/", "hasHeader": "Y", "hasTrailer":"Y","headerDateStartIndex":2
                             , "trailerCheckConfig":[{"checkType" : "Count","columnNumber" : 1, "metricValue" : 40},{"checkType" : "sum","columnNumber" : 1, "metricValue" : 40}], 
                                "columnCheckConfig": [{"checkType" : "NOT NULL", "columnNumber" : 1},{"checkType" : "NOT NULL", "columnNumber" : 1}
                             ,{"checkType" : "DUP", "columnNumber" : 1},{"checkType" : "DUP", "columnNumber" : 2}], "delimiter": "\t" }"""
    
    val diApi:IntegrityDriver = new IntegrityDriver 

    val eitherSpecs = diApi.doDataIntegrityChecks(json);
    if(eitherSpecs.isLeft){
      //Replace this with proper handling of tha code
      System.exit(0);
    }else {
      eitherSpecs.right.map { specs => validateFiles(specs) }      
    }
  }
  
  def validateFiles(specs:Specs) =  {
      specs.hasTrailer match {
        case "Y" => doTrailerBasedValidations() 
      }
  }
  
  
 def doTrailerBasedValidations():Unit = {
   
 }
  
   def checkRowCount(textFile: RDD[(String,String)]) = {
      val num = textFile.mapValues { x =>
        {
          val num = x.lines.length
          num
        }
      }
      println(num.collect()(0))
    }
}
 