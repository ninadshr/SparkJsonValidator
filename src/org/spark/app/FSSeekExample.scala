package org.spark.app

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.net.URI
import org.apache.hadoop.fs.Path
import scala.util.Properties

object FSSeekExample {
  def main(args: Array[String]) {

    val uri = URI.create (args(0));
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DataIntegrityTester")
    val sc: SparkContext = new SparkContext(conf)
    val fs = FileSystem.get(uri, sc.hadoopConfiguration)
    val in = fs.open(new Path(uri));
    val bye = new Array[Byte](1000)
    in.read(fs.getFileStatus(new Path(uri)).getLen - 1000,bye,0,1000)
    
    val str = new String(bye)
    
    println(str.substring(str.lastIndexOf(Properties.lineSeparator), str.length()-1))
    
  }
}