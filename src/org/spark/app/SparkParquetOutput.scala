package org.spark.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.ParquetOutputFormat
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport, AvroReadSupport}
import parquet.hadoop.metadata.CompressionCodecName

object SparkParquetOutput {

  def main(args:Array[String]){
    
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkSampleUpdate")
    val sc: SparkContext = new SparkContext(conf)
        
     val schema:Schema = SchemaBuilder.record("sparkparquetoutput").fields()
                .requiredString("stringField")
                .requiredInt("intField")
                .requiredDouble("doubleField")
                .endRecord();
        
        val r1:GenericRecord = new GenericData.Record(schema);
        r1.put("stringField", "test");
        r1.put("intField", 1);
        r1.put("doubleField", -1.0);
        
        val records = sc.parallelize(ArrayBuffer(r1));
        val keyedRecords = records.keyBy { x => null }
        
        val job = Job.getInstance();
        
        ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport[String]]);
        AvroWriteSupport.setSchema(job.getConfiguration(), schema);
        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        
        keyedRecords.saveAsNewAPIHadoopFile("/your/output/directory",
                classOf[Void], classOf[GenericRecord],
                classOf[ParquetOutputFormat[String]], job.getConfiguration());
    }
}