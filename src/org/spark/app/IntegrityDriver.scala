package org.spark.app

import scala.util.parsing.json.JSON._
import org.spark.app.DIJSONParser.ParserException

class IntegrityDriver {

  def doDataIntegrityChecks(json:String):Either[ParserException,Specs] =  {

    case class TrailerCheckConfig(checkType: String, columnNumber: Int, metricValue: Long)
    case class ColumnCheckConfig(checkType: String, columnNumber: Int)

    val jsonSpec = parseFull("""{ "feedType": "Delimited",  "feedFormat": "Text",  "feedLocation": "/hdfs/usr/spark/", "hasHeader": "Y", "hasTrailer":"Y","headerDateStartIndex":2
                             , "trailerCheckConfig":[{"checkType" : "Count","columnNumber" : 1, "metricValue" : 40},{"checkType" : "sum","columnNumber" : 1, "metricValue" : 40}], 
                                "columnCheckConfig": [{"checkType" : "NOT NULL", "columnNumber" : 1},{"checkType" : "NOT NULL", "columnNumber" : 1}
                             ,{"checkType" : "DUP", "columnNumber" : 1},{"checkType" : "DUP", "columnNumber" : 2}], "delimiter": "\t" }""")

  DIJSONParser.fromJSON(json)

}
  
  def checkColumnSum(specs:Specs):Boolean = {
    true
  }
}