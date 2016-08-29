  package org.spark.app
  
  import scala.util.parsing.json.JSON._
  
  // Types we need
  case class TrailerCheckConfig(checkType: String, columnNumber: Double, metricValue:Double)
  case class ColumnCheckConfig(checkType: String, columnNumber: Double)
  case class Specs(feedType: String, feedFormat: String, feedLocation: String, 
      hasHeader: String, hasTrailer: String, headerDateStartIndex: Double, delimiter: String,
      trailerCheckConfig:List[TrailerCheckConfig],columnCheckConfig:List[ColumnCheckConfig])
  
  object DIJSONParser {
    // Extract your way through your instances
    class ClassCaster[T] {
      def unapply(a: Any): Option[T] = Some(a.asInstanceOf[T])
    }
  
    // Concrete instances of the casters for pattern matching
    object AsMap extends ClassCaster[Map[String, Any]]
    object AsList extends ClassCaster[List[Any]]
    object AsString extends ClassCaster[String]
    object AsBoolean extends ClassCaster[Boolean]
    object AsInt extends ClassCaster[Int]
    object AsDouble extends ClassCaster[Double]
  
    // Balloon your exceptions here. 
    case class ParserException(message: String) extends Exception(message)
  
    // Handle your return policy
    def fromJSON(json: String): Either[ParserException, Specs] = {
      // Push your message for contract in specs
      def error[T](prefix: String): Option[T] =
        throw ParserException("%s must be specified".format(prefix))
  
      // Catch the exceptions and turn them into Left() instances
      try {
        val parsed = parseFull(json)
        if (parsed.isEmpty) throw ParserException("Unable to parse JSON.")
  
        //Using monads to parse my JSON into usable classes. This is all you need for 
        // your conversion in real time.
        val contract = for {
          Some(AsMap(map)) <- List(parsed)
          AsString(feedType) <- map get "feedType" orElse error("feedType")
          AsString(feedFormat) <- map get "feedFormat" orElse error("feedFormat")
          AsString(feedLocation) <- map get "feedLocation" orElse error("feedLocation")
          AsString(hasHeader) <- map get "hasHeader" orElse error("hasHeader")
          AsString(hasTrailer) <- map get "hasTrailer" orElse error("hasTrailer")
          AsDouble(headerDateStartIndex) <- map get "headerDateStartIndex" orElse error("headerDateStartIndex")
          AsString(delimiter) <- map get "delimiter" orElse error("delimiter")
          AsList(trailerCheckConfig) <- map get "trailerCheckConfig"
          AsList(columnCheckConfig) <- map get "columnCheckConfig"
        } yield Specs(feedType, feedFormat, feedLocation, hasHeader, hasTrailer, headerDateStartIndex,
             delimiter, for {
          AsMap(trailerCheckConfigMap) <- trailerCheckConfig
          AsString(checkType) <- trailerCheckConfigMap get "checkType" orElse error("checkType")
          AsDouble(columnNumber) <- trailerCheckConfigMap get "columnNumber" orElse error("columnNumber")
          AsDouble(metricValue) <- trailerCheckConfigMap get "metricValue" orElse error("metricValue")
        } yield TrailerCheckConfig(checkType, columnNumber,metricValue),
        for {
          AsMap(columnCheckConfig) <- columnCheckConfig
          AsString(checkType) <- columnCheckConfig get "checkType" orElse error("checkType")
          AsDouble(checkColumn) <- columnCheckConfig get "columnNumber" orElse error("columnNumber")
        } yield ColumnCheckConfig(checkType, checkColumn))
        Right(contract.head)
      } catch {
        case e: ParserException =>
          Left(e)
        case e =>
          e.printStackTrace()
          Left(ParserException(e.getMessage))
      }
    }
  }
 /* object MyUtilities {
    def parseMyJson(s: String): Option[Any] = {
      parseFull("""{ "name": "Joe Doe",  "age": 45,  "kids": ["Frank", "Marta", "Joan"]}""")
    }
  }*/