package nyctaxi

import java.time.format.DateTimeFormatter
import java.util
import java.util.{Calendar, Date}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.insightedge.spark.context.InsightEdgeConfig
import org.insightedge.spark.implicits.all._
import org.openspaces.spatial.ShapeFactory

import scala.collection.JavaConversions._

/**
  * Created by evgeny on 7/5/17.
  */
object ReadData {

  val goldmanSacksLocation = new Point( -74.013961, 40.714672 )
  val dateFormat = new java.text.SimpleDateFormat("dd-MM-yyyy")
  val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")
  val locationPrecision = 0.001 // about 100 meters
  val brooklynPoligom = ShapeFactory.polygon(
          ShapeFactory.point(-74.0418951311803,40.5695299944867),
          ShapeFactory.point(-74.0418951311803,40.7391279531343),
          ShapeFactory.point(-73.8330410090778,40.7391279531343),
          ShapeFactory.point(-73.8330410090778,40.5695299944867),
          ShapeFactory.point(-74.0418951311803,40.5695299944867))

  val c = Calendar.getInstance()

  //first parameter is link to csv file
  def main(args: Array[String]): Unit = {

    val settings = if (args.length > 0) args else Array("spark://127.0.0.1:7077", "insightedge-space", "insightedge", "127.0.0.1:4174")
    if (settings.length < 4) {
      System.err.println("Usage: LoadDataFrame <spark master url> <space name> <space groups> <space locator>, first parameter must be url to csv file")
      System.exit(1)
    }
    val Array(master, space, groups, locators) = settings.slice( 0, 4 )
    val ieConfig = InsightEdgeConfig(space, Some(groups), Some(locators))

    val conf = new SparkConf().setMaster(master).setAppName("NYC taxi - reader")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Taxi")
      .insightEdgeConfig(ieConfig)
      .getOrCreate()

    val startTime = System.currentTimeMillis()

    /*TODO:
    // 1. query that will bring us areas with maximum passenger_count
    // 2. query for finding few hours with maximum dropoffs
    // 3.
    */

    val taxiTripDataDf = sparkSession.read.grid[TaxiTripData]

    //////////=========================pickups taken from Brooklyn==========================
//    val pickupsFromBrooklyn = taxiTripDataDf.filter(taxiTripDataDf("pickupLocation") geoWithin brooklynPoligom)
//    val pickupsFromBrooklynCount = pickupsFromBrooklyn.count()
    //////////===============================================================================


    //////////~~~~~~~~~~~~~~~~~~~~pickups taken from Brooklyn at 3 busy rush hours~~~~~~~~~
    val dropOffHoursWithMaxCounts = retrieveMostRushHours(taxiTripDataDf, 3)
    val pickupsFromBrooklynAtRushHour = taxiTripDataDf.
            filter(taxiTripDataDf("pickupLocation") geoWithin brooklynPoligom).
            filter(taxiTripDataDf("dropoffHour").isin( dropOffHoursWithMaxCounts:_* ))
    //////////~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


    //////////--------------------Busiest Neighbourhoods----------------
//    val results=findBusiestNeighbourhoods(taxiTripDataDf)
    ////////////////////////////
    val endTime = System.currentTimeMillis()

    sparkSession.stopInsightEdgeContext()

    println( "DataFrame load took " + ( endTime - startTime ) + " msec." )
  }

  def retrieveMostRushHours( taxiTripDataDf : DataFrame, rushHoursCount: Integer ): util.ArrayList[Integer] ={
    val l = taxiTripDataDf.select("dropoffHour").groupBy( "dropoffHour" ).count().sort( "count" ).collectAsList()

    //take list of [rushHoursCount] Row objects with maximum count ( most rush hours )
    val maxCountRows = l.subList( l.size() - rushHoursCount, l.size() )

    var dropOffHoursWithMaxCounts = new util.ArrayList[Integer]()

    for(x <- maxCountRows) dropOffHoursWithMaxCounts.add( x.getAs[Integer]("dropoffHour") )

    println( "maxCountRows :" + maxCountRows )
    println( "dropOffHoursWithMaxCounts :" + dropOffHoursWithMaxCounts )

    dropOffHoursWithMaxCounts
  }

  def findBusiestNeighbourhoods(taxiTripDataDf : DataFrame): scala.collection.immutable.SortedMap[Long,String]={
    val map = LoadNeighbourhoodData.getManhattanNeighbourhoods
    val results =  scala.collection.immutable.SortedMap[Long,String]()
    var count:Long =0
   for( neighbourhood <- map.values ){
     for(shape <- neighbourhood.shape){
       count += taxiTripDataDf.filter(taxiTripDataDf("pickupLocation") geoWithin shape ).count()
     }

     results+(count->neighbourhood.properties.getProperty("NTAName"))
     count=0
   }

    println(results(1))
    println(results(2))
    println(results(3))

    return results

  }


  //TODO
  //1. compare foot print of spark with space
  //2. decrease number of selects in order to receive most busy rush hour
  //3. consider not to create new additional field in our TaxiTripData class for hour
}