package nyctaxi

import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.insightedge.spark.context.InsightEdgeConfig
import org.insightedge.spark.implicits.all._
import org.openspaces.spatial.ShapeFactory

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

  //first parameter is link to csv file
  def main(args: Array[String]): Unit = {

    val settings = if (args.length > 0) args else Array("spark://127.0.0.1:7077", "insightedge-space", "insightedge", "127.0.0.1:4174")
    if (settings.length < 4) {
      System.err.println("Usage: LoadDataFrame <spark master url> <space name> <space groups> <space locator>, first parameter must be url to csv file")
      System.exit(1)
    }
    val Array(master, space, groups, locators) = settings.slice( 0, 4 )
    val ieConfig = InsightEdgeConfig(space, Some(groups), Some(locators))

    val conf = new SparkConf().setMaster("local[2]").setAppName("NYC taxi")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Taxi")
      .insightEdgeConfig(ieConfig)
      .getOrCreate()

    val time1 = System.currentTimeMillis()

    val startTime = System.currentTimeMillis()

    val taxiTripData = sparkSession.read.grid[TaxiTripData]

    val pickupsFromBrooklyn = taxiTripData.filter(taxiTripData("pickupLocation") geoWithin brooklynPoligom)
    val pickupsFromBrooklynCount = pickupsFromBrooklyn.count()

    pickupsFromBrooklyn.printSchema()
    pickupsFromBrooklyn.show()

    println( ">> pickupsFromBrooklynCount=" + pickupsFromBrooklynCount )

    val endTime = System.currentTimeMillis()

    sparkSession.stopInsightEdgeContext()

    println( "DataFrame load took " + ( endTime - startTime ) + " msec." )

  }
}