package nyctaxi

import java.time.format.DateTimeFormatter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.insightedge.spark.context.InsightEdgeConfig
import org.insightedge.spark.implicits.all._

/**
  * Created by evgeny on 7/5/17.
  */
object LoadData {
  //first parameter is link to csv file
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      System.err.println("At least one parameter with link to csv file should be passed")
      System.exit(1)
    }

    val settings = if (args.length > 1) args else Array("spark://127.0.0.1:7077", "insightedge-space", "insightedge", "127.0.0.1:4174", args(0))
    if (settings.length < 5) {
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
    val path = args(0)

    println( "Path:" + path )

    val startTime = System.currentTimeMillis()
    //    import sparkSession.implicits._
    val df = sparkSession.read.option("header","true")./*.option("inferSchema", "true").*/csv(path)/*.as[ TripData ]*/

    val goldmanSacksLocation = new Point( -74.013961, 40.714672 )
    val dateFormat = new java.text.SimpleDateFormat("dd-MM-yyyy")
    val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")
    val locationPrecision = 0.001 // about 100 meters


    //    val count = df.filter( o => o.passenger_count > 2 ).count()

    //val parsedDate = dateTimeFormat.parse( "2016-04-01 00:17:55" )


    //,
    //    df.filter( df( "Passenger_count" ) ==  6 )
    //    val count = df.filter( df("passenger_count") >= 5 ).count() //1017920
    val filteredDf = df.select( "VendorID",
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "passenger_count",
      "trip_distance",
      "pickup_longitude",
      "pickup_latitude" ).
      filter(
        df("dropoff_longitude") >= ( goldmanSacksLocation.longitude - locationPrecision ) &&
          df("dropoff_longitude") <= ( goldmanSacksLocation.longitude + locationPrecision ) &&
          df("dropoff_latitude") >= ( goldmanSacksLocation.latitude - locationPrecision ) &&
          df("dropoff_latitude") <= ( goldmanSacksLocation.latitude + locationPrecision )
        /*TODO add filtering according to working days only*/
        /*df( "tpep_dropoff_datetime" )......*/ )

    df.printSchema()
    filteredDf.show()

    //    conf.setInsightEdgeConfig(ieConfig)

    //consider using append in order not to Overwrite
    filteredDf.write.mode(SaveMode.Overwrite).grid("nytaxi")


    print( "Count:" + filteredDf.count() )

    //val tpep_pickup_datetime = df.collect().tail
    //.getAs("tpep_pickup_datetime")
    //
    //print( ">>> tpep_pickup_datetime=" + tpep_pickup_datetime )
    //    print( "datetime:" + dateTimeFormat.format( tpep_pickup_datetime ) )


    val endTime = System.currentTimeMillis()

    println( "DataFrame load took " + ( endTime - startTime ) + " msec." )

  }
}