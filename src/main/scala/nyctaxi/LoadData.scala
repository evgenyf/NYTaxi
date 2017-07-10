package nyctaxi

import java.text.SimpleDateFormat

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.insightedge.spark.context.InsightEdgeConfig
import org.insightedge.spark.implicits.all._
import org.openspaces.spatial.ShapeFactory

/**
  * Created by evgeny on 7/5/17.
  */
object LoadData {

  val goldmanSacksLocation = new Point( -74.013961, 40.714672 )
  val dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val locationPrecision = 0.001 // about 100 meters

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


//    val parsedDate = dateTimeFormat.parse( "2016-04-07 16:39:04" )

    val startTime = System.currentTimeMillis()

    val df = sparkSession.read.option("header","true").csv(path)

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

    /*

    * */

    val taxiTripsRdd = filteredDf.rdd.map( row => new TaxiTripData(
      id = null,
      row.getAs[String]("VendorID").toInt,
      row.getAs[String]("tpep_pickup_datetime"),
      null,
      //      if( row.getAs[String]("tpep_pickup_datetime").trim.isEmpty ) null else dateTimeFormat.parse( row.getAs[String]("tpep_pickup_datetime") ),
      //if( row.getAs[String]("tpep_dropoff_datetime").trim.isEmpty ) null else dateTimeFormat.parse( row.getAs[String]("tpep_dropoff_datetime") ),
      //if( row.getAs[String]("tpep_dropoff_datetime").trim.isEmpty ) null else dateTimeFormat.parse( row.getAs[String]("tpep_dropoff_datetime") ),
      row.getAs[String]("tpep_dropoff_datetime"),
      null,
      row.getAs[String]("passenger_count").toInt,
      row.getAs[String]("trip_distance").toDouble,
      ShapeFactory.point(
        row.getAs[String]("pickup_longitude").toDouble,
        row.getAs[String]("pickup_latitude").toDouble
      )
    ) )




    taxiTripsRdd.saveToGrid()


/*    import sparkSession.implicits._
    val ds = filteredDf.map( row => new TaxiTripData( -1L, ShapeFactory.point( 1.1, 2.2 ).asInstanceOf[PointImpl] ) )
    val taxiTripData = ds.collect()



    println(s"Saving ${taxiTripData.size} trip data to the space")
    sc.parallelize(taxiTripData).saveToGrid()*/


    df.printSchema()

    filteredDf.printSchema()

    filteredDf.show()

//    filteredDf.write.mode(SaveMode.Overwrite).grid("nytaxi1")

    val endTime = System.currentTimeMillis()

    sparkSession.stopInsightEdgeContext()

    println( "DataFrame load took " + ( endTime - startTime ) + " msec." )
  }
}