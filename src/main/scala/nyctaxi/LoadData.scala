package nyctaxi

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate}
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.insightedge.spark.context.InsightEdgeConfig
import org.insightedge.spark.implicits.all._
import org.insightedge.spark.utils.GridProxyFactory
import org.openspaces.spatial.ShapeFactory

/**
  * Created by evgeny on 7/5/17.
  */
object LoadData {

  val goldmanSacksLocation = new Point( -74.013961, 40.714672 )
  val dateTimePattern = "yyyy-MM-dd HH:mm:ss"
  val locationPrecision = 0.001 // about 100 meters
  val c = Calendar.getInstance()

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
    val paths = args(0).split(",")

    println( "Paths:" + paths.mkString(", ") )

    val startTime = System.currentTimeMillis()

    val df = sparkSession.read.option("header","true").csv(paths.toSeq: _*)

    val numOfPartitions = df.rdd.getNumPartitions

    println( "Total count:" + df.count() + ", partitions number:" + numOfPartitions )

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
          df("dropoff_latitude") <= ( goldmanSacksLocation.latitude + locationPrecision ))
      .filter(d=>isWeekday(d.getAs[String]("tpep_pickup_datetime")))

//    println( "filteredDf count:" + filteredDf.count() )

    val taxiTripsRdd = filteredDf.rdd.map( row => new TaxiTripData(
      id = null,
      row.getAs[String]("VendorID").toInt,
      new SimpleDateFormat(dateTimePattern).parse( row.getAs[String]("tpep_pickup_datetime") ),
      new SimpleDateFormat(dateTimePattern).parse( row.getAs[String]("tpep_dropoff_datetime") ),
      row.getAs[String]("passenger_count").toInt,
      row.getAs[String]("trip_distance").toDouble,
      //added new hour column
      getHour( new SimpleDateFormat(dateTimePattern).parse( row.getAs[String]("tpep_dropoff_datetime") ) ),
      ShapeFactory.point(
        row.getAs[String]("pickup_longitude").toDouble,
        row.getAs[String]("pickup_latitude").toDouble
      )
    ) )

      //clear existing data
    //TODO ! expose indication for clearing space before writing
    GridProxyFactory.getOrCreateClustered(ieConfig).clear(null)
    taxiTripsRdd.saveToGrid()

/*
    df.printSchema()

    filteredDf.printSchema()

    filteredDf.show()
*/

    println( "Rdd count=" + taxiTripsRdd.count() )

    val endTime = System.currentTimeMillis()

    sparkSession.stopInsightEdgeContext()

    println( "DataFrame load took " + ( endTime - startTime ) + " msec." )
  }

  def getHour( date : Date ): Int ={
    c.setTime(date)
    c.get( Calendar.HOUR_OF_DAY )
  }

  def isWeekday(string:String):Boolean = {

    val dateTimeFormat = DateTimeFormatter.ofPattern( dateTimePattern )
    val day=LocalDate.parse(string,dateTimeFormat).getDayOfWeek()

    if (day.compareTo(DayOfWeek.SATURDAY)==1) return false

    if (day.compareTo(DayOfWeek.SUNDAY)==1) return false

    return true
  }
}