package nyctaxi

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate}
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
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

    print( "Arguments:" )
    args.foreach(println)

    if (args.length == 0) {
      System.err.println("At least one parameter with link to csv file should be passed")
      System.exit(1)
    }

    val settings = if (args.length > 3) args else Array("spark://127.0.0.1:7077", "insightedge-space", "insightedge", "127.0.0.1:4174", "n/a", "n/a", "n/a")

    if (settings.length < 6) {
      System.err.println("Usage: LoadDataFrame <spark master url> <space name> <space groups> <space locator>, first parameter must be url to csv file")
      System.exit(1)
    }

    val Array(master, space, groups, locators) = settings.slice(0, 4)
    val ieConfig = InsightEdgeConfig(space, Some(groups), Some(locators))

    val conf = new SparkConf().setMaster(master).setAppName("NYC taxi - loader")
    if (args.length > 1) {
      println("Setting [spark.default.parallelism] :" + args(1))
      conf.set("spark.default.parallelism", args(1))
    }
    val sc = new SparkContext(conf)
    val defaultParallelism = sc.defaultParallelism

    println( "defaultParallelism:" + defaultParallelism )

    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Taxi")
      .insightEdgeConfig(ieConfig)
      .getOrCreate()

    val allProps = conf.getAll

//    val sparkDefaultParallelismVal = conf.get("spark.default.parallelism")

    val paths = args(0).split(",")

    println( "Paths:" + paths.mkString(", ") )

    val startTime = System.currentTimeMillis()


    var df = sparkSession.read.option("header","true")/*.option("fs.local.block.size", 512)*/.csv(paths.toSeq: _*)

    if (args.length > 2) {
      println("Setting partitions count:" + args(2))
      df = df.repartition( args(2).toInt )
    }


    val numOfPartitions = df.rdd.getNumPartitions

//    println( "NUM=" + df.rdd.repartition(numOfPartitions*2).getNumPartitions )

    println( "Partitions number:" + numOfPartitions )

//    println( "Partitions number after increasing:" + df.rdd.getNumPartitions )

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


//    println( "Partitions number 2:" + filteredDf.rdd.getNumPartitions )

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




//    taxiTripsRdd.



/*
    df.printSchema()

    filteredDf.printSchema()

    filteredDf.show()
*/


    val endTime = System.currentTimeMillis()


    val time1 = System.currentTimeMillis()
    //TODO time of writing to file
    val fileName = "results___" + System.currentTimeMillis() + ".txt"
    taxiTripsRdd.saveAsTextFile( fileName )
    val time2 = System.currentTimeMillis()
    //TODO time of persist
    //taxiTripsRdd.persist(StorageLevel.DISK_ONLY)
    //val time3 = System.currentTimeMillis()






//    println( "Rdd count=" + taxiTripsRdd.count() )

    sparkSession.stopInsightEdgeContext()

    println( "DataFrame load took " + ( endTime - startTime ) + " msec., saving as text file took:" + ( time2 - time1 ) + " msec. to file:" + fileName + ", used partitions number:" + numOfPartitions + ", defaultParallelism:" + defaultParallelism )
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