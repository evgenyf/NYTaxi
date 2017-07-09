package nyctaxi

case class TripData(

  var VendorID : String,
  var tpep_pickup_datetime :  String,
  var tpep_dropoff_datetime :  String,
  var passenger_count :  String,
  var trip_distance :  String,
  var pickup_longitude :  String,
  var pickup_latitude :  String,
  var RatecodeID :  String,
  var store_and_fwd_flag :  String,
  var dropoff_longitude :  String,
  var dropoff_latitude :  String,
  var payment_type :  String,
  var fare_amount :  String,
  var extra :  String,
  var mta_tax :  String,
  var tip_amount :  String,
  var tolls_amount :  String,
  var improvement_surcharge :  String,
  var total_amount :  String

) {

  def this() = this(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null )

}