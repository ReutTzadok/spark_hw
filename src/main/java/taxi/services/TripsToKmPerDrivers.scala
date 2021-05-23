package taxi.services

import org.apache.spark.rdd.RDD

object TripsToKmPerDrivers {
  def apply(trips : RDD[Array[String]]) : Array[(Int, Int)] = {
    trips.map(trip => Tuple2(trip(0).toInt, trip(2).toInt))
      .reduceByKey((a, b) => a+b)
      .sortBy(_._2, false)
      .collect()
  }
}
