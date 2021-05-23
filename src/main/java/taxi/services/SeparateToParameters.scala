package taxi.services

import org.apache.spark.rdd.RDD

object SeparateToParameters {
  def apply(trips : RDD[String]) : RDD[Array[String]] = {
    trips.map(trip => trip.split(" "))
  }
}
