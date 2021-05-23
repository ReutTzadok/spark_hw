package taxi.services

import org.apache.spark.rdd.RDD

object FilterByDestination{
  def apply(trips : RDD[Array[String]], city: String) : RDD[Array[String]] = {
    trips.filter(trip => trip(1).equalsIgnoreCase(city))
  }
}
