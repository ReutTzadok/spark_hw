package taxi.services

import org.apache.spark.rdd.RDD

object FilterByKm {
  def apply(trips: RDD[Array[String]], km: Int): RDD[Array[String]] = {
    trips.filter(trip => trip(2).toInt > km)
  }

}
