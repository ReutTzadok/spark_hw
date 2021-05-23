package taxi.services

import org.apache.spark.rdd.RDD

object SumAllKm {
  def apply (trips : RDD[Array[String]]) : Int = {
    trips.map(trip => trip(2).toInt).sum().toInt
  }

}
