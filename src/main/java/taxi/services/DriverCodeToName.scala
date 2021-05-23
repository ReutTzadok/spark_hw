package taxi.services

import org.apache.spark.rdd.RDD

object DriverCodeToName {
  def apply(code: Int, drivers: RDD[Array[String]]) : String = {
//    var name:String =
      drivers.filter(driver => driver(0).replace(",", "").toInt == code)
      .map(driver => driver(1) +" " + driver(2))
      .take(1)(0)
      .replace(",","")
//    name.replace(",","")
  }

}
