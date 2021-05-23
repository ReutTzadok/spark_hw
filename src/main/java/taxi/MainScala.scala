package taxi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import taxi.services.{DriveMostKm, DriverCodeToName, FilterByDestination, FilterByKm, SeparateToParameters, SumAllKm, TripsToKmPerDrivers}

object MainScala {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("Spark Homework").setMaster("local[*]").set("spark.testing.memory", "512000000")
    val sc = new SparkContext(sparkConf)
    val tripsFile: RDD[String] = sc.textFile("data/taxi/trips.txt")
    val driversFile: RDD[String] = sc.textFile("data/taxi/drivers.txt")

    //todo use enum instead of numbers
    println(s"Number of lines: ${tripsFile.count()}")

    val trips = SeparateToParameters(tripsFile)

    val tripsToBoston = FilterByDestination(trips, "boston")

    println(s"Amount of trips to Boston longer than 10 km is ${FilterByKm(tripsToBoston, 10).count()}")

    println(s"sum of km trips to boston is ${SumAllKm(tripsToBoston)}")

    DriveMostKm(TripsToKmPerDrivers(trips), 3)
      .map(line => Tuple2(DriverCodeToName(line._1, SeparateToParameters(driversFile)), line._2))
      .foreach(x => println(s"${x._1}, ${x._2} km"))
  }

}
