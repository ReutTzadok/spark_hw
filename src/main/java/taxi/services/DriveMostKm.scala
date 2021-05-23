package taxi.services

object DriveMostKm {
  def apply (kmPerDrivers : Array[(Int, Int)], numOfDrivers : Int) : Array[(Int, Int)] = {
    kmPerDrivers.take(numOfDrivers)
  }
}
