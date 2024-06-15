package ai.humn.telematics
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Locale
import java.util.logging.{Level, Logger}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try
import java.nio.file.Paths

object ProcessDataFile {
  // Initialize logger
  val logger: Logger = Logger.getLogger(getClass.getName)


  // ------------------------1.Data Validation, Data Clean, and Precompute Metrics---------------------------------------
  // This is a collection of the journey lines with precomputed metrics
  // Benefits:
  // 1.Improved Code Readability and Maintainability: Simplified Logic; Reduced Complexity;
  // 2.Performance Optimization: Single Pass Calculation; Less Repetition;
  // 3.Error Reduction:Consistency;Debugging;
  // 4.Scalability:Ease of Expansion; Precomputing and storing derived values makes it easier to handle batch processing or parallel processing

  // Other driver behavior fields also can be precomputed if needed.
  // calculate distance based on GPS and Geohash, can help detect fraud driver and adhoc query 
  // val distanceKm_gps = calculateGPSDistanceKm(journey(4),journey(6),journey(5),journey(7)) 
  // startGeoHash = calculateGeoHash(journey(4),journey(6))
  // endGeoHash = calculateGeoHash(journey(5),journey(7))
  
  // Schema evolution: Define mappings for different schema versions
  // This can be configurable in the future by json file and/or command line arguments
  val schemaV1FieldToIndex: Map[String, Int] = Map(
    "journeyId" -> 0,
    "driverId" -> 1,
    "startTime" -> 2,
    "endTime" -> 3,
    "startLat" -> 4,
    "startLon" -> 5,
    "endLat" -> 6,
    "endLon" -> 7,
    "startOdometer" -> 8,
    "endOdometer" -> 9
  )
  
  // Metadata structure to store processed data
  case class JourneyMetadata(
    journeyId: String,
    driverId: String,
    startTime: LocalDateTime,
    endTime: LocalDateTime,
    startLat: String,
    startLon: String,
    endLat: String,
    endLon: String,
    startOdometer: Double,
    endOdometer: Double,
    distanceKm: Double,
    duration: Double,
    durationMS: Long,
    avgSpeed: Double
  )

  // Function to clean and validate each line of data based on schema version
  def cleanData(line: String, fieldToIndex: Map[String, Int], zoneId: ZoneId): Option[JourneyMetadata] = {
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH).withZone(zoneId)

    try {
      val journey = line.split(",").map(_.trim)
      if (journey.length == fieldToIndex.size) {
        val journeyId = journey(fieldToIndex("journeyId"))
        val driverId = journey(fieldToIndex("driverId"))
        val startTime = LocalDateTime.parse(journey(fieldToIndex("startTime")), dateFormat)
        val endTime = LocalDateTime.parse(journey(fieldToIndex("endTime")), dateFormat)
        val startOdometer = journey(fieldToIndex("startOdometer")).toDouble
        val endOdometer = journey(fieldToIndex("endOdometer")).toDouble
        val startLat = journey(fieldToIndex("startLat"))
        val startLon = journey(fieldToIndex("startLon"))
        val endLat = journey(fieldToIndex("endLat"))
        val endLon = journey(fieldToIndex("endLon"))

        if (endTime.isAfter(startTime) && endOdometer > startOdometer) {
          val durationMS = Duration.between(startTime, endTime).toMillis
          val duration = durationMS / (1000 * 60.0) // Duration in minutes
          val distanceKm = endOdometer - startOdometer
          val avgSpeed = if (duration != 0) distanceKm / (duration / 60.0) else 0.0

          Some(JourneyMetadata(journeyId, driverId, startTime, endTime, startLat, startLon, endLat, endLon,
            startOdometer, endOdometer, distanceKm, duration, durationMS, avgSpeed))
        } else {
          logger.log(Level.WARNING, s"Invalid data detected: $line")
          None
        }
      } else {
        logger.log(Level.WARNING, s"Invalid format or empty fields: $line")
        None
      }
    } catch {
      case e: Exception =>
        logger.log(Level.SEVERE, s"Error processing line: $line", e)
        None
    }
  }

  // Query functions by duration with start and end
  def queryByDurationRange(journeys: List[JourneyMetadata], durationStart: Double = 90.0, durationEnd: Double = Double.MaxValue): Option[JourneyMetadata] = {
    val filteredJourneys = journeys.filter { journey =>
      val duration = journey.duration
      duration >= durationStart && duration <= durationEnd
    }

    filteredJourneys
  }

  // print journey
  def printJourney(journey: JourneyMetadata): Unit = {
    println(s"journeyId: ${journey.journeyId} ${journey.driver} distance ${journey.distanceKm} durationMS  ${journey.durationMS} avgSpeed in kph was ${journey.avgSpeed}")
  }

  // print journey list
  def printJourneyList(journeys: List[JourneyMetadata]): Unit = {
    journeys.foreach(journey => printJourney(journey))
  }
  
  // Task 1: 
  def task1(journeys: List[JourneyMetadata], durationStart: decimal): Unit = {
    println(s"Journeys of ${durationStart} minutes or more:")
    
    // call queryByDurationRange
    val filteredJourneys = queryByDurationRange(journeys, durationStart)

    if (filteredJourneys.nonEmpty) {
      printJourneyList(filteredJourneys)
    } else {
      println("No journeys matching the duration criteria.")
    }
  }

  // task 2: Find the average speed per journey in kph, directly from the data.
  def task2(journeys: List[JourneyMetadata]): Unit = {
    println("\nAverage speed per journey in kph:")
    printJourneyList(journeys)
  }


  // There is some discrepancy over the whole day when data is ingested, so journey.endTime is more accurate.
  // And you do not need to wait for the long journeys to be completed, just move them to the next day.
  // But you need to filter out the invalid data (maybe uncompleted journeys) for the current day.
  def aggregateByDriver(journeys: List[JourneyMetadata]): Map[String, Double] = {
    // Group journeys by driverId
    val driverGroupedJourneys = journeys.groupBy(_.driverId)
    
    val results = driverGroupedJourneys.map { case (driverId, journeys) =>
      val totalMileage = journeys.map(_.distanceKm).sum
      (driverId, totalMileage)
    }.toMap

    results
  }


  // task 3: Find the total mileage by driver for the whole day.
  def task3(journeys: List[JourneyMetadata]): Map[String, Double] = {
    println("\nTotal mileage by driver for the whole day:")
    val groupedMileage = aggregateByDriver(journeys)
    groupedMileage.foreach { case (driverId, totalMileage) =>
      println(s"$driverId drove $totalMileage kilometers")
    }
    groupedMileage
  }


  // task 4: Find the most active driver from the result of task 3
  def task4(groupedMileage: Map[String, Double]): Unit = {
    println("\nMost active driver - the driver who has driven the most kilometers:")
    if (groupedMileage.nonEmpty) {
      val mostActiveDriver = groupedMileage.maxBy(_._2)._1
      println(s"Most active driver is $mostActiveDriver")
    } else {
      println("No journeys found.")
    }
  }

  // Function to extract batch date from file name
  def extractBatchDateFromFileName(filePath: String): String = {
    val fileName = Paths.get(filePath).getFileName.toString
    val datePattern = "\\d{4}-\\d{2}-\\d{2}".r
    datePattern.findFirstIn(fileName).getOrElse {
      println("No batch date found in file name. Using default date.")
      "2021-10-05" // Default batch date for this testing if not found in file name
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: ProcessDataFile <file-path> <batch-date>")
      System.exit(1)
    }

    val filePath = args(0)
    val batchDate = if (args.length > 1) args(1) else extractBatchDateFromFileName(filePath)
    println(s"Batch Date: $batchDate")
    
    // Timezone also can be a configurable field.
    // More timezones: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    // For example: "America/Los_Angeles"
    val zoneId = ZoneId.of("America/Los_Angeles")

    // Read file
    val lines = Source.fromFile(filePath).getLines().toList

    // Clean and parse data
    val journeys = lines.flatMap(line => cleanData(line, schemaV1FieldToIndex, zoneId))
    // Of course, we can persist the cleaned data to HDFS as a DWD layer in Datawarehouse.
    // But let's just skip this. and print the cleaned data later from task2.


    // Task 1: Find journeys that are 90 minutes or more
    // durationStart as parameter for future adjustment
    val durationStart = 90.0
    task1(journeys, durationStart)
    
    // Task 2: Find the average speed per journey in kph
    task2(journeys)

    // Task 3: Find the total mileage by driver for the whole day
    val groupedMileage = task3(journeys)

    // Task 4: Find the most active driver - the driver who has driven the most kilometers
    task4(groupedMileage)
  
    // println("\nTask 1 - 4 have done. Thank you for your time and effort! Have a good time! ")
  }

}
