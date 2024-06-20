package ai.humn.telematics

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, Duration}
import java.util.Locale
import java.util.logging.{Level, Logger}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try
import java.nio.file.Paths
import java.time.Instant
import java.io.{BufferedReader, FileReader}

object ProcessDataFile {
  // Initialize logger
  val logger: Logger = Logger.getLogger(getClass.getName)

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

        val startTimeMillis = journey(fieldToIndex("startTime")).toLong
        val endTimeMillis = journey(fieldToIndex("endTime")).toLong
        val startTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(startTimeMillis), zoneId)
        val endTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(endTimeMillis), zoneId)


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
  def queryByDurationRange(journeys: List[JourneyMetadata], durationStart: Double = 0.0, durationEnd: Double = Double.MaxValue): List[JourneyMetadata] = {
    val filteredJourneys = journeys.filter { journey =>
      val duration = journey.duration
      duration >= durationStart && duration <= durationEnd
    }

    filteredJourneys
  }

  // print journey
  def printJourney(journey: JourneyMetadata): Unit = {
    println(f"journeyId: ${journey.journeyId} ${journey.driverId} distance ${journey.distanceKm} durationMS  ${journey.durationMS} avgSpeed in kph was ${journey.avgSpeed}%.2f")
  }

  // print journey list
  def printJourneyList(journeys: List[JourneyMetadata]): Unit = {
    journeys.foreach(journey => printJourney(journey))
  }
  
  // queryJourneysByMinimumDuration
  def queryJourneysByMinimumDuration(journeys: List[JourneyMetadata], durationStart: Double): Unit = {
    println(s"Journeys of ${durationStart} minutes or more:")
    
    val filteredJourneys = queryByDurationRange(journeys, durationStart)

    if (filteredJourneys.nonEmpty) {
      printJourneyList(filteredJourneys)
    } else {
      println("No journeys matching the duration criteria.")
    }
  }

  // queryJourneysByAverageSpeedRange: Find the average speed per journey in kph, which is between avgSpeedStart and avgSpeedEnd.
  def queryJourneysByAverageSpeedRange(journeys: List[JourneyMetadata], avgSpeedStart: Double = 0.0 , avgSpeedEnd: Double = Double.MaxValue): List[JourneyMetadata] = {
    val filteredJourneys = journeys.filter { journey =>
      val avgSpeed = journey.avgSpeed
      avgSpeed >= avgSpeedStart && avgSpeed <= avgSpeedEnd
    }

    filteredJourneys
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

  // Function to find the most active driver
  def findMostActiveDriver(aggregateData: Map[String, Double]): (String, Double) = {
    aggregateData.maxBy(_._2)
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

  // Function to read lines from file
  def readLinesFromFile(filePath: String, withHeader: Boolean = true): List[String] = {
    var reader: BufferedReader = null
    var lines = new ListBuffer[String]()
    
    try {
      reader = new BufferedReader(new FileReader(filePath))
      var line: String = null

      // Skip the first line if withHeader is true
      var isFirstLine = false 
      if (withHeader) {
        isFirstLine = true
      }
      
      while ({ line = reader.readLine(); line != null }) {
        if (isFirstLine) {
          isFirstLine = false
        } else {
          lines += line.trim 
        }
      }
    } catch {
      case e: Exception => println(s"Error reading file: ${e.getMessage}")
    } finally {
      if (reader != null) {
        try {
          reader.close()
        } catch {
          case e: Exception => println(s"Error closing reader: ${e.getMessage}")
        }
      }
    }
    
    // Convert to list and remove duplicates
    lines.toList.distinct  
  }

  def processFile(filePath: String, batchDate: String, zoneId: ZoneId): Unit = {
    // Read file
    val withHeader = true
    val lines = readLinesFromFile(filePath)
    
    val journeys = lines.flatMap(line => cleanData(line.toString, schemaV1FieldToIndex, zoneId))

    // Task 1: Find journeys that are 90 minutes or more
    // durationStart as parameter for future adjustment
    val durationStart = 90.0
    queryJourneysByMinimumDuration(journeys, durationStart)
    
    // Task 2: Find the average speed per journey in kph
    println("\nAverage speed per journey in kph:")
    val avgSpeedStart = 0.0
    val avgSpeedEnd = Double.MaxValue 
    val filteredJourneys = queryJourneysByAverageSpeedRange(journeys, avgSpeedStart, avgSpeedEnd)
    printJourneyList(filteredJourneys)

    // Task 3: Find the total mileage by driver for the whole day
    println("\nTotal mileage by driver for the whole day:")
    val totalMileageByDriver = aggregateByDriver(journeys)
    totalMileageByDriver.foreach { case (driverId, totalMileage) =>
      println(s"$driverId drove $totalMileage kilometers")
    }

    // Task 4: Find the most active driver - the driver who has driven the most kilometers
    println("\nMost active driver - the driver who has driven the most kilometers:")
    val mostActiveDriver = findMostActiveDriver(totalMileageByDriver)
    println(s"Most active driver is ${mostActiveDriver._1}")
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
    // This can be configured in the future by json file and/or command line arguments
    val zoneId = ZoneId.of("America/Los_Angeles")

    try {
      processFile(filePath, batchDate, zoneId)
    } catch { 
      case e: Exception => println(s"Error: ${e.getMessage}")
    } 
  
    // println("\nTask 1 - 4 have done. Thank you for your time and effort! Have a good time! ")
  }

}
