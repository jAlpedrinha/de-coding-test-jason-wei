package ai.humn.telematics

import java.time._
import java.util.TimeZone
import java.util.logging.Logger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import ai.humn.telematics.ProcessDataFile._

class ProcessDataFileTest extends AnyFlatSpec with Matchers {

  // Initialize test logger
  val logger: Logger = Logger.getLogger(getClass.getName)

  // Test data for cleanData function
  val validLine = "1,123,2023-06-18 10:00:00,2023-06-18 11:30:00,37.7749,-122.4194,34.0522,-118.2437,100.0,200.0"
  val invalidLine = "1,123,2023-06-18 11:30:00,2023-06-18 10:00:00,37.7749,-122.4194,34.0522,-118.2437,200.0,100.0"
  val fieldToIndex = Map(
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
  val zoneId: ZoneId = ZoneId.of(TimeZone.getDefault.getID)

  // Test cases for cleanData function
  "cleanData" should "return Some(JourneyMetadata) for valid input" in {
    val result = ProcessDataFile.cleanData(validLine, fieldToIndex, zoneId)
    result.isDefined shouldBe true
  }

  it should "return None for invalid input" in {
    val result = ProcessDataFile.cleanData(invalidLine, fieldToIndex, zoneId)
    result.isDefined shouldBe false
  }

  // Test cases for queryByDurationRange function
  "queryByDurationRange" should "filter journeys by duration range" in {
    val journeys = List(
      ProcessDataFile.cleanData(validLine, fieldToIndex, zoneId).get,
      ProcessDataFile.cleanData(invalidLine, fieldToIndex, zoneId).getOrElse(fail("Invalid test data"))
    )
    val result = ProcessDataFile.queryByDurationRange(journeys, 60.0, 120.0)
    result.length shouldBe 1
  }

  it should "return empty list when no journeys match the duration range" in {
    val journeys = List(
      ProcessDataFile.cleanData(validLine, fieldToIndex, zoneId).get
    )
    val result = ProcessDataFile.queryByDurationRange(journeys, 120.0, 180.0)
    result shouldBe empty
  }

  // Test cases for queryJourneysByAverageSpeedRange function
  "queryJourneysByAverageSpeedRange" should "filter journeys by average speed range" in {
    val journeys = List(
      ProcessDataFile.cleanData(validLine, fieldToIndex, zoneId).get,
      ProcessDataFile.cleanData(invalidLine, fieldToIndex, zoneId).getOrElse(fail("Invalid test data"))
    )
    val result = ProcessDataFile.queryJourneysByAverageSpeedRange(journeys, 0.0, 100.0)
    result.length shouldBe 1
  }

  it should "return empty list when no journeys match the average speed range" in {
    val journeys = List(
      ProcessDataFile.cleanData(validLine, fieldToIndex, zoneId).get
    )
    val result = ProcessDataFile.queryJourneysByAverageSpeedRange(journeys, 200.0, 300.0)
    result shouldBe empty
  }

  // Test cases for aggregateByDriver function
  "aggregateByDriver" should "calculate total mileage per driver" in {
    val journeys = List(
      ProcessDataFile.cleanData(validLine, fieldToIndex, zoneId).get,
      ProcessDataFile.cleanData(invalidLine, fieldToIndex, zoneId).getOrElse(fail("Invalid test data"))
    )
    val result = ProcessDataFile.aggregateByDriver(journeys)
    result.nonEmpty shouldBe true
  }

  it should "return empty map when no journeys are provided" in {
    val journeys = List.empty[JourneyMetadata]
    val result = ProcessDataFile.aggregateByDriver(journeys)
    result shouldBe empty
  }

  // Test cases for extractBatchDateFromFileName function
  "extractBatchDateFromFileName" should "extract batch date from file name" in {
    val filePath = "/path/to/2023-06-18-data.csv"
    val result = ProcessDataFile.extractBatchDateFromFileName(filePath)
    result shouldBe "2023-06-18"
  }

  it should "use default date when batch date is not found in file name" in {
    val filePath = "/path/to/data.csv"
    val result = ProcessDataFile.extractBatchDateFromFileName(filePath)
    result shouldBe "2021-10-05" // Default batch date as defined in the function
  }

}