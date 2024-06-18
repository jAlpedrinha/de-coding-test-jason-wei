package ai.humn.telematics
import org.scalatest.funsuite.AnyFunSuite
import java.time.{LocalDateTime, ZoneId}
import ai.humn.telematics.ProcessDataFile._
import scala.util.Success

class ProcessDataFileTest extends AnyFunSuite {

  val testZoneId: ZoneId = ZoneId.of("America/Los_Angeles")
  val testFieldToIndex: Map[String, Int] = schemaV1FieldToIndex

  // Sample valid and invalid data lines for testing
  val validDataLine = "1,1,1633046400000,1633050000000,34.0522,-118.2437,36.7783,-119.4179,1000,1050"
  val invalidDataLine = "1,1,invalidTime,1633050000000,34.0522,-118.2437,36.7783,-119.4179,1000,950"
  val shortDataLine = "1,1,1633046400000,1633050000000,34.0522,-118.2437,36.7783,-119.4179,1000"

  test("cleanData should return JourneyMetadata for valid data") {
    val result = cleanData(validDataLine, testFieldToIndex, testZoneId)
    assert(result.isDefined)
    val journey = result.get
    assert(journey.journeyId == "1")
    assert(journey.driverId == "1")
    assert(journey.distanceKm == 50)
  }

  test("cleanData should return None for invalid data") {
    val result = cleanData(invalidDataLine, testFieldToIndex, testZoneId)
    assert(result.isEmpty)
  }

  test("cleanData should return None for short data") {
    val result = cleanData(shortDataLine, testFieldToIndex, testZoneId)
    assert(result.isEmpty)
  }

  val sampleJourneys = List(
    JourneyMetadata("1", "1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(100), "34.0522", "-118.2437", "36.7783", "-119.4179", 1000, 1050, 50, 100, 6000000, 30),
    JourneyMetadata("2", "1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(45), "34.0522", "-118.2437", "36.7783", "-119.4179", 1050, 1100, 50, 45, 2700000, 66.67),
    JourneyMetadata("3", "2", LocalDateTime.now(), LocalDateTime.now().plusMinutes(30), "34.0522", "-118.2437", "36.7783", "-119.4179", 1100, 1150, 50, 30, 1800000, 100)
  )

  test("queryByDurationRange should filter journeys by duration range") {
    val result = queryByDurationRange(sampleJourneys, 40.0, 120.0)
    assert(result.length == 2)
  }

  test("queryJourneysByMinimumDuration should filter and print journeys with minimum duration") {
    val durationStart = 90.0
    val filteredJourneys = queryByDurationRange(sampleJourneys, durationStart)
    assert(filteredJourneys.length == 1)
    assert(filteredJourneys.head.journeyId == "1")
  }

  test("queryJourneysByAverageSpeedRange should filter journeys by average speed range") {
    val result = queryJourneysByAverageSpeedRange(sampleJourneys, 60.0, 120.0)
    assert(result.length == 2)
  }

  test("aggregateByDriver should calculate total mileage by driver") {
    val result = aggregateByDriver(sampleJourneys)
    assert(result.size == 2)
    assert(result("1") == 100.0)
    assert(result("2") == 50.0)
  }

  test("extractBatchDateFromFileName should extract date from file name") {
    val filePath = "/path/to/data_2021-10-05.csv"
    val result = extractBatchDateFromFileName(filePath)
    assert(result == "2021-10-05")
  }

  test("extractBatchDateFromFileName should return default date if no date found in file name") {
    val filePath = "/path/to/datafile.csv"
    val result = extractBatchDateFromFileName(filePath)
    assert(result == "2021-10-05")
  }
}

