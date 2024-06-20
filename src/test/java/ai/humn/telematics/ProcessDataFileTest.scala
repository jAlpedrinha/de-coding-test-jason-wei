package ai.humn.telematics

import java.time._
import java.util.TimeZone
import java.util.logging.Logger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import ai.humn.telematics.ProcessDataFile._

class ProcessDataFileTest extends AnyFlatSpec with Matchers {
  val zoneId: ZoneId = ZoneId.of("America/Los_Angeles")

  "cleanData" should "return JourneyMetadata for valid data" in {
    val line = "1,driver1,1633046400000,1633050000000,37.7749,-122.4194,37.7749,-122.4194,100,150"
    val result = ProcessDataFile.cleanData(line, ProcessDataFile.schemaV1FieldToIndex, zoneId)
    result shouldBe defined

    val journey = result.get
    journey.journeyId shouldBe "1"
    journey.driverId shouldBe "driver1"
    journey.startOdometer shouldBe 100
    journey.endOdometer shouldBe 150
    journey.distanceKm shouldBe 50
  }

  it should "return None for invalid data" in {
    val line = "1,driver1,invalid_timestamp,1633050000000,37.7749,-122.4194,37.7749,-122.4194,100,150"
    val result = ProcessDataFile.cleanData(line, ProcessDataFile.schemaV1FieldToIndex, zoneId)
    result shouldBe None
  }

  "queryByDurationRange" should "filter journeys by duration" in {
    val journeys = List(
      ProcessDataFile.JourneyMetadata("1", "driver1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(100), "37.7749", "-122.4194", "37.7749", "-122.4194", 100, 150, 50, 100, 6000000, 30),
      ProcessDataFile.JourneyMetadata("2", "driver2", LocalDateTime.now(), LocalDateTime.now().plusMinutes(50), "37.7749", "-122.4194", "37.7749", "-122.4194", 100, 130, 30, 50, 3000000, 30)
    )

    val result = ProcessDataFile.queryByDurationRange(journeys, 90)
    result.size shouldBe 1
    result.head.journeyId shouldBe "1"
  }

  "queryJourneysByAverageSpeedRange" should "filter journeys by average speed" in {
    val journeys = List(
      ProcessDataFile.JourneyMetadata("1", "driver1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(100), "37.7749", "-122.4194", "37.7749", "-122.4194", 100, 150, 50, 100, 6000000, 30),
      ProcessDataFile.JourneyMetadata("2", "driver2", LocalDateTime.now(), LocalDateTime.now().plusMinutes(50), "37.7749", "-122.4194", "37.7749", "-122.4194", 100, 130, 30, 50, 3000000, 60)
    )

    val result = ProcessDataFile.queryJourneysByAverageSpeedRange(journeys, 40)
    result.size shouldBe 1
    result.head.journeyId shouldBe "2"
  }

  "aggregateByDriver" should "calculate total mileage by driver" in {
    val journeys = List(
      ProcessDataFile.JourneyMetadata("1", "driver1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(100), "37.7749", "-122.4194", "37.7749", "-122.4194", 100, 150, 50, 100, 6000000, 30),
      ProcessDataFile.JourneyMetadata("2", "driver1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(50), "37.7749", "-122.4194", "37.7749", "-122.4194", 150, 180, 30, 50, 3000000, 60)
    )

    val result = ProcessDataFile.aggregateByDriver(journeys)
    result.size shouldBe 1
    result("driver1") shouldBe 80.0
  }

  "findMostActiveDriver" should "return the driver with the most mileage" in {
    val aggregateData = Map(
      "driver1" -> 80.0,
      "driver2" -> 50.0
    )

    val result = ProcessDataFile.findMostActiveDriver(aggregateData)
    result._1 shouldBe "driver1"
    result._2 shouldBe 80.0
  }

  "extractBatchDateFromFileName" should "extract date from filename" in {
    val filePath = "/path/to/data_2023-06-20.csv"
    val result = ProcessDataFile.extractBatchDateFromFileName(filePath)
    result shouldBe "2023-06-20"
  }

  it should "use default date if no date found" in {
    val filePath = "/path/to/data.csv"
    val result = ProcessDataFile.extractBatchDateFromFileName(filePath)
    result shouldBe "2021-10-05"
  }

  "readLinesFromFile" should "read lines from file and skip header" in {
    val filePath = "src/test/resources/2021-10-05_journeys.csv"
    val result = ProcessDataFile.readLinesFromFile(filePath)
    result should not be empty
    result.head should not include "journeyId,driverId,startTime,endTime,startLat,startLon,endLat,endLon,startOdometer,endOdometer"
  }

  it should "handle empty file gracefully" in {
    val filePath = "/path/to/empty/file.txt"
    val result = ProcessDataFile.readLinesFromFile(filePath)
    result shouldBe empty
  }
}