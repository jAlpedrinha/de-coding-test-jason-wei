package ai.humn.telematics

import org.scalatest._
import java.time._
import java.util.TimeZone
import scala.collection.mutable.ListBuffer


class ProcessDataFileSpec extends FlatSpec with Matchers with MockFactory {
  // Test case for valid journey data
  "cleanData" should "parse valid journey data correctly" in {
    val line = "1,driver1,2023-06-01 12:00:00,2023-06-01 13:30:00,37.7749,-122.4194,34.0522,-118.2437,100.0,150.0"
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
    val zoneId = ZoneId.of("UTC")

    val result = ProcessDataFile.cleanData(line, fieldToIndex, zoneId)

    result shouldBe defined
    val journey = result.get
    journey.journeyId shouldEqual "1"
    journey.driverId shouldEqual "driver1"
    journey.startLat shouldEqual "37.7749"
    journey.endLat shouldEqual "34.0522"
    journey.startOdometer shouldEqual 100.0
    journey.endOdometer shouldEqual 150.0
  }

  // Test case for invalid journey data (start time after end time)
  it should "return None for journey with invalid start and end times" in {
    val line = "1,driver1,2023-06-01 13:30:00,2023-06-01 12:00:00,37.7749,-122.4194,34.0522,-118.2437,100.0,150.0"
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
    val zoneId = ZoneId.of("UTC")

    val result = ProcessDataFile.cleanData(line, fieldToIndex, zoneId)

    result shouldBe None
  }

  // Test case for invalid journey data (missing fields)
  it should "return None for journey with missing fields" in {
    val line = "1,driver1,2023-06-01 12:00:00,2023-06-01 13:30:00,37.7749,-122.4194,34.0522,-118.2437,100.0"
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
    val zoneId = ZoneId.of("UTC")

    val result = ProcessDataFile.cleanData(line, fieldToIndex, zoneId)

    result shouldBe None
  }

  // Test case for filtering journeys by duration range
  "queryByDurationRange" should "return journeys within specified duration range" in {
    val journeys = List(
      ProcessDataFile.JourneyMetadata("1", "driver1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(100), "37.7749", "-122.4194", "34.0522", "-118.2437", 100.0, 200.0, 100.0, 100.0, 6000000, 60.0),
      ProcessDataFile.JourneyMetadata("2", "driver2", LocalDateTime.now(), LocalDateTime.now().plusMinutes(50), "37.7749", "-122.4194", "34.0522", "-118.2437", 150.0, 200.0, 50.0, 50.0, 3000000, 60.0),
      ProcessDataFile.JourneyMetadata("3", "driver3", LocalDateTime.now(), LocalDateTime.now().plusMinutes(120), "37.7749", "-122.4194", "34.0522", "-118.2437", 200.0, 300.0, 100.0, 120.0, 7200000, 50.0)
    )

    val durationStart = 90.0
    val result = ProcessDataFile.queryByDurationRange(journeys, durationStart)

    result should have length 1
    result.head.journeyId shouldEqual "3"
  }

  // Test case for aggregating total mileage by driver
  "aggregateByDriver" should "return total mileage for each driver" in {
    val journeys = List(
      ProcessDataFile.JourneyMetadata("1", "driver1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(100), "37.7749", "-122.4194", "34.0522", "-118.2437", 100.0, 200.0, 100.0, 100.0, 6000000, 60.0),
      ProcessDataFile.JourneyMetadata("2", "driver1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(50), "37.7749", "-122.4194", "34.0522", "-118.2437", 150.0, 200.0, 50.0, 50.0, 3000000, 60.0),
      ProcessDataFile.JourneyMetadata("3", "driver2", LocalDateTime.now(), LocalDateTime.now().plusMinutes(120), "37.7749", "-122.4194", "34.0522", "-118.2437", 200.0, 300.0, 100.0, 120.0, 7200000, 50.0)
    )

    val result = ProcessDataFile.aggregateByDriver(journeys)

    result should have size 2
    result("driver1") shouldEqual 150.0
    result("driver2") shouldEqual 100.0
  }

  // Test case for logging invalid data
  "cleanData" should "log warning for invalid journey data" in {
    val mockLogger = mock[Logger]
    ProcessDataFile.logger = mockLogger

    val line = "1,driver1,2023-06-01 13:30:00,2023-06-01 12:00:00,37.7749,-122.4194,34.0522,-118.2437,100.0,150.0"
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
    val zoneId = ZoneId.of("UTC")

    val result = ProcessDataFile.cleanData(line, fieldToIndex, zoneId)

    result shouldBe None
    (mockLogger.log(_: Level, _: String)).expects(Level.WARNING, s"Invalid data detected: $line")
  }

  // Test case for extracting batch date from file name
  "extractBatchDateFromFileName" should "extract batch date from valid file name" in {
    val filePath = "/path/to/your/file/data_2023-06-01.csv"
    val result = ProcessDataFile.extractBatchDateFromFileName(filePath)
    result shouldEqual "2023-06-01"
  }

  it should "use default batch date when no date found in file name" in {
    val filePath = "/path/to/your/file/data.csv"
    val result = ProcessDataFile.extractBatchDateFromFileName(filePath)
    result shouldEqual "2021-10-05"  // Assuming "2021-10-05" is the default batch date
  }

  // Test case for task1 with journeys
  "task1" should "print journeys of given duration or more" in {
    val mockPrintStream = new java.io.ByteArrayOutputStream()
    Console.withOut(mockPrintStream) {
      val journeys = List(
        ProcessDataFile.JourneyMetadata("1", "driver1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(100), "37.7749", "-122.4194", "34.0522", "-118.2437", 100.0, 200.0, 100.0, 100.0, 6000000, 60.0),
        ProcessDataFile.JourneyMetadata("2", "driver2", LocalDateTime.now(), LocalDateTime.now().plusMinutes(50), "37.7749", "-122.4194", "34.0522", "-118.2437", 150.0, 200.0, 50.0, 50.0, 3000000, 60.0),
        ProcessDataFile.JourneyMetadata("3", "driver3", LocalDateTime.now(), LocalDateTime.now().plusMinutes(120), "37.7749", "-122.4194", "34.0522", "-118.2437", 200.0, 300.0, 100.0, 120.0, 7200000, 50.0)
      )
      ProcessDataFile.task1(journeys, 90.0)
    }
    val output = mockPrintStream.toString.trim
    output should include("Journeys of 90.0 minutes or more:")
    output should include("journeyId: 1 driver1 distance 100.0 durationMS 6000000 avgSpeed in kph was 60.0")
    output should include("journeyId: 3 driver3 distance 100.0 durationMS 7200000 avgSpeed in kph was 50.0")
    output should not include "No journeys matching the duration criteria."
  }

  // Test case for task1 with no matching journeys
  it should "print no journeys matching the duration criteria" in {
    val mockPrintStream = new java.io.ByteArrayOutputStream()
    Console.withOut(mockPrintStream) {
      val journeys = List.empty[ProcessDataFile.JourneyMetadata]
      ProcessDataFile.task1(journeys, 90.0)
    }
    val output = mockPrintStream.toString.trim
    output should include("Journeys of 90.0 minutes or more:")
    output should include("No journeys matching the duration criteria.")
  }


  // Test case for task2 with journeys
  "task2" should "print average speed per journey in kph" in {
    val mockPrintStream = new java.io.ByteArrayOutputStream()
    Console.withOut(mockPrintStream) {
      val journeys = List(
        ProcessDataFile.JourneyMetadata("1", "driver1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(100), "37.7749", "-122.4194", "34.0522", "-118.2437", 100.0, 200.0, 100.0, 100.0, 6000000, 60.0),
        ProcessDataFile.JourneyMetadata("2", "driver2", LocalDateTime.now(), LocalDateTime.now().plusMinutes(50), "37.7749", "-122.4194", "34.0522", "-118.2437", 150.0, 200.0, 50.0, 50.0, 3000000, 60.0)
      )
      ProcessDataFile.task2(journeys)
    }
    val output = mockPrintStream.toString.trim
    output should include("Average speed per journey in kph:")
    output should include("journeyId: 1 driver1 distance 100.0 durationMS 6000000 avgSpeed in kph was 60.0")
    output should include("journeyId: 2 driver2 distance 50.0 durationMS 3000000 avgSpeed in kph was 60.0")
  }

  // Test case for task2 with no journeys
  it should "print no journeys for average speed calculation" in {
    val mockPrintStream = new java.io.ByteArrayOutputStream()
    Console.withOut(mockPrintStream) {
      val journeys = List.empty[ProcessDataFile.JourneyMetadata]
      ProcessDataFile.task2(journeys)
    }
    val output = mockPrintStream.toString.trim
    output should include("Average speed per journey in kph:")
    output should include("No journeys found.")
  }


  // Mocking example for testing task3
  "task3" should "return total mileage by driver for the whole day" in {
    val mockPrintStream = new java.io.ByteArrayOutputStream()
    Console.withOut(mockPrintStream) {
      val journeys = List(
        ProcessDataFile.JourneyMetadata("1", "driver1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(100), "37.7749", "-122.4194", "34.0522", "-118.2437", 100.0, 200.0, 100.0, 100.0, 6000000, 60.0),
        ProcessDataFile.JourneyMetadata("2", "driver2", LocalDateTime.now(), LocalDateTime.now().plusMinutes(50), "37.7749", "-122.4194", "34.0522", "-118.2437", 150.0, 200.0, 50.0, 50.0, 3000000, 60.0),
        ProcessDataFile.JourneyMetadata("3", "driver1", LocalDateTime.now(), LocalDateTime.now().plusMinutes(120), "37.7749", "-122.4194", "34.0522", "-118.2437", 200.0, 300.0, 100.0, 120.0, 7200000, 50.0)
      )
      val result = ProcessDataFile.task3(journeys)
      result should have size 2
      result("driver1") shouldBe 200.0
      result("driver2") shouldBe 50.0
    }
    val output = mockPrintStream.toString.trim
    output should include("Total mileage by driver for the whole day:")
    output should include("driver1 drove 200.0 kilometers")
    output should include("driver2 drove 50.0 kilometers")
  }

  it should "return empty map when no journeys provided" in {
    val mockPrintStream = new java.io.ByteArrayOutputStream()
    Console.withOut(mockPrintStream) {
      val journeys = List.empty[ProcessDataFile.JourneyMetadata]
      val result = ProcessDataFile.task3(journeys)
      result shouldBe empty
    }
    val output = mockPrintStream.toString.trim
    output should include("Total mileage by driver for the whole day:")
    output should not include "kilometers"
  }


  // Test case for task4 with journeys
  "task4" should "print most active driver based on mileage" in {
    val mockPrintStream = new java.io.ByteArrayOutputStream()
    Console.withOut(mockPrintStream) {
      val groupedMileage = Map(
        "driver1" -> 200.0,
        "driver2" -> 50.0,
        "driver3" -> 100.0
      )
      ProcessDataFile.task4(groupedMileage)
    }
    val output = mockPrintStream.toString.trim
    output should include("Most active driver - the driver who has driven the most kilometers:")
    output should include("Most active driver is driver1")
  }

  // Test case for task4 with no drivers
  it should "print no journeys found when no drivers provided" in {
    val mockPrintStream = new java.io.ByteArrayOutputStream()
    Console.withOut(mockPrintStream) {
      val groupedMileage = Map.empty[String, Double]
      ProcessDataFile.task4(groupedMileage)
    }
    val output = mockPrintStream.toString.trim
    output should include("Most active driver - the driver who has driven the most kilometers:")
    output should include("No journeys found.")
  }
  
}