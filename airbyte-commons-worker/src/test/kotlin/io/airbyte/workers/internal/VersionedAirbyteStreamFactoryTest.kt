/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.protocol.AirbyteMessageMigrator
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.commons.protocol.ConfiguredAirbyteCatalogMigrator
import io.airbyte.commons.protocol.migrations.AirbyteMessageMigration
import io.airbyte.commons.protocol.migrations.ConfiguredAirbyteCatalogMigration
import io.airbyte.commons.protocol.serde.AirbyteMessageV0Deserializer
import io.airbyte.commons.protocol.serde.AirbyteMessageV0Serializer
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.commons.version.Version
import io.airbyte.protocol.models.v0.AirbyteLogMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration
import io.airbyte.workers.testutils.AirbyteMessageUtils
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.mockk.CapturingSlot
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.junit.platform.commons.util.ClassLoaderUtils
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.io.StringReader
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.stream.Stream

private const val STREAM_NAME = "user_preferences"
private const val FIELD_NAME = "favorite_color"
private val VALID_MESSAGE_TEMPLATE =
  """
  {"type":"RECORD","record":{"namespace":"public","stream":"documents","data":{"value":"%s"},"emitted_at":1695224525688}}
  
  """.trimIndent()

internal class VersionedAirbyteStreamFactoryTest {
  private lateinit var serDeProvider: AirbyteMessageSerDeProvider
  private lateinit var migratorFactory: AirbyteProtocolVersionedMigratorFactory
  private lateinit var gsonPksExtractor: GsonPksExtractor
  private lateinit var mockLogger: KLogger
  private lateinit var streamFactory: VersionedAirbyteStreamFactory<Any>
  private lateinit var debugSlot: CapturingSlot<Function0<Any?>>
  private lateinit var infoSlot: CapturingSlot<Function0<Any?>>
  private lateinit var warningSlot: CapturingSlot<Function0<Any?>>

  @BeforeEach
  fun setup() {
    debugSlot = slot()
    infoSlot = slot()
    warningSlot = slot()

    gsonPksExtractor = mockk(relaxed = true)
    mockLogger =
      spyk(KotlinLogging.logger { }) {
        every { debug(message = capture(debugSlot)) } returns Unit
        every { info(message = capture(infoSlot)) } returns Unit
        every { warn(message = capture(warningSlot)) } returns Unit
      }
    serDeProvider = spyk(AirbyteMessageSerDeProvider(listOf(AirbyteMessageV0Deserializer()), listOf(AirbyteMessageV0Serializer())))
    serDeProvider.initialize()

    val airbyteMessageMigrator =
      AirbyteMessageMigrator( // TODO once data types v1 is re-enabled, this test should contain the migration
        mutableListOf<AirbyteMessageMigration<*, *>?>(),
      )
    airbyteMessageMigrator.initialize()
    val configuredAirbyteCatalogMigrator =
      ConfiguredAirbyteCatalogMigrator( // TODO once data types v1 is re-enabled, this test should contain the migration
        mutableListOf<ConfiguredAirbyteCatalogMigration<*, *>?>(),
      )
    configuredAirbyteCatalogMigrator.initialize()
    migratorFactory = spyk(AirbyteProtocolVersionedMigratorFactory(airbyteMessageMigrator, configuredAirbyteCatalogMigrator))
    streamFactory =
      VersionedAirbyteStreamFactory(
        serDeProvider = serDeProvider,
        migratorFactory = migratorFactory,
        gsonPksExtractor = gsonPksExtractor,
        invalidLineFailureConfiguration = InvalidLineFailureConfiguration(false),
        logger = mockLogger,
        metricClient = mockk(relaxed = true),
        protocolVersion = AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION,
      )
    streamFactory.initializeForProtocolVersion(AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION)
  }

  @Test
  fun testValid() {
    val record1 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "green")
    val messageStream = stringToMessageStream(Jsons.serialize(record1))
    val expectedStream = Stream.of(record1)

    assertEquals(expectedStream.toList(), messageStream.toList())
  }

  @Test
  fun testValidBigInteger() {
    val record =
      AirbyteMessageUtils.createRecordMessage(
        STREAM_NAME,
        FIELD_NAME,
        BigInteger.valueOf(Long.Companion.MAX_VALUE).add(BigInteger.ONE),
      )

    val messageStream = stringToMessageStream(Jsons.serialize(record))
    val expectedStream = Stream.of(record)

    assertEquals(expectedStream.toList(), messageStream.toList())
  }

  @Test
  fun testValidBigDecimal() {
    val record =
      AirbyteMessageUtils.createRecordMessage(
        STREAM_NAME,
        FIELD_NAME,
        BigDecimal("1234567890.1234567890"),
      )

    val messageStream = stringToMessageStream(Jsons.serialize(record))
    val expectedStream = Stream.of(record)

    assertEquals(expectedStream.toList(), messageStream.toList())
  }

  @Test
  fun testLoggingLine() {
    val invalidRecord = "invalid line"

    val messageStream = stringToMessageStream(invalidRecord)

    assertEquals(mutableListOf<Any?>(), messageStream.toList())
    verify(exactly = 2) { mockLogger.info(message = any()) }
    assertEquals(String.format(MALFORMED_NON_AIRBYTE_RECORD_LOG_MESSAGE, CONNECTION_ID_NOT_PRESENT, invalidRecord), infoSlot.captured.invoke())
  }

  @Test
  fun testLoggingLevel() {
    val expectedMessage = "warning"
    val logMessage = AirbyteMessageUtils.createLogMessage(AirbyteLogMessage.Level.WARN, expectedMessage)

    val messageStream = stringToMessageStream(Jsons.serialize(logMessage))

    assertEquals(mutableListOf<Any?>(), messageStream.toList())
    verify(exactly = 1) { mockLogger.warn(message = any()) }
    assertEquals(expectedMessage, warningSlot.captured.invoke())
  }

  @Test
  fun testFailDeserializationObvious() {
    val invalidRecord = "{ \"type\": \"abc\"}"

    val messageStream = stringToMessageStream(invalidRecord)

    assertEquals(mutableListOf<Any?>(), messageStream.toList())
    verify(exactly = 2) { mockLogger.info(message = any()) }
    assertEquals(String.format(MALFORMED_NON_AIRBYTE_RECORD_LOG_MESSAGE, CONNECTION_ID_NOT_PRESENT, invalidRecord), infoSlot.captured.invoke())
  }

  @Test
  fun testFailDeserializationSubtle() {
    val invalidRecord = "{\"type\": \"spec\", \"data\": {}}"

    val messageStream = stringToMessageStream(invalidRecord)

    assertEquals(mutableListOf<Any?>(), messageStream.toList())
    verify(exactly = 2) { mockLogger.info(message = any()) }
    assertEquals(String.format(MALFORMED_NON_AIRBYTE_RECORD_LOG_MESSAGE, CONNECTION_ID_NOT_PRESENT, invalidRecord), infoSlot.captured.invoke())
  }

  @Test
  fun testFailValidation() {
    val invalidRecord = "{ \"fish\": \"tuna\"}"

    val messageStream = stringToMessageStream(invalidRecord)

    assertEquals(mutableListOf<Any?>(), messageStream.toList())

    verify(atLeast = 1) { mockLogger.info(message = any()) }
    verify(atLeast = 1) { mockLogger.error(message = any()) }
  }

  @ParameterizedTest
  @ValueSource(
    strings = [
      // Missing closing bracket.
      "{\"type\":\"RECORD\", \"record\": {\"stream\": \"transactions\", \"data\": {\"amount\": \"100.00\"",
      // Infinity is invalid JSON.
      "{\"type\":\"RECORD\", \"record\": {\"stream\": \"transactions\", \"data\": {\"transaction_id\": Infinity }}}",
      // Infinity is invalid JSON. Python code generates JSON strings with a space.
      "{\"type\": \"RECORD\", \"record\": {\"stream\": \"transactions\", \"data\": {\"transaction_id\": Infinity }}}",
      // Infinity is invalid JSON. Lowercase types.
      "{\"type\": \"record\", \"record\": {\"stream\": \"transactions\", \"data\": {\"transaction_id\": Infinity }}}",
    ],
  )
  fun testMalformedRecordShouldOnlyDebugLog(invalidRecord: String) {
    val expected = String.format(MALFORMED_AIRBYTE_RECORD_LOG_MESSAGE, CONNECTION_ID_NOT_PRESENT, invalidRecord)
    stringToMessageStream(invalidRecord).toList()
    verifyBlankedRecordRecordWarning()
    verify(exactly = 1) { mockLogger.debug(message = any()) }
    assertEquals(expected, debugSlot.captured.invoke())
  }

  @Test
  fun testToAirbyteMessageValid() {
    val messageLine = String.format(VALID_MESSAGE_TEMPLATE, "hello")
    assertEquals(
      1,
      streamFactory
        .toAirbyteMessage(messageLine)
        .toList()
        .size,
    )
  }

  @Test
  fun testToAirbyteMessageRandomLog() {
    val randomLog = "I should not be sent on the same channel than the airbyte messages"
    val expected = String.format(MALFORMED_NON_AIRBYTE_RECORD_LOG_MESSAGE, CONNECTION_ID_NOT_PRESENT, randomLog)
    assertEquals(
      0,
      streamFactory
        .toAirbyteMessage(randomLog)
        .toList()
        .size,
    )
    verify(exactly = 1) { mockLogger.info(message = any()) }
    assertEquals(expected, infoSlot.captured.invoke())
  }

  @Test
  fun testToAirbyteMessageMixedUpRecordShouldOnlyDebugLog() {
    val messageLine = "It shouldn't be here ${String.format(VALID_MESSAGE_TEMPLATE, "hello")}"
    val expected = String.format(MALFORMED_AIRBYTE_RECORD_LOG_MESSAGE, CONNECTION_ID_NOT_PRESENT, messageLine)
    streamFactory.toAirbyteMessage(messageLine)
    verifyBlankedRecordRecordWarning()
    verify(exactly = 1) { mockLogger.debug(message = any()) }
    assertEquals(expected, debugSlot.captured.invoke())
  }

  @Test
  fun testToAirbyteMessageMixedUpRecordFailureDisable() {
    val messageLine = "It shouldn't be here ${String.format(VALID_MESSAGE_TEMPLATE, "hello")}"
    val expected = String.format(MALFORMED_AIRBYTE_RECORD_LOG_MESSAGE, CONNECTION_ID_NOT_PRESENT, messageLine)
    assertEquals(
      0,
      streamFactory
        .toAirbyteMessage(messageLine)
        .toList()
        .size,
    )
    verifyBlankedRecordRecordWarning()
    verify(exactly = 1) { mockLogger.debug(message = any()) }
    assertEquals(expected, debugSlot.captured.invoke())
  }

  @Test
  fun testToAirbyteMessageVeryLongMessageDontFail() {
    // This roughly corresponds to a 25_000_000 * 2-bytes string.
    val longStringBuilder = StringBuilder(25000000)
    for (@Suppress("UNUSED_PARAMETER")i in 0..25000000 - 1) {
      longStringBuilder.append("a")
    }
    val messageLine = String.format(VALID_MESSAGE_TEMPLATE, longStringBuilder)
    assertTrue(
      streamFactory
        .toAirbyteMessage(messageLine)
        .toList()
        .isNotEmpty(),
    )
  }

  @Test
  fun testCreate() {
    val initialVersion = Version("0.1.2")
    val bufferedReader = BufferedReader(StringReader(""))
    val streamFactory =
      VersionedAirbyteStreamFactory<Any>(
        serDeProvider = serDeProvider,
        migratorFactory = migratorFactory,
        protocolVersion = initialVersion,
        connectionId = Optional.empty(),
        configuredAirbyteCatalog = Optional.empty(),
        invalidLineFailureConfiguration = InvalidLineFailureConfiguration(false),
        gsonPksExtractor = gsonPksExtractor,
        metricClient = mockk(relaxed = true),
        logger = mockLogger,
      )
    streamFactory.create(bufferedReader)

    verify(exactly = 1) { migratorFactory.getAirbyteMessageMigrator<Any>(initialVersion) }
  }

  @Test
  fun testCreateWithVersionDetection() {
    val initialVersion = Version("0.0.0")
    val streamFactory =
      VersionedAirbyteStreamFactory<Any>(
        serDeProvider = serDeProvider,
        migratorFactory = migratorFactory,
        protocolVersion = initialVersion,
        connectionId = Optional.empty(),
        configuredAirbyteCatalog = Optional.empty(),
        invalidLineFailureConfiguration = InvalidLineFailureConfiguration(false),
        gsonPksExtractor = gsonPksExtractor,
        metricClient = mockk(relaxed = true),
        logger = mockLogger,
      ).withDetectVersion(true)

    val bufferedReader =
      getBufferedReader("version-detection/logs-with-version.jsonl")
    val stream: Stream<AirbyteMessage> = streamFactory.create(bufferedReader)

    val messageCount = stream.toList().size.toLong()
    assertEquals(1, messageCount)
  }

  @Test
  fun testCreateWithVersionDetectionFallback() {
    val initialVersion = Version("0.0.6")
    val streamFactory =
      VersionedAirbyteStreamFactory<Any>(
        serDeProvider = serDeProvider,
        migratorFactory = migratorFactory,
        protocolVersion = initialVersion,
        connectionId = Optional.empty(),
        configuredAirbyteCatalog = Optional.empty(),
        invalidLineFailureConfiguration = InvalidLineFailureConfiguration(false),
        gsonPksExtractor = gsonPksExtractor,
        metricClient = mockk(relaxed = true),
        logger = mockLogger,
      ).withDetectVersion(true)

    val bufferedReader =
      getBufferedReader("version-detection/logs-without-version.jsonl")
    val stream: Stream<AirbyteMessage> = streamFactory.create(bufferedReader)

    val messageCount = stream.toList().size.toLong()
    assertEquals(1, messageCount)
  }

  @Test
  fun testCreateWithVersionDetectionWithoutSpecMessage() {
    val initialVersion = Version("0.0.1")
    val streamFactory =
      VersionedAirbyteStreamFactory<Any>(
        serDeProvider = serDeProvider,
        migratorFactory = migratorFactory,
        protocolVersion = initialVersion,
        connectionId = Optional.empty(),
        configuredAirbyteCatalog = Optional.empty(),
        invalidLineFailureConfiguration = InvalidLineFailureConfiguration(false),
        gsonPksExtractor = gsonPksExtractor,
        metricClient = mockk(relaxed = true),
        logger = mockLogger,
      ).withDetectVersion(true)

    val bufferedReader =
      getBufferedReader("version-detection/logs-without-spec-message.jsonl")
    val stream: Stream<AirbyteMessage> = streamFactory.create(bufferedReader)

    val messageCount = stream.toList().size.toLong()
    assertEquals(2, messageCount)
  }

  private fun getBufferedReader(resourceFile: String?): BufferedReader =
    BufferedReader(
      InputStreamReader(
        ClassLoaderUtils.getDefaultClassLoader().getResourceAsStream(resourceFile)!!,
        Charset.defaultCharset(),
      ),
    )

  private fun stringToMessageStream(inputString: String): Stream<AirbyteMessage> {
    val inputStream: InputStream = ByteArrayInputStream(inputString.toByteArray(StandardCharsets.UTF_8))
    val bufferedReader = BufferedReader(InputStreamReader(inputStream, StandardCharsets.UTF_8))
    val stream = streamFactory.create(bufferedReader)
    verifyStreamHeader()
    return stream
  }

  private fun verifyBlankedRecordRecordWarning() {
    val expected = "Could not parse the string received from source, it seems to be a record message"
    verify(exactly = 1) { mockLogger.warn(message = any()) }
    assertEquals(expected, warningSlot.captured.invoke())
  }

  private fun verifyStreamHeader() {
    val expected = "Reading messages from protocol version ${AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize()}"
    verify(exactly = 1) { mockLogger.info(message = any()) }
    assertEquals(expected, infoSlot.captured.invoke())
  }
}
