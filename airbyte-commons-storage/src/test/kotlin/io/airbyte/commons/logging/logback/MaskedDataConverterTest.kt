/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.spi.ILoggingEvent
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogSource
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private class MaskedDataConverterTest {
  private lateinit var converter: MaskedDataConverter

  @BeforeEach
  fun setup() {
    converter = MaskedDataConverter(specMaskFile = TEST_SPEC_SECRET_MASK_YAML)
  }

  @Test
  fun testMaskingMessageWithStringSecret() {
    val loggingEvent =
      mockk<ILoggingEvent> {
        every { formattedMessage } returns JSON_WITH_STRING_SECRETS
        every { mdcPropertyMap } returns emptyMap()
      }

    val result = converter.convert(event = loggingEvent)

    val json = Jsons.deserialize(result)
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(FOO).asText())
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(OTHER).get("bar").asText())
  }

  @Test
  fun testMaskingMessageWithStringSecretWithQuotes() {
    val loggingEvent =
      mockk<ILoggingEvent> {
        every { formattedMessage } returns JSON_WITH_STRING_WITH_QUOTE_SECRETS
        every { mdcPropertyMap } returns emptyMap()
      }

    val result = converter.convert(event = loggingEvent)

    val json = Jsons.deserialize(result)
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(FOO).asText())
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(OTHER).get("bar").asText())
  }

  @Test
  fun testMaskingMessageWithNumberSecret() {
    val loggingEvent =
      mockk<ILoggingEvent> {
        every { formattedMessage } returns JSON_WITH_NUMBER_SECRETS
        every { mdcPropertyMap } returns emptyMap()
      }

    val result = converter.convert(event = loggingEvent)

    val json = Jsons.deserialize(result)
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(FOO).asText())
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(OTHER).get("bar").asText())
  }

  @Test
  fun testMaskingMessageWithWithoutSecrets() {
    val loggingEvent =
      mockk<ILoggingEvent> {
        every { formattedMessage } returns JSON_WITHOUT_SECRETS
        every { mdcPropertyMap } returns emptyMap()
      }

    val result = converter.convert(event = loggingEvent)

    val json = Jsons.deserialize(result)
    assertNotEquals(AirbyteSecretConstants.SECRETS_MASK, json["prop1"].asText())
    assertNotEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(OTHER).get("prop2").asText())
    assertNotEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(OTHER).get("prop3").asText())
  }

  @Test
  fun testMaskingMessageThatDoesNotMatchPattern() {
    val message = "This is some log message that doesn't match the pattern."
    val loggingEvent =
      mockk<ILoggingEvent> {
        every { formattedMessage } returns message
        every { mdcPropertyMap } returns emptyMap()
      }

    val result = converter.convert(event = loggingEvent)

    assertFalse(result.contains(AirbyteSecretConstants.SECRETS_MASK))
    assertEquals(message, result)
  }

  @Test
  fun testMissingMaskingFileDoesNotPreventLogging() {
    val logEvent =
      mockk<ILoggingEvent> {
        every { formattedMessage } returns JSON_WITHOUT_SECRETS
        every { mdcPropertyMap } returns emptyMap()
      }

    Assertions.assertDoesNotThrow {
      val converter = MaskedDataConverter(specMaskFile = "/does_not_exist.yaml")
      val result = converter.convert(event = logEvent)
      assertEquals(JSON_WITHOUT_SECRETS, result)
    }
  }

  @Test
  fun testMaskingMessageWithSqlValues() {
    val loggingEvent =
      mockk<ILoggingEvent> {
        every { formattedMessage } returns TEST_LOGGED_SQL_VALUES
        every { mdcPropertyMap } returns LogSource.DESTINATION.toMdc()
      }

    val result = converter.convert(event = loggingEvent)

    assertEquals(REDACTED_LOGGED_SQL_VALUES, result)
  }

  @Test
  fun testMaskingMessageWithRecordContents() {
    val loggingEvent =
      mockk<ILoggingEvent> {
        every { formattedMessage } returns TEST_LOGGED_RECORD_CONTENTS
        every { mdcPropertyMap } returns LogSource.DESTINATION.toMdc()
      }

    val result = converter.convert(event = loggingEvent)

    assertEquals(REDACTED_LOGGED_RECORD_CONTENTS, result)
  }

  @Test
  fun testMaskingPlainTextLogLine() {
    val message = "500 Server Error: Internal Server Error for url: https://localhost/api/v1/search?limit=100&archived=false&hapikey=secret-key_1"
    val loggingEvent =
      mockk<ILoggingEvent> {
        every { formattedMessage } returns message
        every { mdcPropertyMap } returns emptyMap()
      }
    val result = converter.convert(event = loggingEvent)
    assertFalse(result.contains("apikey=secret-key_1"))
    assertTrue(result.contains("apikey=${AirbyteSecretConstants.SECRETS_MASK}"))
  }

  companion object {
    private const val FOO: String = "foo"
    private const val OTHER: String = "other"
    private const val JSON_WITH_STRING_SECRETS = "{\"$FOO\":\"test\",\"$OTHER\":{\"prop\":\"value\",\"bar\":\"1234\"}}"
    private const val JSON_WITH_NUMBER_SECRETS = "{\"$FOO\":\"test\",\"$OTHER\":{\"prop\":\"value\",\"bar\":1234}}"
    private const val JSON_WITH_STRING_WITH_QUOTE_SECRETS = "{\"$FOO\":\"\\\"test\\\"\",\"$OTHER\":{\"prop\":\"value\",\"bar\":\"1234\"}}"
    private const val JSON_WITHOUT_SECRETS = "{\"prop1\":\"test\",\"$OTHER\":{\"prop2\":\"value\",\"prop3\":1234}}"
    private const val REDACTED_LOGGED_SQL_VALUES =
      (
        "ERROR pool-4-thread-1 i.a.c.i.d.a.FlushWorkers(flush\$lambda$6):192 Flush Worker (632c9) -- flush worker " +
          "error: java.lang.RuntimeException: org.jooq.exception.DataAccessException: SQL [insert into " +
          "\"airbyte_internal\".\"public_raw__stream_foo\" (_airbyte_raw_id, _airbyte_data, _airbyte_meta, _airbyte_extracted_at, " +
          "_airbyte_loaded_at) values (${AirbyteSecretConstants.SECRETS_MASK}"
      )
    private const val REDACTED_LOGGED_RECORD_CONTENTS: String =
      (
        "ERROR i.a.c.i.b.Destination\$ShimToSerializedAirbyteMessageConsumer(consumeMessage):120" +
          " Received invalid message:${AirbyteSecretConstants.SECRETS_MASK}"
      )
    private const val TEST_LOGGED_SQL_VALUES: String =
      (
        "ERROR pool-4-thread-1 i.a.c.i.d.a.FlushWorkers(flush\$lambda\$6):192 Flush Worker (632c9) -- flush worker " +
          "error: java.lang.RuntimeException: org.jooq.exception.DataAccessException: SQL [insert into " +
          "\"airbyte_internal\".\"public_raw__stream_foo\" (_airbyte_raw_id, _airbyte_data, _airbyte_meta, _airbyte_extracted_at, " +
          "_airbyte_loaded_at) values ('UUID', a bunch of other stuff"
      )
    private const val TEST_LOGGED_RECORD_CONTENTS: String =
      (
        "ERROR i.a.c.i.b.Destination\$ShimToSerializedAirbyteMessageConsumer(consumeMessage):120 " +
          "Received invalid message: {\"type\":\"RECORD\",\"record\":{\"namespace\":\""
      )
    private const val TEST_SPEC_SECRET_MASK_YAML = "/test_spec_secret_mask.yaml"
  }
}
