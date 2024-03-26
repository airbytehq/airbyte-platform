/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.constants.AirbyteSecretConstants;
import io.airbyte.commons.json.Jsons;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.Message;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link MaskedDataInterceptor} Log4j rewrite policy.
 */
class MaskedDataInterceptorTest {

  private static final String FOO = "foo";
  private static final String OTHER = "other";
  private static final String JSON_WITH_STRING_SECRETS = "{\"" + FOO + "\":\"test\",\"" + OTHER + "\":{\"prop\":\"value\",\"bar\":\"1234\"}}";
  private static final String JSON_WITH_STRING_WITH_QUOTE_SECRETS =
      "{\"" + FOO + "\":\"\\\"test\\\"\",\"" + OTHER + "\":{\"prop\":\"value\",\"bar\":\"1234\"}}";
  private static final String JSON_WITH_NUMBER_SECRETS = "{\"" + FOO + "\":\"test\",\"" + OTHER + "\":{\"prop\":\"value\",\"bar\":1234}}";
  private static final String JSON_WITHOUT_SECRETS = "{\"prop1\":\"test\",\"" + OTHER + "\":{\"prop2\":\"value\",\"prop3\":1234}}";
  public static final String TEST_SPEC_SECRET_MASK_YAML = "/test_spec_secret_mask.yaml";

  public static final String TEST_LOGGED_RECORD_CONTENTS =
      "2024-03-21 12:19:08 \u001B[43mdestination\u001B[0m > ERROR i.a.c.i.b.Destination$ShimToSerializedAirbyteMessageConsumer(consumeMessage):120 "
          + "Received invalid message: {\"type\":\"RECORD\",\"record\":{\"namespace\":\"";
  public static final String REDACTED_LOGGED_RECORD_CONTENTS =
      "2024-03-21 12:19:08 \u001B[43mdestination\u001B[0m > ERROR i.a.c.i.b.Destination$ShimToSerializedAirbyteMessageConsumer(consumeMessage):120 "
          + "Received invalid message:"
          + AirbyteSecretConstants.SECRETS_MASK;
  public static final String TEST_LOGGED_SQL_VALUES =
      "2024-03-19 20:03:43 \u001B[43mdestination\u001B[0m > ERROR pool-4-thread-1 i.a.c.i.d.a.FlushWorkers(flush$lambda$6):192 Flush Worker (632c9) "
          + "-- flush worker "
          + "error: java.lang.RuntimeException: org.jooq.exception.DataAccessException: SQL [insert into "
          + "\"airbyte_internal\".\"public_raw__stream_foo\" (_airbyte_raw_id, _airbyte_data, _airbyte_meta, _airbyte_extracted_at, "
          + "_airbyte_loaded_at) values ('UUID', a bunch of other stuff";

  public static final String REDACTED_LOGGED_SQL_VALUES =
      "2024-03-19 20:03:43 \u001B[43mdestination\u001B[0m > ERROR pool-4-thread-1 i.a.c.i.d.a.FlushWorkers(flush$lambda$6):192 Flush Worker (632c9) "
          + "-- flush worker "
          + "error: java.lang.RuntimeException: org.jooq.exception.DataAccessException: SQL [insert into "
          + "\"airbyte_internal\".\"public_raw__stream_foo\" (_airbyte_raw_id, _airbyte_data, _airbyte_meta, _airbyte_extracted_at, "
          + "_airbyte_loaded_at) values ("
          + AirbyteSecretConstants.SECRETS_MASK;

  @Test
  void testMaskingMessageWithStringSecret() {
    final Message message = mock(Message.class);
    final LogEvent logEvent = mock(LogEvent.class);
    when(message.getFormattedMessage()).thenReturn(JSON_WITH_STRING_SECRETS);
    when(logEvent.getMessage()).thenReturn(message);

    final MaskedDataInterceptor interceptor = MaskedDataInterceptor.createPolicy(TEST_SPEC_SECRET_MASK_YAML);

    final LogEvent result = interceptor.rewrite(logEvent);

    final JsonNode json = Jsons.deserialize(result.getMessage().getFormattedMessage());
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(FOO).asText());
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(OTHER).get("bar").asText());
  }

  @Test
  void testMaskingMessageWithStringSecretWithQuotes() {
    final Message message = mock(Message.class);
    final LogEvent logEvent = mock(LogEvent.class);
    when(message.getFormattedMessage()).thenReturn(JSON_WITH_STRING_WITH_QUOTE_SECRETS);
    when(logEvent.getMessage()).thenReturn(message);

    final MaskedDataInterceptor interceptor = MaskedDataInterceptor.createPolicy(TEST_SPEC_SECRET_MASK_YAML);
    final LogEvent result = interceptor.rewrite(logEvent);

    final JsonNode json = Jsons.deserialize(result.getMessage().getFormattedMessage());
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(FOO).asText());
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(OTHER).get("bar").asText());
  }

  @Test
  void testMaskingMessageWithNumberSecret() {
    final Message message = mock(Message.class);
    final LogEvent logEvent = mock(LogEvent.class);
    when(message.getFormattedMessage()).thenReturn(JSON_WITH_NUMBER_SECRETS);
    when(logEvent.getMessage()).thenReturn(message);

    final MaskedDataInterceptor interceptor = MaskedDataInterceptor.createPolicy(TEST_SPEC_SECRET_MASK_YAML);

    final LogEvent result = interceptor.rewrite(logEvent);

    final JsonNode json = Jsons.deserialize(result.getMessage().getFormattedMessage());
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(FOO).asText());
    assertEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(OTHER).get("bar").asText());
  }

  @Test
  void testMaskingMessageWithoutSecret() {
    final Message message = mock(Message.class);
    final LogEvent logEvent = mock(LogEvent.class);
    when(message.getFormattedMessage()).thenReturn(JSON_WITHOUT_SECRETS);
    when(logEvent.getMessage()).thenReturn(message);

    final MaskedDataInterceptor interceptor = MaskedDataInterceptor.createPolicy(TEST_SPEC_SECRET_MASK_YAML);

    final LogEvent result = interceptor.rewrite(logEvent);

    final JsonNode json = Jsons.deserialize(result.getMessage().getFormattedMessage());
    assertNotEquals(AirbyteSecretConstants.SECRETS_MASK, json.get("prop1").asText());
    assertNotEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(OTHER).get("prop2").asText());
    assertNotEquals(AirbyteSecretConstants.SECRETS_MASK, json.get(OTHER).get("prop3").asText());
  }

  @Test
  void testMaskingMessageThatDoesNotMatchPattern() {
    final String actualMessage = "This is some log message that doesn't match the pattern.";
    final Message message = mock(Message.class);
    final LogEvent logEvent = mock(LogEvent.class);
    when(message.getFormattedMessage()).thenReturn(actualMessage);
    when(logEvent.getMessage()).thenReturn(message);

    final MaskedDataInterceptor interceptor = MaskedDataInterceptor.createPolicy(TEST_SPEC_SECRET_MASK_YAML);

    final LogEvent result = interceptor.rewrite(logEvent);
    assertFalse(result.getMessage().getFormattedMessage().contains(AirbyteSecretConstants.SECRETS_MASK));
    assertEquals(actualMessage, result.getMessage().getFormattedMessage());
  }

  @Test
  void testMissingMaskingFileDoesNotPreventLogging() {
    final Message message = mock(Message.class);
    final LogEvent logEvent = mock(LogEvent.class);
    when(message.getFormattedMessage()).thenReturn(JSON_WITHOUT_SECRETS);
    when(logEvent.getMessage()).thenReturn(message);

    assertDoesNotThrow(() -> {
      final MaskedDataInterceptor interceptor = MaskedDataInterceptor.createPolicy("/does_not_exist.yaml");
      final LogEvent result = interceptor.rewrite(logEvent);
      assertEquals(JSON_WITHOUT_SECRETS, result.getMessage().getFormattedMessage());
    });
  }

  @Test
  void testMaskingMessageWithSqlValues() {
    final Message message = mock(Message.class);
    final LogEvent logEvent = mock(LogEvent.class);
    when(message.getFormattedMessage()).thenReturn(TEST_LOGGED_SQL_VALUES);
    when(logEvent.getMessage()).thenReturn(message);

    final MaskedDataInterceptor interceptor = MaskedDataInterceptor.createPolicy(TEST_SPEC_SECRET_MASK_YAML);

    final LogEvent result = interceptor.rewrite(logEvent);
    assertEquals(REDACTED_LOGGED_SQL_VALUES, result.getMessage().getFormattedMessage());
  }

  @Test
  void testMaskingMessageWithRecordContents() {
    final Message message = mock(Message.class);
    final LogEvent logEvent = mock(LogEvent.class);
    when(message.getFormattedMessage()).thenReturn(TEST_LOGGED_RECORD_CONTENTS);
    when(logEvent.getMessage()).thenReturn(message);

    final MaskedDataInterceptor interceptor = MaskedDataInterceptor.createPolicy(TEST_SPEC_SECRET_MASK_YAML);

    final LogEvent result = interceptor.rewrite(logEvent);
    assertEquals(REDACTED_LOGGED_RECORD_CONTENTS, result.getMessage().getFormattedMessage());
  }

}
