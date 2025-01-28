/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.requester;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.airbyte.connector_builder.api.model.generated.StreamRead;
import io.airbyte.connector_builder.api.model.generated.StreamReadAuxiliaryRequestsInner;
import io.airbyte.connector_builder.api.model.generated.StreamReadLogsInner;
import io.airbyte.connector_builder.api.model.generated.StreamReadSlicesInner;
import io.airbyte.connector_builder.command_runner.SynchronousCdkCommandRunner;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class AirbyteCdkRequesterImplTest {

  private static final String READ_STREAM_COMMAND = "test_read";
  private static final JsonNode A_CONFIG;
  private static final JsonNode A_MANIFEST;
  private static final List<JsonNode> A_STATE;
  private static final String A_STREAM = "test";
  private static final Integer A_LIMIT = 1;

  static {
    try {
      A_CONFIG = new ObjectMapper().readTree("{\"config\": 1}");
      A_MANIFEST = new ObjectMapper().readTree("{\"manifest\": 1}");
      A_STATE = Collections.singletonList(new ObjectMapper().readTree("{\"key\": \"value\"}"));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private SynchronousCdkCommandRunner commandRunner;
  private AirbyteCdkRequesterImpl requester;

  @BeforeEach
  void setUp() {
    commandRunner = mock(SynchronousCdkCommandRunner.class);
    requester = new AirbyteCdkRequesterImpl(commandRunner);
  }

  ArgumentCaptor<String> testReadStreamSuccess(final Integer recordLimit, final Integer pageLimit, final Integer sliceLimit) throws Exception {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());

    final JsonNode response = mapper.readTree(
        "{\"test_read_limit_reached\": true, \"logs\":[{\"message\":\"log message1\", \"level\":\"INFO\"}, {\"message\":\"log message2\", "
            + "\"level\":\"INFO\"}], \"slices\": [{\"pages\": [{\"records\": [{\"record\": 1}]}], \"slice_descriptor\": {\"startDatetime\": "
            + "\"2023-11-01T00:00:00+00:00\", \"listItem\": \"item\"}, \"state\": [{\"airbyte\": \"state\"}]}, {\"pages\": []}],"
            + "\"inferred_schema\": {\"schema\": 1}, \"latest_config_update\": { \"config_key\": \"config_value\"},"
            + "\"auxiliary_requests\": [{\"title\": \"Refresh token\",\"description\": \"Obtains access token\",\"request\": {\"url\": "
            + "\"https://a-url.com/oauth2/v1/tokens/bearer\",\"headers\": {\"Content-Type\": "
            + "\"application/x-www-form-urlencoded\"},\"http_method\": \"POST\",\"body\": \"a_request_body\"},\"response\": {\"status\": 200,"
            + "\"body\": \"a_response_body\",\"headers\": {\"Date\": \"Tue, 11 Jul 2023 16:28:10 GMT\"}}}]}");
    final ArgumentCaptor<String> configCaptor = ArgumentCaptor.forClass(String.class);
    when(commandRunner.runCommand(eq(READ_STREAM_COMMAND), configCaptor.capture(), any(), any()))
        .thenReturn(new AirbyteRecordMessage().withData(response));

    final StreamRead streamRead = requester.readStream(A_MANIFEST, A_CONFIG, A_STATE, A_STREAM, recordLimit, pageLimit, sliceLimit);

    final boolean testReadLimitReached = mapper.convertValue(response.get("test_read_limit_reached"), new TypeReference<>() {});
    assertEquals(testReadLimitReached, streamRead.getTestReadLimitReached());

    assertEquals(2, streamRead.getSlices().size());
    final List<StreamReadSlicesInner> slices = mapper.convertValue(response.get("slices"), new TypeReference<>() {});
    assertEquals(slices, streamRead.getSlices());

    assertEquals(2, streamRead.getLogs().size());
    final List<StreamReadLogsInner> logs = mapper.convertValue(response.get("logs"), new TypeReference<>() {});
    assertEquals(logs, streamRead.getLogs());

    final List<StreamReadAuxiliaryRequestsInner> auxiliaryRequests = mapper.convertValue(
        response.get("auxiliary_requests"), new TypeReference<>() {});
    assertEquals(auxiliaryRequests, streamRead.getAuxiliaryRequests());

    return configCaptor;
  }

  @Test
  void whenReadStreamWithLimitsThenReturnAdaptedCommandRunnerResponse() throws Exception {
    // If all test read limits are present, all are passed along in command config
    final ArgumentCaptor<String> configCaptor = testReadStreamSuccess(A_LIMIT, A_LIMIT, A_LIMIT);
    assertRunCommandArgs(configCaptor, READ_STREAM_COMMAND, A_LIMIT, A_LIMIT, A_LIMIT);

    // Test read limits which are not present will not be in the adapted command config
    final ArgumentCaptor<String> noRecordLimitConfigCaptor = testReadStreamSuccess(null, A_LIMIT, A_LIMIT);
    assertRunCommandArgs(noRecordLimitConfigCaptor, READ_STREAM_COMMAND, null, A_LIMIT, A_LIMIT);

    final ArgumentCaptor<String> noPageLimitConfigCaptor = testReadStreamSuccess(A_LIMIT, null, A_LIMIT);
    assertRunCommandArgs(noPageLimitConfigCaptor, READ_STREAM_COMMAND, A_LIMIT, null, A_LIMIT);

    final ArgumentCaptor<String> noSliceLimitConfigCaptor = testReadStreamSuccess(A_LIMIT, A_LIMIT, null);
    assertRunCommandArgs(noSliceLimitConfigCaptor, READ_STREAM_COMMAND, A_LIMIT, A_LIMIT, null);

    // If any of the test read limits get special handling, it may be worth validating a
    // more exhaustive set of permutations, but for now this should be plenty
    final ArgumentCaptor<String> onlyPageLimitConfigCaptor = testReadStreamSuccess(null, A_LIMIT, null);
    assertRunCommandArgs(onlyPageLimitConfigCaptor, READ_STREAM_COMMAND, null, A_LIMIT, null);
  }

  @Test
  void whenReadStreamWithExcessiveLimitsThenThrowException() throws Exception {
    assertThrows(AirbyteCdkInvalidInputException.class, () -> testReadStreamSuccess(requester.maxRecordLimit + 1, A_LIMIT, A_LIMIT));
    assertThrows(AirbyteCdkInvalidInputException.class, () -> testReadStreamSuccess(A_LIMIT, requester.maxPageLimit + 1, A_LIMIT));
    assertThrows(AirbyteCdkInvalidInputException.class, () -> testReadStreamSuccess(A_LIMIT, A_LIMIT, requester.maxSliceLimit + 1));
  }

  @Test
  void whenReadStreamWithoutLimitThenReturnAdaptedCommandRunnerResponse() throws Exception {
    final ArgumentCaptor<String> configCaptor = testReadStreamSuccess(null, null, null);
    assertRunCommandArgs(configCaptor, READ_STREAM_COMMAND, null, null, null);
  }

  @Test
  void givenStreamIsNullWhenReadStreamThenThrowException() throws Exception {
    when(commandRunner.runCommand(eq(READ_STREAM_COMMAND), any(), any(), any()))
        .thenReturn(
            new AirbyteRecordMessage().withData(new ObjectMapper().readTree("{\"streams\":[{\"name\": \"missing stream\", \"stream\": null}]}")));
    assertThrows(AirbyteCdkInvalidInputException.class, () -> requester.readStream(A_MANIFEST, A_CONFIG, A_STATE, null, A_LIMIT, A_LIMIT, A_LIMIT));
  }

  @Test
  void whenStateIsNotNullAdaptStateConvertsItDirectlyToString() throws IOException {
    String adaptedState = requester.adaptState(A_STATE);
    assertEquals("""
                 [ {
                   "key" : "value"
                 } ]""", adaptedState);
  }

  @Test
  void whenStateIsNullAdaptStateReturnsAnEmptyArray() throws IOException {
    String adaptedState = requester.adaptState(null);
    assertEquals("[ ]", adaptedState);
  }

  void assertRunCommandArgs(final ArgumentCaptor<String> configCaptor, final String command) throws Exception {
    // assert runCommand arguments: We are doing this because the `runCommand` received a JSON as a
    // string and we don't care about the variations in formatting
    // to make this test flaky. Casting the string passed to `runCommand` as a JSON will remove this
    // flakiness
    final JsonNode config = A_CONFIG.deepCopy();
    ((ObjectNode) config).put("__command", command);
    ((ObjectNode) config).set("__injected_declarative_manifest", A_MANIFEST);
    assertEquals(config, new ObjectMapper().readTree(configCaptor.getValue()));
  }

  void assertRunCommandArgs(final ArgumentCaptor<String> configCaptor,
                            final String command,
                            final Integer recordLimit,
                            final Integer pageLimit,
                            final Integer sliceLimit)
      throws Exception {
    final JsonNode config = A_CONFIG.deepCopy();
    ((ObjectNode) config).put("__command", command);
    ((ObjectNode) config).set("__injected_declarative_manifest", A_MANIFEST);
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode commandConfig = mapper.createObjectNode();
    if (recordLimit != null) {
      commandConfig.put("max_records", recordLimit);
    }
    if (pageLimit != null) {
      commandConfig.put("max_pages_per_slice", pageLimit);
    }
    if (sliceLimit != null) {
      commandConfig.put("max_slices", sliceLimit);
    }
    ((ObjectNode) config).set("__test_read_config", commandConfig);
    assertEquals(config, new ObjectMapper().readTree(configCaptor.getValue()));
  }

}
