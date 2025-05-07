/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.connectorbuilder.api.model.generated.StreamRead;
import io.airbyte.connectorbuilder.api.model.generated.StreamReadLogsInner;
import io.airbyte.connectorbuilder.api.model.generated.StreamReadRequestBody;
import io.airbyte.connectorbuilder.api.model.generated.StreamReadSlicesInner;
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connectorbuilder.exceptions.ConnectorBuilderException;
import io.airbyte.connectorbuilder.requester.AirbyteCdkRequester;
import java.io.IOException;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StreamHandlerTest {

  private static final String jsonLogs = """
                                         [
                                           {
                                             "message":"slice:{}",
                                             "level":"INFO"
                                           }
                                         ]""";
  private static final String jsonSlice = """
                                          [
                                            {
                                              "pages": [
                                                {
                                                  "records": []
                                                }
                                              ]
                                            }
                                          ]
                                          """;
  final ObjectMapper objectMapper = new ObjectMapper();
  List<StreamReadLogsInner> logs = objectMapper.readValue(jsonLogs, new TypeReference<List<StreamReadLogsInner>>() {});
  List<StreamReadSlicesInner> slices = objectMapper.readValue(jsonSlice, new TypeReference<List<StreamReadSlicesInner>>() {});

  private final StreamRead streamRead = new StreamRead().logs(logs).slices(slices);
  private static final JsonNode A_CONFIG;
  private static final JsonNode A_MANIFEST;
  private static final List<JsonNode> A_STATE;
  private static final String A_STREAM = "test";
  private static final Integer A_LIMIT = 1;

  static {
    try {
      A_CONFIG = new ObjectMapper().readTree("{\"config\": 1}");
      A_MANIFEST = new ObjectMapper().readTree("{\"manifest\": 1}");
      A_STATE = Lists.newArrayList(new ObjectMapper().readTree("{}"));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private AirbyteCdkRequester requester;
  private StreamHandler handler;

  StreamHandlerTest() throws JsonProcessingException {}

  @BeforeEach
  void setUp() {
    requester = mock(AirbyteCdkRequester.class);
    handler = new StreamHandler(requester);
  }

  @Test
  void whenReadStreamThenReturnRequesterResponse() throws Exception {
    when(requester.readStream(A_MANIFEST, null, A_CONFIG, A_STATE, A_STREAM, A_LIMIT, A_LIMIT, A_LIMIT)).thenReturn(streamRead);
    final StreamRead response =
        handler.readStream(
            new StreamReadRequestBody().manifest(A_MANIFEST).config(A_CONFIG).state(A_STATE).stream(A_STREAM).recordLimit(A_LIMIT).pageLimit(A_LIMIT)
                .sliceLimit(A_LIMIT));
    assertEquals(streamRead, response);
  }

  @Test
  void givenIOExceptionWhenReadStreamThenRaiseConnectorBuilderException() throws Exception {
    when(requester.readStream(A_MANIFEST, null, A_CONFIG, A_STATE, A_STREAM, A_LIMIT, A_LIMIT, A_LIMIT)).thenThrow(IOException.class);
    assertThrows(ConnectorBuilderException.class,
        () -> handler.readStream(
            new StreamReadRequestBody()
                .manifest(A_MANIFEST)
                .config(A_CONFIG)
                .stream(A_STREAM)
                .state(A_STATE)
                .recordLimit(A_LIMIT)
                .pageLimit(A_LIMIT)
                .sliceLimit(A_LIMIT)));
  }

  @Test
  void givenAirbyteCdkInvalidInputExceptionWhenReadStreamThenRaiseConnectorBuilderException() throws Exception {
    when(requester.readStream(A_MANIFEST, null, A_CONFIG, A_STATE, A_STREAM, A_LIMIT, A_LIMIT, A_LIMIT))
        .thenThrow(AirbyteCdkInvalidInputException.class);
    assertThrows(AirbyteCdkInvalidInputException.class,
        () -> handler.readStream(
            new StreamReadRequestBody()
                .manifest(A_MANIFEST)
                .config(A_CONFIG)
                .stream(A_STREAM)
                .state(A_STATE)
                .recordLimit(A_LIMIT)
                .pageLimit(A_LIMIT)
                .sliceLimit(A_LIMIT)));
  }

}
