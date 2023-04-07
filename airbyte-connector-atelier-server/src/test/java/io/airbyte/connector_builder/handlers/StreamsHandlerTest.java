/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.connector_builder.api.model.generated.StreamsListRead;
import io.airbyte.connector_builder.api.model.generated.StreamsListReadStreamsInner;
import io.airbyte.connector_builder.api.model.generated.StreamsListRequestBody;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.ConnectorBuilderException;
import io.airbyte.connector_builder.requester.AirbyteCdkRequester;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StreamsHandlerTest {

  private static final StreamsListRead STREAM_LIST_READ = new StreamsListRead().streams(
      List.of(new StreamsListReadStreamsInner().name("a name").url("a url")));
  private static final JsonNode A_CONFIG;
  private static final JsonNode A_MANIFEST;

  static {
    try {
      A_CONFIG = new ObjectMapper().readTree("{\"config\": 1}");
      A_MANIFEST = new ObjectMapper().readTree("{\"manifest\": 1}");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private AirbyteCdkRequester requester;
  private StreamsHandler handler;

  @BeforeEach
  void setUp() {
    requester = mock(AirbyteCdkRequester.class);
    handler = new StreamsHandler(requester);
  }

  @Test
  void whenListStreamsThenReturnRequesterResponse() throws Exception {
    when(requester.listStreams(A_MANIFEST, A_CONFIG)).thenReturn(STREAM_LIST_READ);
    final StreamsListRead response = handler.listStreams(new StreamsListRequestBody().manifest(A_MANIFEST).config(A_CONFIG));
    assertEquals(STREAM_LIST_READ, response);
  }

  @Test
  void givenIOExceptionWhenListStreamsThenRaiseConnectorBuilderException() throws Exception {
    when(requester.listStreams(A_MANIFEST, A_CONFIG)).thenThrow(IOException.class);
    assertThrows(ConnectorBuilderException.class, () -> handler.listStreams(new StreamsListRequestBody().manifest(A_MANIFEST).config(A_CONFIG)));
  }

  @Test
  void givenAirbyteCdkInvalidInputExceptionWhenListStreamsThenRaiseConnectorBuilderException() throws Exception {
    when(requester.listStreams(A_MANIFEST, A_CONFIG)).thenThrow(AirbyteCdkInvalidInputException.class);
    assertThrows(AirbyteCdkInvalidInputException.class,
        () -> handler.listStreams(new StreamsListRequestBody().manifest(A_MANIFEST).config(A_CONFIG)));
  }

}
