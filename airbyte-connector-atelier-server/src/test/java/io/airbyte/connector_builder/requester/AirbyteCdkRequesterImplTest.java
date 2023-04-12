/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.requester;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.connector_builder.api.model.generated.StreamsListRead;
import io.airbyte.connector_builder.api.model.generated.StreamsListReadStreamsInner;
import io.airbyte.connector_builder.command_runner.SynchronousCdkCommandRunner;
import io.airbyte.connector_builder.exceptions.CdkProcessException;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class AirbyteCdkRequesterImplTest {

  private static final String LIST_STREAMS_COMMAND = "list_streams";
  private static final JsonNode A_CONFIG;
  private static final JsonNode A_MANIFEST;
  private static final String EMPTY_CATALOG = "";

  static {
    try {
      A_CONFIG = new ObjectMapper().readTree("{\"config\": 1}");
      A_MANIFEST = new ObjectMapper().readTree("{\"manifest\": 1}");
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

  @Test
  void whenListStreamsThenReturnAdaptedCommandRunnerResponse() throws Exception {
    final ArgumentCaptor<String> configCaptor = ArgumentCaptor.forClass(String.class);
    when(commandRunner.runCommand(eq(LIST_STREAMS_COMMAND), configCaptor.capture(), eq(EMPTY_CATALOG)))
        .thenReturn(new AirbyteRecordMessage().withData(new ObjectMapper()
            .readTree("{\"streams\":[{\"name\":\"a name\", \"url\": \"a url\"}, {\"name\":\"another name\", \"url\": \"another url\"}]}")));

    final StreamsListRead streamsListRead = requester.listStreams(A_MANIFEST, A_CONFIG);

    // assert returned object
    assertEquals(2, streamsListRead.getStreams().size());
    assertEquals(new StreamsListReadStreamsInner().name("a name").url("a url"), streamsListRead.getStreams().get(0));
    assertEquals(new StreamsListReadStreamsInner().name("another name").url("another url"), streamsListRead.getStreams().get(1));

    // assert runCommand arguments: We are doing this because the `runCommand` received a JSON as a
    // string and we don't care about the variations in formatting
    // to make this test flaky. Casting the string passed to `runCommand` as a JSON will remove this
    // flakiness
    final JsonNode listStreamsConfig = A_CONFIG.deepCopy();
    ((ObjectNode) listStreamsConfig).put("__command", LIST_STREAMS_COMMAND);
    ((ObjectNode) listStreamsConfig).set("__injected_declarative_manifest", A_MANIFEST);
    assertEquals(listStreamsConfig, new ObjectMapper().readTree(configCaptor.getValue()));
  }

  @Test
  void givenNameIsNullWhenListStreamsThenThrowException() throws Exception {
    when(commandRunner.runCommand(eq(LIST_STREAMS_COMMAND), any(), any()))
        .thenReturn(new AirbyteRecordMessage().withData(new ObjectMapper().readTree("{\"streams\":[{\"url\": \"missing name\"}]}")));
    assertThrows(CdkProcessException.class, () -> requester.listStreams(A_MANIFEST, A_CONFIG));
  }

  @Test
  void givenUrlIsNullWhenListStreamsThenThrowException() throws Exception {
    when(commandRunner.runCommand(eq(LIST_STREAMS_COMMAND), any(), any()))
        .thenReturn(new AirbyteRecordMessage().withData(new ObjectMapper().readTree("{\"streams\":[{\"name\": \"missing url\", \"url\": null}]}")));
    assertThrows(CdkProcessException.class, () -> requester.listStreams(A_MANIFEST, A_CONFIG));
  }

}
