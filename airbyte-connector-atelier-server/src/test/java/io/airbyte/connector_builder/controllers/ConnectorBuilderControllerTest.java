/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.api.model.generated.ResolveManifestRequestBody;
import io.airbyte.connector_builder.api.model.generated.StreamRead;
import io.airbyte.connector_builder.api.model.generated.StreamReadRequestBody;
import io.airbyte.connector_builder.api.model.generated.StreamReadSlicesInner;
import io.airbyte.connector_builder.api.model.generated.StreamReadSlicesInnerPagesInner;
import io.airbyte.connector_builder.api.model.generated.StreamsListRead;
import io.airbyte.connector_builder.api.model.generated.StreamsListRequestBody;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.handlers.HealthHandler;
import io.airbyte.connector_builder.handlers.ResolveManifestHandler;
import io.airbyte.connector_builder.handlers.StreamsHandler;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorBuilderControllerTest {

  private ConnectorBuilderController controller;
  private HealthHandler healthHandler;
  private ResolveManifestHandler resolveManifestHandler;
  private StreamsHandler streamsHandler;

  private StreamsListRequestBody streamsListRequestBody;
  private StreamsListRead streamsListResponse;
  private ResolveManifestRequestBody resolveManifestRequestBody;
  private ResolveManifest resolveManifest;

  @BeforeEach
  void setup() {
    this.healthHandler = mock(HealthHandler.class);
    this.resolveManifestHandler = mock(ResolveManifestHandler.class);
    this.streamsHandler = mock(StreamsHandler.class);

    this.streamsListRequestBody = mock(StreamsListRequestBody.class);
    this.streamsListResponse = mock(StreamsListRead.class);
    this.resolveManifestRequestBody = mock(ResolveManifestRequestBody.class);
    this.resolveManifest = mock(ResolveManifest.class);

    this.controller = new ConnectorBuilderController(this.healthHandler, this.resolveManifestHandler, this.streamsHandler);
  }

  @Test
  void testReadStream() {
    final StreamRead readResponse = this.controller.readStream(new StreamReadRequestBody());

    final List<StreamReadSlicesInner> slices = readResponse.getSlices();
    assertEquals(1, slices.size());

    final List<StreamReadSlicesInnerPagesInner> pages = slices.get(0).getPages();
    assertEquals(1, pages.size());

    final List<Object> records = pages.get(0).getRecords();
    assertEquals(2, records.size());
  }

  @Test
  void whenListStreamsThenReturnHandlerResponse() {
    when(streamsHandler.listStreams(streamsListRequestBody)).thenReturn(streamsListResponse);
    final StreamsListRead response = this.controller.listStreams(streamsListRequestBody);
    assertEquals(streamsListResponse, response);
  }

  @Test
  void givenExceptionWhenListStreamsThenThrowSameException() {
    when(streamsHandler.listStreams(any())).thenThrow(AirbyteCdkInvalidInputException.class);
    assertThrows(AirbyteCdkInvalidInputException.class, () -> this.controller.listStreams(streamsListRequestBody));
  }

  @Test
  void whenResolveManifestThenReturnHandlerResponse() {
    when(resolveManifestHandler.resolveManifest(resolveManifestRequestBody)).thenReturn(resolveManifest);
    final ResolveManifest response = this.controller.resolveManifest(resolveManifestRequestBody);
    assertEquals(resolveManifest, response);
  }

  @Test
  void givenExceptionWhenResolveManifestThenThrowSameException() {
    when(resolveManifestHandler.resolveManifest(resolveManifestRequestBody)).thenThrow(AirbyteCdkInvalidInputException.class);
    assertThrows(AirbyteCdkInvalidInputException.class, () -> this.controller.resolveManifest(resolveManifestRequestBody));
  }

}
