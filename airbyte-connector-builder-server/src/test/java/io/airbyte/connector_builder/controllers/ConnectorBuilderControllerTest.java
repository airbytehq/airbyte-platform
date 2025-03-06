/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.handlers.AssistProxyHandler;
import io.airbyte.connector_builder.handlers.ConnectorContributionHandler;
import io.airbyte.connector_builder.handlers.HealthHandler;
import io.airbyte.connector_builder.handlers.ResolveManifestHandler;
import io.airbyte.connector_builder.handlers.StreamHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorBuilderControllerTest {

  private ConnectorBuilderController controller;
  private HealthHandler healthHandler;
  private StreamHandler streamHandler;
  private ResolveManifestHandler resolveManifestHandler;
  private StreamReadRequestBody streamReadRequestBody;
  private StreamRead streamReadResponse;
  private ResolveManifestRequestBody resolveManifestRequestBody;
  private ResolveManifest resolveManifest;
  private ConnectorContributionHandler connectorContributionHandler;
  private AssistProxyHandler assistProxyHandler;

  @BeforeEach
  void setup() {
    this.healthHandler = mock(HealthHandler.class);
    this.resolveManifestHandler = mock(ResolveManifestHandler.class);
    this.streamHandler = mock(StreamHandler.class);

    this.streamReadRequestBody = mock(StreamReadRequestBody.class);
    this.streamReadResponse = mock(StreamRead.class);
    this.resolveManifestRequestBody = mock(ResolveManifestRequestBody.class);
    this.resolveManifest = mock(ResolveManifest.class);
    this.connectorContributionHandler = mock(ConnectorContributionHandler.class);
    this.assistProxyHandler = mock(AssistProxyHandler.class);

    this.controller =
        new ConnectorBuilderController(this.healthHandler, this.resolveManifestHandler, this.streamHandler, this.connectorContributionHandler,
            this.assistProxyHandler);
  }

  @Test
  void whenReadStreamThenReturnHandlerResponse() {
    when(streamHandler.readStream(streamReadRequestBody)).thenReturn(streamReadResponse);
    final StreamRead response = this.controller.readStream(streamReadRequestBody);
    assertEquals(streamReadResponse, response);
  }

  @Test
  void givenExceptionWhenReadStreamThenThrowSameException() {
    when(streamHandler.readStream(any())).thenThrow(AirbyteCdkInvalidInputException.class);
    assertThrows(AirbyteCdkInvalidInputException.class, () -> this.controller.readStream(streamReadRequestBody));
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
