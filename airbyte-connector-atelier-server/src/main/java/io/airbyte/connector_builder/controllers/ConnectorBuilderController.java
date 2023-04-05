/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.controllers;

import io.airbyte.connector_builder.api.generated.V1Api;
import io.airbyte.connector_builder.api.model.generated.HealthCheckRead;
import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.api.model.generated.ResolveManifestRequestBody;
import io.airbyte.connector_builder.api.model.generated.StreamRead;
import io.airbyte.connector_builder.api.model.generated.StreamReadRequestBody;
import io.airbyte.connector_builder.api.model.generated.StreamReadSlicesInner;
import io.airbyte.connector_builder.api.model.generated.StreamReadSlicesInnerPagesInner;
import io.airbyte.connector_builder.api.model.generated.StreamsListRead;
import io.airbyte.connector_builder.api.model.generated.StreamsListRequestBody;
import io.airbyte.connector_builder.handlers.HealthHandler;
import io.airbyte.connector_builder.handlers.ResolveManifestHandler;
import io.airbyte.connector_builder.handlers.StreamsHandler;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import java.util.HashMap;
import java.util.List;

/**
 * Micronaut controller that defines the behavior for all endpoints related to building and testing
 * low-code connectors using the Connector Builder from the Airbyte web application.
 */
@Controller("/v1")
@Context
public class ConnectorBuilderController implements V1Api {

  private final HealthHandler healthHandler;
  private final ResolveManifestHandler resolveManifestHandler;
  private final StreamsHandler streamsHandler;

  public ConnectorBuilderController(final HealthHandler healthHandler,
                                    final ResolveManifestHandler resolveManifestHandler,
                                    final StreamsHandler streamsHandler) {
    this.healthHandler = healthHandler;
    this.resolveManifestHandler = resolveManifestHandler;
    this.streamsHandler = streamsHandler;
  }

  @Override
  @Get(uri = "/health",
       produces = MediaType.APPLICATION_JSON)
  @ExecuteOn(TaskExecutors.IO)
  public HealthCheckRead getHealthCheck() {
    return healthHandler.getHealthCheck();
  }

  @Override
  @Post(uri = "/streams/list",
        produces = MediaType.APPLICATION_JSON)
  @ExecuteOn(TaskExecutors.IO)
  public StreamsListRead listStreams(final StreamsListRequestBody streamsListRequestBody) {
    return streamsHandler.listStreams(streamsListRequestBody);
  }

  @Override
  @Post(uri = "/stream/read",
        produces = MediaType.APPLICATION_JSON)
  @ExecuteOn(TaskExecutors.IO)
  public StreamRead readStream(final StreamReadRequestBody streamReadRequestBody) {
    final HashMap<String, String> recordOne = new HashMap<>();
    recordOne.put("name", "Joel Miller");
    final HashMap<String, String> recordTwo = new HashMap<>();
    recordTwo.put("name", "Ellie Williams");

    final StreamReadSlicesInnerPagesInner pages = new StreamReadSlicesInnerPagesInner();
    pages.setRecords(List.of(recordOne, recordTwo));
    final StreamReadSlicesInner slices = new StreamReadSlicesInner();
    slices.setPages(List.of(pages));
    final StreamRead readResponse = new StreamRead();
    readResponse.setSlices(List.of(slices));
    return readResponse;
  }

  @Override
  @Post(uri = "/manifest/resolve",
        produces = MediaType.APPLICATION_JSON)
  @ExecuteOn(TaskExecutors.IO)
  public ResolveManifest resolveManifest(final ResolveManifestRequestBody resolveManifestRequestBody) {
    return resolveManifestHandler.resolveManifest(resolveManifestRequestBody);
  }

}
