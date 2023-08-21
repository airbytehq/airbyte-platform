/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.controllers;

import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;

import io.airbyte.connector_builder.api.generated.V1Api;
import io.airbyte.connector_builder.api.model.generated.HealthCheckRead;
import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.api.model.generated.ResolveManifestRequestBody;
import io.airbyte.connector_builder.api.model.generated.StreamRead;
import io.airbyte.connector_builder.api.model.generated.StreamReadRequestBody;
import io.airbyte.connector_builder.handlers.HealthHandler;
import io.airbyte.connector_builder.handlers.ResolveManifestHandler;
import io.airbyte.connector_builder.handlers.StreamHandler;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

/**
 * Micronaut controller that defines the behavior for all endpoints related to building and testing
 * low-code connectors using the Connector Builder from the Airbyte web application.
 */
@Controller("/v1")
@Context
public class ConnectorBuilderController implements V1Api {

  private final HealthHandler healthHandler;
  private final StreamHandler streamHandler;
  private final ResolveManifestHandler resolveManifestHandler;

  public ConnectorBuilderController(final HealthHandler healthHandler,
                                    final ResolveManifestHandler resolveManifestHandler,
                                    final StreamHandler streamHandler) {
    this.healthHandler = healthHandler;
    this.streamHandler = streamHandler;
    this.resolveManifestHandler = resolveManifestHandler;
  }

  @Override
  @Get(uri = "/health",
       produces = MediaType.APPLICATION_JSON)
  @Secured(SecurityRule.IS_ANONYMOUS)
  @ExecuteOn(TaskExecutors.IO)
  public HealthCheckRead getHealthCheck() {
    return healthHandler.getHealthCheck();
  }

  @Override
  @Post(uri = "/stream/read",
        produces = MediaType.APPLICATION_JSON)
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(TaskExecutors.IO)
  public StreamRead readStream(final StreamReadRequestBody streamReadRequestBody) {
    return streamHandler.readStream(streamReadRequestBody);
  }

  @Override
  @Post(uri = "/manifest/resolve",
        produces = MediaType.APPLICATION_JSON)
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(TaskExecutors.IO)
  public ResolveManifest resolveManifest(final ResolveManifestRequestBody resolveManifestRequestBody) {
    return resolveManifestHandler.resolveManifest(resolveManifestRequestBody);
  }

}
