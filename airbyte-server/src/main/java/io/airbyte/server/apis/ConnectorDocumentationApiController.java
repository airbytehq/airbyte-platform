/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.ConnectorDocumentationApi;
import io.airbyte.api.model.generated.ConnectorDocumentationRead;
import io.airbyte.api.model.generated.ConnectorDocumentationRequestBody;
import io.airbyte.commons.auth.SecuredWorkspace;
import io.airbyte.commons.server.handlers.ConnectorDocumentationHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/connector_documentation")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
@SuppressWarnings("MissingJavadocType")
public class ConnectorDocumentationApiController implements ConnectorDocumentationApi {

  private final ConnectorDocumentationHandler connectorDocumentationHandler;

  public ConnectorDocumentationApiController(final ConnectorDocumentationHandler connectorDocumentationHandler) {
    this.connectorDocumentationHandler = connectorDocumentationHandler;
  }

  @Override
  @Post(uri = "/get")
  @Status(HttpStatus.OK)
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectorDocumentationRead getConnectorDocumentation(ConnectorDocumentationRequestBody connectorDocumentationRequestBody) {
    return ApiHelper.execute(() -> connectorDocumentationHandler.getConnectorDocumentation(connectorDocumentationRequestBody));
  }

}
