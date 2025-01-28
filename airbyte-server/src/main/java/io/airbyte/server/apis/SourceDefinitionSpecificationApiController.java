/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;

import io.airbyte.api.generated.SourceDefinitionSpecificationApi;
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/source_definition_specifications")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class SourceDefinitionSpecificationApiController implements SourceDefinitionSpecificationApi {

  private final ConnectorDefinitionSpecificationHandler connectorDefinitionSpecificationHandler;

  public SourceDefinitionSpecificationApiController(final ConnectorDefinitionSpecificationHandler connectorDefinitionSpecificationHandler) {
    this.connectorDefinitionSpecificationHandler = connectorDefinitionSpecificationHandler;
  }

  @SuppressWarnings("LineLength")
  @Post("/get")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceDefinitionSpecificationRead getSourceDefinitionSpecification(@Body final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId) {
    return ApiHelper.execute(() -> connectorDefinitionSpecificationHandler.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId));
  }

  @Post("/get_for_source")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceDefinitionSpecificationRead getSpecificationForSourceId(@Body final SourceIdRequestBody sourceIdRequestBody) {
    return ApiHelper.execute(() -> connectorDefinitionSpecificationHandler.getSpecificationForSourceId(sourceIdRequestBody));
  }

}
