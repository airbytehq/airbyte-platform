/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;

import io.airbyte.api.generated.DestinationDefinitionSpecificationApi;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/destination_definition_specifications")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class DestinationDefinitionSpecificationApiController implements DestinationDefinitionSpecificationApi {

  private final ConnectorDefinitionSpecificationHandler connectorDefinitionSpecificationHandler;

  public DestinationDefinitionSpecificationApiController(final ConnectorDefinitionSpecificationHandler connectorDefinitionSpecificationHandler) {
    this.connectorDefinitionSpecificationHandler = connectorDefinitionSpecificationHandler;
  }

  @SuppressWarnings("LineLength")
  @Post("/get")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public DestinationDefinitionSpecificationRead getDestinationDefinitionSpecification(@Body final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId) {
    return ApiHelper.execute(() -> connectorDefinitionSpecificationHandler.getDestinationSpecification(destinationDefinitionIdWithWorkspaceId));
  }

  @Post("/get_for_destination")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public DestinationDefinitionSpecificationRead getSpecificationForDestinationId(@Body final DestinationIdRequestBody destinationIdRequestBody) {
    return ApiHelper.execute(() -> connectorDefinitionSpecificationHandler.getSpecificationForDestinationId(destinationIdRequestBody));
  }

}
