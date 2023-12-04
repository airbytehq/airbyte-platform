/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.generated.DeploymentMetadataApi;
import io.airbyte.api.model.generated.DeploymentMetadataRead;
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.annotation.Controller;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/deployment/metadata")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
public class DeploymentMetadataApiController implements DeploymentMetadataApi {

  private final DeploymentMetadataHandler deploymentMetadataHandler;

  public DeploymentMetadataApiController(final DeploymentMetadataHandler deploymentMetadataHandler) {
    this.deploymentMetadataHandler = deploymentMetadataHandler;
  }

  @Override
  public DeploymentMetadataRead getDeploymentMetadata() {
    return ApiHelper.execute(() -> deploymentMetadataHandler.getDeploymentMetadata());
  }

}
