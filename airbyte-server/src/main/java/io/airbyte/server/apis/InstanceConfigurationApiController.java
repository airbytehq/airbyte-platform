/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.generated.InstanceConfigurationApi;
import io.airbyte.api.model.generated.InstanceConfigurationResponse;
import io.airbyte.api.model.generated.InstanceConfigurationSetupRequestBody;
import io.airbyte.commons.server.handlers.InstanceConfigurationHandler;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/instance_configuration")
@Secured(SecurityRule.IS_ANONYMOUS)
public class InstanceConfigurationApiController implements InstanceConfigurationApi {

  private final InstanceConfigurationHandler instanceConfigurationHandler;

  public InstanceConfigurationApiController(final InstanceConfigurationHandler instanceConfigurationHandler) {
    this.instanceConfigurationHandler = instanceConfigurationHandler;
  }

  @Override
  @Get
  public InstanceConfigurationResponse getInstanceConfiguration() {
    return ApiHelper.execute(instanceConfigurationHandler::getInstanceConfiguration);
  }

  @Override
  public InstanceConfigurationResponse setupInstanceConfiguration(InstanceConfigurationSetupRequestBody instanceConfigurationSetupRequestBody) {
    return ApiHelper.execute(() -> instanceConfigurationHandler.setupInstanceConfiguration(instanceConfigurationSetupRequestBody));
  }

}
