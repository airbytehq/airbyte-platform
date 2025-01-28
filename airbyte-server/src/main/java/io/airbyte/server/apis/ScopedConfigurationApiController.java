/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;

import io.airbyte.api.generated.ScopedConfigurationApi;
import io.airbyte.api.model.generated.ScopedConfigurationContextRequestBody;
import io.airbyte.api.model.generated.ScopedConfigurationContextResponse;
import io.airbyte.api.model.generated.ScopedConfigurationCreateRequestBody;
import io.airbyte.api.model.generated.ScopedConfigurationCreateResponse;
import io.airbyte.api.model.generated.ScopedConfigurationDeleteRequestBody;
import io.airbyte.api.model.generated.ScopedConfigurationDeleteResponse;
import io.airbyte.api.model.generated.ScopedConfigurationListRequestBody;
import io.airbyte.api.model.generated.ScopedConfigurationListResponse;
import io.airbyte.api.model.generated.ScopedConfigurationRead;
import io.airbyte.api.model.generated.ScopedConfigurationReadRequestBody;
import io.airbyte.api.model.generated.ScopedConfigurationReadResponse;
import io.airbyte.api.model.generated.ScopedConfigurationUpdateRequestBody;
import io.airbyte.api.model.generated.ScopedConfigurationUpdateResponse;
import io.airbyte.commons.server.handlers.ScopedConfigurationHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import java.util.UUID;

@Controller("/api/v1/scoped_configuration")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class ScopedConfigurationApiController implements ScopedConfigurationApi {

  private final ScopedConfigurationHandler scopedConfigurationHandler;

  public ScopedConfigurationApiController(final ScopedConfigurationHandler scopedConfigurationHandler) {
    this.scopedConfigurationHandler = scopedConfigurationHandler;
  }

  @SuppressWarnings("LineLength")
  @Post("/create")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ScopedConfigurationCreateResponse createScopedConfiguration(@Body final ScopedConfigurationCreateRequestBody scopedConfigurationCreateRequestBody) {
    return ApiHelper.execute(() -> {
      final ScopedConfigurationRead createdScopedConfiguration = scopedConfigurationHandler.insertScopedConfiguration(
          scopedConfigurationCreateRequestBody);

      final ScopedConfigurationCreateResponse response = new ScopedConfigurationCreateResponse();
      response.setData(createdScopedConfiguration);
      return response;
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/delete")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ScopedConfigurationDeleteResponse deleteScopedConfiguration(@Body final ScopedConfigurationDeleteRequestBody scopedConfigurationDeleteRequestBody) {
    return ApiHelper.execute(() -> {
      final UUID scopedConfigurationId = scopedConfigurationDeleteRequestBody.getScopedConfigurationId();
      scopedConfigurationHandler.deleteScopedConfiguration(scopedConfigurationId);

      return new ScopedConfigurationDeleteResponse().scopedConfigurationId(scopedConfigurationId);
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/list")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ScopedConfigurationListResponse getScopedConfigurationsList(@Body final ScopedConfigurationListRequestBody scopedConfigurationListRequestBody) {
    return ApiHelper.execute(() -> {
      final var scopedConfigurations = scopedConfigurationHandler.listScopedConfigurations(scopedConfigurationListRequestBody.getConfigKey());
      return new ScopedConfigurationListResponse().scopedConfigurations(scopedConfigurations);
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/get")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ScopedConfigurationReadResponse getScopedConfigurationById(@Body final ScopedConfigurationReadRequestBody scopedConfigurationReadRequestBody) {
    return ApiHelper.execute(() -> {
      final UUID scopedConfigurationId = scopedConfigurationReadRequestBody.getScopedConfigurationId();
      final ScopedConfigurationRead scopedConfiguration = scopedConfigurationHandler.getScopedConfiguration(scopedConfigurationId);

      return new ScopedConfigurationReadResponse().data(scopedConfiguration);
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/get_context")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ScopedConfigurationContextResponse getScopedConfigurationContext(@Body final ScopedConfigurationContextRequestBody scopedConfigurationContextRequestBody) {
    return ApiHelper.execute(() -> scopedConfigurationHandler.getScopedConfigurationContext(scopedConfigurationContextRequestBody));
  }

  @SuppressWarnings("LineLength")
  @Post("/update")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ScopedConfigurationUpdateResponse updateScopedConfiguration(@Body final ScopedConfigurationUpdateRequestBody scopedConfigurationUpdateRequestBody) {
    return ApiHelper.execute(() -> {
      final UUID scopedConfigurationId = scopedConfigurationUpdateRequestBody.getScopedConfigurationId();
      final ScopedConfigurationCreateRequestBody scopedConfigData = scopedConfigurationUpdateRequestBody.getData();

      final ScopedConfigurationRead updatedScopedConfiguration = scopedConfigurationHandler.updateScopedConfiguration(
          scopedConfigurationId,
          scopedConfigData);

      return new ScopedConfigurationUpdateResponse().data(updatedScopedConfiguration);
    });
  }

}
