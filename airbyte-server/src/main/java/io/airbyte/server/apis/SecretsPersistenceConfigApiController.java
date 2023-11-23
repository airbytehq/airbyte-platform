/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;

import io.airbyte.api.generated.SecretsPersistenceConfigApi;
import io.airbyte.api.model.generated.CreateOrUpdateSecretsPersistenceConfigRequestBody;
import io.airbyte.api.model.generated.SecretPersistenceConfig;
import io.airbyte.api.model.generated.SecretPersistenceConfigGetRequestBody;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException;
import io.airbyte.commons.server.handlers.SecretPersistenceConfigHandler;
import io.airbyte.config.ScopeType;
import io.airbyte.config.secrets.SecretCoordinate;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.featureflag.Workspace;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings("PMD.PreserveStackTrace")
@Controller("/api/v1/secrets_persistence_config")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Slf4j
public class SecretsPersistenceConfigApiController implements SecretsPersistenceConfigApi {

  private final WorkspaceService workspaceService;
  private final SecretPersistenceConfigService secretPersistenceConfigService;
  private final SecretPersistenceConfigHandler secretPersistenceConfigHandler;
  private final FeatureFlagClient featureFlagClient;

  public SecretsPersistenceConfigApiController(final WorkspaceService workspaceService,
                                               final SecretPersistenceConfigService secretPersistenceConfigService,
                                               final SecretPersistenceConfigHandler secretPersistenceConfigHandler,
                                               final FeatureFlagClient featureFlagClient) {
    this.workspaceService = workspaceService;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
    this.secretPersistenceConfigHandler = secretPersistenceConfigHandler;
    this.featureFlagClient = featureFlagClient;
  }

  @Override
  @Post("/create_or_update")
  @Secured({ADMIN})
  @SuppressWarnings("MissingSwitchDefault")
  public void createOrUpdateSecretsPersistenceConfig(@Body final CreateOrUpdateSecretsPersistenceConfigRequestBody requestBody) {

    ApiHelper.execute(() -> {
      switch (requestBody.getScope()) {
        case WORKSPACE -> {
          if (!featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Workspace(requestBody.getScopeId()))) {
            throw new BadObjectSchemaKnownException(
                String.format("Runtime secret persistence is not supported for workspace %s", requestBody.getScopeId()));
          }
        }
        case ORGANIZATION -> {
          if (!featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(requestBody.getScopeId()))) {
            throw new BadObjectSchemaKnownException(
                String.format("Runtime secret persistence is not supported for organization %s", requestBody.getScopeId()));
          }
        }
        default -> throw new IllegalStateException("Unexpected value: " + requestBody.getScope());
      }
      return null;
    });

    ApiHelper.execute(
        () -> {

          final SecretCoordinate secretCoordinate = secretPersistenceConfigHandler.buildRsmCoordinate(
              Enums.convertTo(requestBody.getScope(), io.airbyte.config.ScopeType.class),
              requestBody.getScopeId());
          final String secretPersistenceConfigCoordinate = secretPersistenceConfigHandler.writeToEnvironmentSecretPersistence(
              secretCoordinate,
              Jsons.serialize(requestBody.getConfiguration()));

          return secretPersistenceConfigService.createOrUpdateSecretPersistenceConfig(
              Enums.convertTo(requestBody.getScope(), io.airbyte.config.ScopeType.class),
              requestBody.getScopeId(),
              Enums.convertTo(requestBody.getSecretPersistenceType(), io.airbyte.config.SecretPersistenceConfig.SecretPersistenceType.class),
              secretPersistenceConfigCoordinate);
        });
  }

  @Post("/get")
  @Secured({ADMIN})
  @Override
  public SecretPersistenceConfig getSecretsPersistenceConfig(final SecretPersistenceConfigGetRequestBody requestBody) {
    if (Objects.requireNonNull(requestBody.getScopeType()) == io.airbyte.api.model.generated.ScopeType.WORKSPACE) {
      ApiHelper.execute(() -> {
        throw new BadObjectSchemaKnownException(
            String.format("Runtime secret persistence is not supported for workspace %s", requestBody.getScopeId()));
      });
    }
    return ApiHelper.execute(
        () -> {
          final io.airbyte.config.SecretPersistenceConfig secretPersistenceConfig =
              secretPersistenceConfigService.getSecretPersistenceConfig(ScopeType.ORGANIZATION, requestBody.getScopeId());
          return secretPersistenceConfigHandler.buildSecretPersistenceConfigResponse(secretPersistenceConfig);
        });
  }

}
