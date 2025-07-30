/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.SecretsPersistenceConfigApi
import io.airbyte.api.model.generated.CreateOrUpdateSecretsPersistenceConfigRequestBody
import io.airbyte.api.model.generated.ScopeType
import io.airbyte.api.model.generated.SecretPersistenceConfig
import io.airbyte.api.model.generated.SecretPersistenceConfigGetRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException
import io.airbyte.commons.server.handlers.SecretPersistenceConfigHandler
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.featureflag.Workspace
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.Objects

@Controller("/api/v1/secrets_persistence_config")
@Secured(SecurityRule.IS_AUTHENTICATED)
class SecretsPersistenceConfigApiController(
  private val workspaceService: WorkspaceService,
  private val secretPersistenceConfigService: SecretPersistenceConfigService,
  private val secretPersistenceConfigHandler: SecretPersistenceConfigHandler,
  private val featureFlagClient: FeatureFlagClient,
) : SecretsPersistenceConfigApi {
  @Post("/create_or_update")
  @Secured(AuthRoleConstants.ADMIN)
  override fun createOrUpdateSecretsPersistenceConfig(
    @Body requestBody: CreateOrUpdateSecretsPersistenceConfigRequestBody,
  ) {
    execute<Any?> {
      when (requestBody.scope) {
        ScopeType.WORKSPACE -> {
          if (!featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Workspace(requestBody.scopeId))) {
            throw BadObjectSchemaKnownException(
              String.format("Runtime secret persistence is not supported for workspace %s", requestBody.scopeId),
            )
          }
        }

        ScopeType.ORGANIZATION -> {
          if (!featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(requestBody.scopeId))) {
            throw BadObjectSchemaKnownException(
              String.format("Runtime secret persistence is not supported for organization %s", requestBody.scopeId),
            )
          }
        }

        else -> throw IllegalStateException("Unexpected value: " + requestBody.scope)
      }
      null
    }

    execute {
      val secretCoordinate =
        secretPersistenceConfigHandler.buildRsmCoordinate(
          requestBody.scope.convertTo<io.airbyte.config.ScopeType>(),
          requestBody.scopeId,
        )
      val secretPersistenceConfigCoordinate =
        secretPersistenceConfigHandler.writeToEnvironmentSecretPersistence(
          secretCoordinate,
          Jsons.serialize(requestBody.configuration),
        )
      secretPersistenceConfigService.createOrUpdate(
        requestBody.scope.convertTo<io.airbyte.config.ScopeType>(),
        requestBody.scopeId,
        requestBody.secretPersistenceType.convertTo<io.airbyte.config.SecretPersistenceConfig.SecretPersistenceType>(),
        secretPersistenceConfigCoordinate,
      )
    }
  }

  @Post("/get")
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE)
  override fun getSecretsPersistenceConfig(
    @Body requestBody: SecretPersistenceConfigGetRequestBody,
  ): SecretPersistenceConfig? {
    if (Objects.requireNonNull(requestBody.scopeType) == io.airbyte.api.model.generated.ScopeType.WORKSPACE) {
      execute<Any> {
        throw BadObjectSchemaKnownException(
          String.format("Runtime secret persistence is not supported for workspace %s", requestBody.scopeId),
        )
      }
    }
    return execute {
      val secretPersistenceConfig =
        secretPersistenceConfigService.get(io.airbyte.config.ScopeType.ORGANIZATION, requestBody.scopeId)
      secretPersistenceConfigHandler.buildSecretPersistenceConfigResponse(secretPersistenceConfig)
    }
  }
}
