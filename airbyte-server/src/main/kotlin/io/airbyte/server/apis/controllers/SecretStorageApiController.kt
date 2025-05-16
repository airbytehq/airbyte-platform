/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.SecretStorageApi
import io.airbyte.api.model.generated.MigrateSecretStorageRequestBody
import io.airbyte.api.model.generated.SecretStorageCreateRequestBody
import io.airbyte.api.model.generated.SecretStorageIdRequestBody
import io.airbyte.api.model.generated.SecretStorageListRequestBody
import io.airbyte.api.model.generated.SecretStorageRead
import io.airbyte.api.model.generated.SecretStorageReadList
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.SecretStorageWithConfig
import io.airbyte.domain.models.UserId
import io.airbyte.domain.services.secrets.SecretMigrationService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import jakarta.validation.Valid
import jakarta.validation.constraints.NotNull

@Controller("/api/v1/secret_storage")
@ExecuteOn(AirbyteTaskExecutors.IO)
class SecretStorageApiController(
  private val secretStorageService: SecretStorageService,
  private val secretMigrationService: SecretMigrationService,
  private val currentUserService: CurrentUserService,
) : SecretStorageApi {
  @RequiresIntent(Intent.ManageSecretStorages)
  override fun createSecretStorage(
    @Body secretStorageCreateRequestBody:
      @Valid @NotNull
      SecretStorageCreateRequestBody,
  ): SecretStorageRead {
    val secretStorage =
      secretStorageService.createSecretStorage(
        SecretStorageCreate(
          scopeType =
            when (secretStorageCreateRequestBody.scopeType) {
              io.airbyte.api.model.generated.ScopeType.ORGANIZATION -> io.airbyte.domain.models.SecretStorageScopeType.ORGANIZATION
              io.airbyte.api.model.generated.ScopeType.WORKSPACE -> io.airbyte.domain.models.SecretStorageScopeType.WORKSPACE
            },
          scopeId = secretStorageCreateRequestBody.scopeId,
          storageType =
            when (secretStorageCreateRequestBody.secretStorageType) {
              io.airbyte.api.model.generated.SecretStorageType.VAULT -> SecretStorageType.VAULT
              io.airbyte.api.model.generated.SecretStorageType.AWS_SECRETS_MANAGER -> SecretStorageType.AWS_SECRETS_MANAGER
              io.airbyte.api.model.generated.SecretStorageType.GOOGLE_SECRET_MANAGER -> SecretStorageType.GOOGLE_SECRET_MANAGER
              io.airbyte.api.model.generated.SecretStorageType.AZURE_KEY_VAULT -> SecretStorageType.AZURE_KEY_VAULT
              io.airbyte.api.model.generated.SecretStorageType.LOCAL_TESTING -> SecretStorageType.LOCAL_TESTING
            },
          descriptor = secretStorageCreateRequestBody.descriptor,
          configuredFromEnvironment = false,
          createdBy = UserId(currentUserService.currentUser.userId),
        ),
        storageConfig = secretStorageCreateRequestBody.config,
      )
    return SecretStorageWithConfig(secretStorage, null).toApiModel()
  }

  @RequiresIntent(Intent.ManageSecretStorages)
  override fun deleteSecretStorage(
    @Body secretStorageIdRequestBody:
      @Valid @NotNull
      SecretStorageIdRequestBody,
  ) {
    secretStorageService.deleteSecretStorage(
      SecretStorageId(secretStorageIdRequestBody.secretStorageId),
      UserId(currentUserService.currentUser.userId),
    )
  }

  @RequiresIntent(Intent.ManageSecretStorages)
  override fun getSecretStorage(
    @Body secretStorageIdRequestBody: SecretStorageIdRequestBody,
  ): SecretStorageRead {
    val secretStorage = secretStorageService.getById(SecretStorageId(secretStorageIdRequestBody.secretStorageId))
    return if (secretStorage.configuredFromEnvironment) {
      SecretStorageWithConfig(secretStorage, null).toApiModel()
    } else {
      secretStorageService.hydrateStorageConfig(secretStorage).toApiModel()
    }
  }

  @RequiresIntent(Intent.ManageSecretStorages)
  override fun listSecretStorage(
    @Body secretStorageListRequestBody:
      @Valid @NotNull
      SecretStorageListRequestBody,
  ): SecretStorageReadList =
    SecretStorageReadList()
      .secretStorages(
        secretStorageService
          .listSecretStorage(
            when (secretStorageListRequestBody.scopeType) {
              io.airbyte.api.model.generated.ScopeType.ORGANIZATION -> io.airbyte.domain.models.SecretStorageScopeType.ORGANIZATION
              io.airbyte.api.model.generated.ScopeType.WORKSPACE -> io.airbyte.domain.models.SecretStorageScopeType.WORKSPACE
            },
            secretStorageListRequestBody.scopeId,
          ).map { SecretStorageWithConfig(it, null).toApiModel() },
      )

  @RequiresIntent(Intent.ManageSecretStorages)
  override fun migrateSecretStorage(
    @Body migrateSecretStorageRequestBody: MigrateSecretStorageRequestBody,
  ) {
    secretMigrationService.migrateSecrets(
      SecretStorageId(migrateSecretStorageRequestBody.fromSecretStorageId),
      SecretStorageId(migrateSecretStorageRequestBody.toSecretStorageId),
      when (migrateSecretStorageRequestBody.scopeType) {
        io.airbyte.api.model.generated.ScopeType.ORGANIZATION -> io.airbyte.config.ScopeType.ORGANIZATION
        io.airbyte.api.model.generated.ScopeType.WORKSPACE -> io.airbyte.config.ScopeType.WORKSPACE
      },
      migrateSecretStorageRequestBody.scopeId,
    )
  }
}

private fun SecretStorageWithConfig.toApiModel(): SecretStorageRead {
  val secretStorageRead = SecretStorageRead()
  secretStorageRead.id(this.secretStorage.id?.value)
  secretStorageRead.isConfiguredFromEnvironment(this.secretStorage.configuredFromEnvironment)
  secretStorageRead.config(this.config)
  secretStorageRead.scopeId(this.secretStorage.scopeId)
  secretStorageRead.scopeType(
    when (this.secretStorage.scopeType) {
      io.airbyte.domain.models.SecretStorageScopeType.ORGANIZATION -> io.airbyte.api.model.generated.ScopeType.ORGANIZATION
      io.airbyte.domain.models.SecretStorageScopeType.WORKSPACE -> io.airbyte.api.model.generated.ScopeType.WORKSPACE
    },
  )
  secretStorageRead.secretStorageType(
    when (this.secretStorage.storageType) {
      SecretStorageType.VAULT -> io.airbyte.api.model.generated.SecretStorageType.VAULT
      SecretStorageType.AWS_SECRETS_MANAGER -> io.airbyte.api.model.generated.SecretStorageType.AWS_SECRETS_MANAGER
      SecretStorageType.GOOGLE_SECRET_MANAGER -> io.airbyte.api.model.generated.SecretStorageType.GOOGLE_SECRET_MANAGER
      SecretStorageType.AZURE_KEY_VAULT -> io.airbyte.api.model.generated.SecretStorageType.AZURE_KEY_VAULT
      SecretStorageType.LOCAL_TESTING -> io.airbyte.api.model.generated.SecretStorageType.LOCAL_TESTING
    },
  )
  return secretStorageRead
}
