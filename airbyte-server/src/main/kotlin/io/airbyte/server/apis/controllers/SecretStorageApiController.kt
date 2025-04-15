/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.SecretStorageApi
import io.airbyte.api.model.generated.SecretStorageIdRequestBody
import io.airbyte.api.model.generated.SecretStorageRead
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.SecretStorageWithConfig
import io.airbyte.domain.services.secrets.SecretStorageService
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn

@Controller("/api/v1/secret_storage")
@ExecuteOn(AirbyteTaskExecutors.IO)
class SecretStorageApiController(
  private val secretStorageService: SecretStorageService,
) : SecretStorageApi {
  @RequiresIntent(Intent.ManageSecretStorages)
  override fun getSecretStorage(
    @Body secretStorageIdRequestBody: SecretStorageIdRequestBody,
  ): SecretStorageRead {
    val secretStorage = secretStorageService.getById(SecretStorageId(secretStorageIdRequestBody.secretStorageId))
    return secretStorageService.hydrateStorageConfig(secretStorage).toApiModel()
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
