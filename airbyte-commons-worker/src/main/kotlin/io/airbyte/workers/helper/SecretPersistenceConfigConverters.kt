/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.api.client.model.generated.SecretStorageRead
import io.airbyte.api.client.model.generated.SecretStorageType
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.json.Jsons
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.api.client.model.generated.SecretPersistenceConfig as ApiSecretPersistenceConfig

fun ApiSecretPersistenceConfig.toModel(): SecretPersistenceConfig =
  SecretPersistenceConfig()
    .withScopeType(
      Enums.convertTo(
        this.scopeType,
        io.airbyte.config.ScopeType::class.java,
      ),
    ).withScopeId(this.scopeId)
    .withConfiguration(Jsons.deserializeToStringMap(this.configuration))
    .withSecretPersistenceType(
      Enums.convertTo(
        this.secretPersistenceType,
        SecretPersistenceConfig.SecretPersistenceType::class.java,
      ),
    )

fun SecretStorageRead.toConfigModel(): SecretPersistenceConfig =
  SecretPersistenceConfig()
    .withScopeType(
      when (this.scopeType) {
        ScopeType.ORGANIZATION -> io.airbyte.config.ScopeType.ORGANIZATION
        ScopeType.WORKSPACE -> io.airbyte.config.ScopeType.WORKSPACE
      },
    ).withScopeId(this.scopeId)
    .withConfiguration(Jsons.deserializeToStringMap(this.config))
    .withSecretPersistenceType(
      when (this.secretStorageType) {
        SecretStorageType.AWS_SECRETS_MANAGER -> SecretPersistenceConfig.SecretPersistenceType.AWS
        SecretStorageType.GOOGLE_SECRET_MANAGER -> SecretPersistenceConfig.SecretPersistenceType.GOOGLE
        SecretStorageType.VAULT -> SecretPersistenceConfig.SecretPersistenceType.VAULT
        SecretStorageType.LOCAL_TESTING -> SecretPersistenceConfig.SecretPersistenceType.TESTING
        SecretStorageType.AZURE_KEY_VAULT -> throw IllegalStateException("Azure Key Vault is not supported")
      },
    )
