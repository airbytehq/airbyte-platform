/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ScopeType
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.secrets.SecretsHelpers
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.metrics.MetricClient
import jakarta.inject.Singleton
import java.util.UUID

data class SecretHydrationContext(
  val organizationId: OrganizationId,
  val workspaceId: WorkspaceId,
) {
  companion object {
    @JvmStatic
    @Deprecated(
      message = "Use constructor directly once callers have been migrated to Kotlin",
      replaceWith = ReplaceWith("SecretHydrationContext(organizationId, workspaceId)"),
    )
    fun fromJava(
      organizationId: UUID,
      workspaceId: UUID,
    ): SecretHydrationContext = SecretHydrationContext(OrganizationId(organizationId), WorkspaceId(workspaceId))
  }
}

@Singleton
class SecretPersistenceService(
  private val defaultSecretPersistence: SecretPersistence,
  private val secretStorageService: SecretStorageService,
  private val secretPersistenceConfigService: SecretPersistenceConfigService,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
) {
  private fun getStorageIdFromConfig(jsonConfig: JsonNode): SecretStorageId? {
    // TODO(pedro): For now we only get one secret storage ID from the config,
    //  but we could support hydrating each secret individually from its corresponding storage.
    val secretStorageIds = SecretsHelpers.SecretReferenceHelpers.getSecretStorageIdsFromConfig(jsonConfig)
    return when {
      secretStorageIds.size > 1 -> throw IllegalStateException("Multiple secret storage IDs found in the config: $secretStorageIds")
      secretStorageIds.isNotEmpty() -> SecretStorageId(secretStorageIds.first())
      else -> null
    }
  }

  private fun getPersistenceByStorageId(secretStorageId: SecretStorageId): SecretPersistence {
    val secretStorage = secretStorageService.getById(secretStorageId)
    val secretStorageConfig = secretStorageService.hydrateStorageConfig(secretStorage).config
    val secretPersistenceConfig =
      SecretPersistenceConfig()
        .withScopeId(secretStorage.scopeId)
        .withScopeType(
          when (secretStorage.scopeType) {
            SecretStorageScopeType.WORKSPACE -> ScopeType.WORKSPACE
            SecretStorageScopeType.ORGANIZATION -> ScopeType.ORGANIZATION
          },
        ).withSecretPersistenceType(
          when (secretStorage.storageType) {
            SecretStorageType.AWS_SECRETS_MANAGER -> SecretPersistenceConfig.SecretPersistenceType.AWS
            SecretStorageType.GOOGLE_SECRET_MANAGER -> SecretPersistenceConfig.SecretPersistenceType.GOOGLE
            SecretStorageType.VAULT -> SecretPersistenceConfig.SecretPersistenceType.VAULT
            SecretStorageType.LOCAL_TESTING -> SecretPersistenceConfig.SecretPersistenceType.TESTING
            SecretStorageType.AZURE_KEY_VAULT -> throw IllegalStateException("Azure Key Vault is not supported")
          },
        ).withConfiguration(Jsons.deserializeToStringMap(secretStorageConfig))

    return RuntimeSecretPersistence(secretPersistenceConfig, metricClient)
  }

  private fun getLegacyByOrganizationId(organizationId: OrganizationId): SecretPersistence {
    val secretPersistenceConfig = secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.value)
    return RuntimeSecretPersistence(secretPersistenceConfig, metricClient)
  }

  fun getPersistenceFromConfig(
    jsonConfig: JsonNode,
    context: SecretHydrationContext,
  ): SecretPersistence {
    val secretStorageId = getStorageIdFromConfig(jsonConfig)
    val useRuntimeSecretPersistence = featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(context.organizationId.value))
    return when {
      secretStorageId != null -> getPersistenceByStorageId(secretStorageId)
      useRuntimeSecretPersistence -> getLegacyByOrganizationId(context.organizationId)
      else -> defaultSecretPersistence
    }
  }
}
