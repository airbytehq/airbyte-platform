/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.hydration

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.api.client.model.generated.SecretPersistenceConfig
import io.airbyte.api.client.model.generated.SecretPersistenceConfigGetRequestBody
import io.airbyte.api.client.model.generated.SecretStorageIdRequestBody
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.SecretsHelpers
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.metrics.MetricClient
import io.airbyte.workers.helper.toConfigModel
import io.airbyte.workers.helper.toModel
import java.io.IOException
import java.lang.RuntimeException
import java.util.UUID

/**
 * Performs secrets hydration of raw JSON connector configs.
 */
class ConnectorSecretsHydrator(
  private val secretsRepositoryReader: SecretsRepositoryReader,
  private val airbyteApiClient: AirbyteApiClient,
  private val useRuntimeSecretPersistence: Boolean,
  private val defaultSecretPersistence: SecretPersistence,
  private val metricClient: MetricClient,
) {
  private fun getPersistenceByStorageId(secretStorageId: UUID): SecretPersistence {
    val secretStorage = airbyteApiClient.secretStorageApi.getSecretStorage(SecretStorageIdRequestBody(secretStorageId))
    return RuntimeSecretPersistence(secretStorage.toConfigModel(), metricClient)
  }

  private fun getLegacyPersistenceByOrgId(organizationId: UUID): SecretPersistence {
    val secretPersistenceConfig: SecretPersistenceConfig
    try {
      secretPersistenceConfig =
        airbyteApiClient.secretPersistenceConfigApi.getSecretsPersistenceConfig(
          SecretPersistenceConfigGetRequestBody(ScopeType.ORGANIZATION, organizationId),
        )
    } catch (e: IOException) {
      throw RuntimeException(e)
    }

    return RuntimeSecretPersistence(secretPersistenceConfig.toModel(), metricClient)
  }

  private fun getPersistence(
    config: ConfigWithSecretReferences,
    context: SecretHydrationContext,
  ): SecretPersistence {
    val secretStorageId = SecretsHelpers.SecretReferenceHelpers.getSecretStorageIdFromConfig(config)
    return when {
      secretStorageId != null -> getPersistenceByStorageId(secretStorageId)
      useRuntimeSecretPersistence -> getLegacyPersistenceByOrgId(context.organizationId)
      else -> defaultSecretPersistence
    }
  }

  fun hydrateConfig(
    config: ConfigWithSecretReferences,
    context: SecretHydrationContext,
  ): JsonNode? {
    val secretPersistence = getPersistence(config, context)
    return secretsRepositoryReader.hydrateConfig(config, secretPersistence)
  }
}
