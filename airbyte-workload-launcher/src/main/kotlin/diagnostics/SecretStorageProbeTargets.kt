/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DataplaneSecretStoragesRequestBody
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.micronaut.runtime.AirbyteInternalApiClientConfig
import jakarta.inject.Singleton
import java.util.UUID

/**
 * A secret store to check, and the workspace or organization it is configured for.
 */
data class SecretStorageProbeTarget(
  val secretStorageId: UUID,
  val scopeType: ScopeType,
  val scopeId: UUID,
)

/**
 * Asks Airbyte which distinct secret stores this data plane's workloads use.
 */
@Singleton
class SecretStorageProbeTargets(
  private val airbyteApiClient: AirbyteApiClient,
  private val airbyteInternalApiClientConfig: AirbyteInternalApiClientConfig,
) {
  fun current(): List<SecretStorageProbeTarget> =
    airbyteApiClient.dataplaneApi
      .listDataplaneSecretStorages(DataplaneSecretStoragesRequestBody(clientId = airbyteInternalApiClientConfig.auth.clientId))
      .secretStorages
      .map { SecretStorageProbeTarget(it.secretStorageId, it.scopeType, it.scopeId) }
}
