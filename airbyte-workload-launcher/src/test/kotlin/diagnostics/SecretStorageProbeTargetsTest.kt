/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DataplaneSecretStorage
import io.airbyte.api.client.model.generated.DataplaneSecretStoragesRequestBody
import io.airbyte.api.client.model.generated.DataplaneSecretStoragesResponse
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.micronaut.runtime.AirbyteInternalApiClientConfig
import io.airbyte.micronaut.runtime.AirbyteInternalApiClientConfig.AuthConfig
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ServerException
import java.util.UUID

internal class SecretStorageProbeTargetsTest {
  private val clientId = "dataplane-client-id"
  private val apiClient = mockk<AirbyteApiClient>()
  private val targets = SecretStorageProbeTargets(apiClient, AirbyteInternalApiClientConfig(auth = AuthConfig(clientId = clientId)))

  @Test
  fun `returns each store Airbyte lists for this data plane`() {
    val workspaceStore = DataplaneSecretStorage(UUID.randomUUID(), ScopeType.WORKSPACE, UUID.randomUUID())
    val organizationStore = DataplaneSecretStorage(UUID.randomUUID(), ScopeType.ORGANIZATION, UUID.randomUUID())
    every { apiClient.dataplaneApi.listDataplaneSecretStorages(DataplaneSecretStoragesRequestBody(clientId)) } returns
      DataplaneSecretStoragesResponse(listOf(workspaceStore, organizationStore))

    assertEquals(
      listOf(
        SecretStorageProbeTarget(workspaceStore.secretStorageId, ScopeType.WORKSPACE, workspaceStore.scopeId),
        SecretStorageProbeTarget(organizationStore.secretStorageId, ScopeType.ORGANIZATION, organizationStore.scopeId),
      ),
      targets.current(),
    )
  }

  @Test
  fun `returns no targets when Airbyte lists no stores`() {
    every { apiClient.dataplaneApi.listDataplaneSecretStorages(DataplaneSecretStoragesRequestBody(clientId)) } returns
      DataplaneSecretStoragesResponse(emptyList())

    assertEquals(emptyList<SecretStorageProbeTarget>(), targets.current())
  }

  @Test
  fun `passes on an API error`() {
    every { apiClient.dataplaneApi.listDataplaneSecretStorages(any()) } throws ServerException("unavailable", 503)

    assertThrows<ServerException> { targets.current() }
  }
}
