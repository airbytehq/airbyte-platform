/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers

import com.fasterxml.jackson.databind.node.POJONode
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.SecretsPersistenceConfigApi
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.api.client.model.generated.SecretPersistenceConfig
import io.airbyte.api.client.model.generated.SecretPersistenceType
import io.airbyte.commons.json.Jsons
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.workers.helper.SecretPersistenceConfigHelper
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectorSecretsHydratorTest {
  @Test
  fun `uses runtime hydration if ff enabled for organization id`() {
    val airbyteApiClient: AirbyteApiClient = mockk()
    val secretsRepositoryReader: SecretsRepositoryReader = mockk()
    val secretsApiClient: SecretsPersistenceConfigApi = mockk()
    val useRuntimeSecretPersistence = true

    every { airbyteApiClient.secretPersistenceConfigApi } returns secretsApiClient

    val hydrator =
      ConnectorSecretsHydrator(
        secretsRepositoryReader,
        airbyteApiClient,
        useRuntimeSecretPersistence,
      )

    val unhydratedConfig = POJONode("un-hydrated")
    val hydratedConfig = POJONode("hydrated")

    val orgId = UUID.randomUUID()

    val secretConfig =
      SecretPersistenceConfig(
        secretPersistenceType = SecretPersistenceType.AWS,
        configuration = Jsons.jsonNode(mapOf<String, String>()),
        scopeId = orgId,
        scopeType = ScopeType.ORGANIZATION,
      )

    val runtimeSecretPersistence = RuntimeSecretPersistence(mockk())

    mockkStatic(SecretPersistenceConfigHelper::class)
    every { SecretPersistenceConfigHelper.fromApiSecretPersistenceConfig(secretConfig) } returns runtimeSecretPersistence

    every { secretsApiClient.getSecretsPersistenceConfig(any()) } returns secretConfig
    every { secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(unhydratedConfig, runtimeSecretPersistence) } returns hydratedConfig

    val result = hydrator.hydrateConfig(unhydratedConfig, orgId)

    verify { secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(unhydratedConfig, runtimeSecretPersistence) }

    Assertions.assertEquals(hydratedConfig, result)
  }

  @Test
  fun `uses default hydration if ff not enabled for organization id`() {
    val airbyteApiClient: AirbyteApiClient = mockk()
    val secretsRepositoryReader: SecretsRepositoryReader = mockk()
    val secretsApiClient: SecretsPersistenceConfigApi = mockk()
    val useRuntimeSecretPersistence = false

    every { airbyteApiClient.secretPersistenceConfigApi } returns secretsApiClient

    val hydrator =
      ConnectorSecretsHydrator(
        secretsRepositoryReader,
        airbyteApiClient,
        useRuntimeSecretPersistence,
      )

    val unhydratedConfig = POJONode("un-hydrated")
    val hydratedConfig = POJONode("hydrated")

    val orgId = UUID.randomUUID()

    every { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(unhydratedConfig) } returns hydratedConfig

    val result = hydrator.hydrateConfig(unhydratedConfig, orgId)

    verify { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(unhydratedConfig) }

    Assertions.assertEquals(hydratedConfig, result)
  }
}
