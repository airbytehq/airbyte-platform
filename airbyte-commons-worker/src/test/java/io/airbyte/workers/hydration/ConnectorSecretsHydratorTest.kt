/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.hydration

import com.fasterxml.jackson.databind.node.POJONode
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.SecretsPersistenceConfigApi
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.api.client.model.generated.SecretPersistenceConfig
import io.airbyte.api.client.model.generated.SecretPersistenceConfigGetRequestBody
import io.airbyte.api.client.model.generated.SecretPersistenceType
import io.airbyte.api.client.model.generated.SecretStorageIdRequestBody
import io.airbyte.api.client.model.generated.SecretStorageRead
import io.airbyte.commons.json.Jsons
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretReferenceConfig
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.metrics.MetricClient
import io.airbyte.workers.helper.toConfigModel
import io.mockk.EqMatcher
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.mockkStatic
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectorSecretsHydratorTest {
  private val defaultSecretPersistence = mockk<SecretPersistence>()

  @BeforeEach
  fun setup() {
    clearAllMocks()
    mockkConstructor(RuntimeSecretPersistence::class)
  }

  @Test
  fun `uses runtime hydration if ff enabled for organization id`() {
    val airbyteApiClient: AirbyteApiClient = mockk()
    val metricClient: MetricClient = mockk()
    val secretsRepositoryReader: SecretsRepositoryReader = mockk()
    val secretsApiClient: SecretsPersistenceConfigApi = mockk()
    val useRuntimeSecretPersistence = true

    every { airbyteApiClient.secretPersistenceConfigApi } returns secretsApiClient

    val hydrator =
      ConnectorSecretsHydrator(
        secretsRepositoryReader,
        airbyteApiClient,
        useRuntimeSecretPersistence,
        defaultSecretPersistence,
        metricClient,
      )

    val unhydratedConfig =
      ConfigWithSecretReferences(
        POJONode("un-hydrated"),
        mapOf(
          "$" to
            SecretReferenceConfig(
              secretCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate(),
            ),
        ),
      )
    val hydratedConfig = POJONode("hydrated")

    val orgId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    val secretConfig =
      SecretPersistenceConfig(
        secretPersistenceType = SecretPersistenceType.AWS,
        configuration = Jsons.jsonNode(mapOf<String, String>()),
        scopeId = orgId,
        scopeType = ScopeType.ORGANIZATION,
      )

    every { secretsApiClient.getSecretsPersistenceConfig(any()) } returns secretConfig
    every { secretsRepositoryReader.hydrateConfig(unhydratedConfig, any<Map<UUID?, SecretPersistence>>()) } returns hydratedConfig

    val result =
      hydrator.hydrateConfig(
        unhydratedConfig,
        SecretHydrationContext(
          organizationId = orgId,
          workspaceId = workspaceId,
        ),
      )

    verify { secretsApiClient.getSecretsPersistenceConfig(SecretPersistenceConfigGetRequestBody(ScopeType.ORGANIZATION, orgId)) }
    verify { constructedWith<RuntimeSecretPersistence>(EqMatcher(secretConfig), EqMatcher(metricClient)) }
    verify { secretsRepositoryReader.hydrateConfig(unhydratedConfig, any<Map<UUID?, SecretPersistence>>()) }

    Assertions.assertEquals(hydratedConfig, result)
  }

  @Test
  fun `uses default hydration if ff not enabled for organization id`() {
    val airbyteApiClient: AirbyteApiClient = mockk()
    val metricClient: MetricClient = mockk()
    val secretsRepositoryReader: SecretsRepositoryReader = mockk()
    val secretsApiClient: SecretsPersistenceConfigApi = mockk()
    val useRuntimeSecretPersistence = false

    every { airbyteApiClient.secretPersistenceConfigApi } returns secretsApiClient

    val hydrator =
      ConnectorSecretsHydrator(
        secretsRepositoryReader,
        airbyteApiClient,
        useRuntimeSecretPersistence,
        defaultSecretPersistence,
        metricClient,
      )

    val unhydratedConfig =
      ConfigWithSecretReferences(
        POJONode("un-hydrated"),
        mapOf(
          "$" to
            SecretReferenceConfig(
              secretCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate(),
            ),
        ),
      )
    val hydratedConfig = POJONode("hydrated")

    val orgId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    every { secretsRepositoryReader.hydrateConfig(unhydratedConfig, mapOf(null to defaultSecretPersistence)) } returns
      hydratedConfig

    val result =
      hydrator.hydrateConfig(
        unhydratedConfig,
        SecretHydrationContext(
          organizationId = orgId,
          workspaceId = workspaceId,
        ),
      )

    verify { secretsRepositoryReader.hydrateConfig(unhydratedConfig, mapOf(null to defaultSecretPersistence)) }

    Assertions.assertEquals(hydratedConfig, result)
  }

  @Test
  fun `uses persistence from secret storage`() {
    val airbyteApiClient: AirbyteApiClient = mockk()
    val metricClient: MetricClient = mockk()
    val secretsRepositoryReader: SecretsRepositoryReader = mockk()
    val useRuntimeSecretPersistence = true

    val hydrator =
      ConnectorSecretsHydrator(
        secretsRepositoryReader,
        airbyteApiClient,
        useRuntimeSecretPersistence,
        defaultSecretPersistence,
        metricClient,
      )

    val secretStorageId = UUID.randomUUID()

    val unhydratedConfig =
      ConfigWithSecretReferences(
        Jsons.emptyObject(),
        mapOf(
          "$.password" to
            SecretReferenceConfig(
              secretCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate("my-secret-coord", 1),
              secretStorageId = secretStorageId,
            ),
        ),
      )
    val hydratedConfig = Jsons.jsonNode(mapOf("password" to "my-secret"))

    val orgId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    val secretStorage =
      mockk<SecretStorageRead> {
        every { id } returns secretStorageId
      }
    val secretStorageConfig = mockk<io.airbyte.config.SecretPersistenceConfig>()

    mockkStatic("io.airbyte.workers.helper.SecretPersistenceConfigConvertersKt")
    every { secretStorage.toConfigModel() } returns secretStorageConfig

    every { airbyteApiClient.secretStorageApi.getSecretStorage(SecretStorageIdRequestBody(secretStorageId)) } returns secretStorage
    every { secretsRepositoryReader.hydrateConfig(unhydratedConfig, any<Map<UUID?, SecretPersistence>>()) } returns hydratedConfig

    val result =
      hydrator.hydrateConfig(
        unhydratedConfig,
        SecretHydrationContext(
          organizationId = orgId,
          workspaceId = workspaceId,
        ),
      )

    verify { airbyteApiClient.secretStorageApi.getSecretStorage(SecretStorageIdRequestBody(secretStorageId)) }
    verify { constructedWith<RuntimeSecretPersistence>(EqMatcher(secretStorageConfig), EqMatcher(metricClient)) }
    verify { secretsRepositoryReader.hydrateConfig(unhydratedConfig, any<Map<UUID?, SecretPersistence>>()) }

    Assertions.assertEquals(hydratedConfig, result)
  }
}
