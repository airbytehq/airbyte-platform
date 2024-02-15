package io.airbyte.workers

import com.fasterxml.jackson.databind.node.POJONode
import io.airbyte.api.client.generated.SecretsPersistenceConfigApi
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.api.client.model.generated.SecretPersistenceConfig
import io.airbyte.api.client.model.generated.SecretPersistenceType
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.workers.helper.SecretPersistenceConfigHelper
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

class BaseInputHydratorTest {
  @Test
  fun `uses runtime hydration if ff enabled for organization id`() {
    val secretsRepositoryReader: SecretsRepositoryReader = mockk()
    val secretsApiClient: SecretsPersistenceConfigApi = mockk()
    val featureFlagClient: FeatureFlagClient = mockk()

    val hydrator =
      BaseInputHydrator(
        secretsRepositoryReader,
        secretsApiClient,
        featureFlagClient,
      )

    val unhyrdatedConfig = POJONode("un-hydrated")
    val hydratedConfig = POJONode("hydrated")

    val orgId = UUID.randomUUID()

    val secretConfig =
      SecretPersistenceConfig()
        .scopeId(orgId)
        .scopeType(ScopeType.ORGANIZATION)
        .secretPersistenceType(SecretPersistenceType.AWS)

    val runtimeSecretPersistence = RuntimeSecretPersistence(mockk())

    mockkStatic(SecretPersistenceConfigHelper::class)
    every { SecretPersistenceConfigHelper.fromApiSecretPersistenceConfig(secretConfig) } returns runtimeSecretPersistence

    every { secretsApiClient.getSecretsPersistenceConfig(any()) } returns secretConfig
    every { secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(unhyrdatedConfig, runtimeSecretPersistence) } returns hydratedConfig

    every { featureFlagClient.boolVariation(eq(UseRuntimeSecretPersistence), eq(Organization(orgId))) } returns true

    val result = hydrator.hydrateConfig(unhyrdatedConfig, orgId)

    verify { secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(unhyrdatedConfig, runtimeSecretPersistence) }

    Assertions.assertEquals(hydratedConfig, result)
  }

  @Test
  fun `uses default hydration if ff not enabled for organization id`() {
    val secretsRepositoryReader: SecretsRepositoryReader = mockk()
    val secretsApiClient: SecretsPersistenceConfigApi = mockk()
    val featureFlagClient: FeatureFlagClient = mockk()

    val hydrator =
      BaseInputHydrator(
        secretsRepositoryReader,
        secretsApiClient,
        featureFlagClient,
      )

    val unhyrdatedConfig = POJONode("un-hydrated")
    val hydratedConfig = POJONode("hydrated")

    val orgId = UUID.randomUUID()

    every { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(unhyrdatedConfig) } returns hydratedConfig

    every { featureFlagClient.boolVariation(eq(UseRuntimeSecretPersistence), eq(Organization(orgId))) } returns false

    val result = hydrator.hydrateConfig(unhyrdatedConfig, orgId)

    verify { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(unhyrdatedConfig) }

    Assertions.assertEquals(hydratedConfig, result)
  }
}
