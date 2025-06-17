/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.api.client.model.generated.SecretPersistenceConfig
import io.airbyte.api.client.model.generated.SecretPersistenceType
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteSecret
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.MapperConfig
import io.airbyte.config.SyncMode
import io.airbyte.config.mapper.configs.AesEncryptionConfig
import io.airbyte.config.mapper.configs.AesMode
import io.airbyte.config.mapper.configs.AesPadding
import io.airbyte.config.mapper.configs.EncryptionConfig
import io.airbyte.config.mapper.configs.EncryptionMapperConfig
import io.airbyte.config.mapper.configs.HashingConfig
import io.airbyte.config.mapper.configs.HashingMapperConfig
import io.airbyte.config.mapper.configs.HashingMethods
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.mappers.transformations.EncryptionMapper
import io.airbyte.mappers.transformations.HashingMapper
import io.airbyte.mappers.transformations.Mapper
import io.airbyte.metrics.MetricClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class MapperSecretHydrationHelperTest {
  companion object {
    private const val SECRET_VALUE = "my_secret_value"
    private const val SECRET_COORDINATE = "airbyte_coordinate"
    private val ORGANIZATION_ID = UUID.randomUUID()
  }

  private val airbyteApiClient = mockk<AirbyteApiClient>()
  private val secretsRepositoryReader = mockk<SecretsRepositoryReader>(relaxed = true)
  private val objectMapper = MoreMappers.initMapper()
  private val hashingMapper = HashingMapper(objectMapper)
  private val encryptionMapper = EncryptionMapper(objectMapper)
  private val metricClient = mockk<MetricClient>(relaxed = true)

  @Suppress("UNCHECKED_CAST")
  private val mapperSecretHydrationHelper =
    MapperSecretHydrationHelper(
      mappers = listOf(encryptionMapper as Mapper<MapperConfig>, hashingMapper as Mapper<MapperConfig>),
      secretsRepositoryReader = secretsRepositoryReader,
      airbyteApiClient = airbyteApiClient,
      metricClient = metricClient,
    )

  @BeforeEach
  fun setup() {
    every {
      airbyteApiClient.secretPersistenceConfigApi.getSecretsPersistenceConfig(any())
    } returns SecretPersistenceConfig(SecretPersistenceType.TESTING, Jsons.emptyObject(), ScopeType.ORGANIZATION, ORGANIZATION_ID)
  }

  @Test
  fun `test hydrate mapper config`() {
    val mapperConfig =
      EncryptionMapperConfig(
        config =
          AesEncryptionConfig(
            algorithm = EncryptionConfig.ALGO_AES,
            targetField = "target",
            mode = AesMode.CBC,
            padding = AesPadding.NoPadding,
            key = AirbyteSecret.Reference(SECRET_COORDINATE),
          ),
      )

    val mapperConfigJson = Jsons.jsonNode(mapperConfig.config())
    val configWithSecrets =
      Jsons.jsonNode(
        mapOf(
          "algorithm" to "AES",
          "targetField" to "target",
          "mode" to "CBC",
          "padding" to "NoPadding",
          "key" to SECRET_VALUE,
        ),
      )

    every { secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(eq(mapperConfigJson), any()) } returns configWithSecrets

    val catalog = generateCatalogWithMapper(mapperConfig)
    val hydratedConfig = mapperSecretHydrationHelper.hydrateMapperSecrets(catalog, true, ORGANIZATION_ID)

    val expectedConfig =
      mapperConfig.copy(
        config =
          AesEncryptionConfig(
            algorithm = EncryptionConfig.ALGO_AES,
            targetField = "target",
            mode = AesMode.CBC,
            padding = AesPadding.NoPadding,
            key = AirbyteSecret.Hydrated(SECRET_VALUE),
          ),
      )

    Assertions.assertEquals(
      expectedConfig,
      hydratedConfig.streams
        .first()
        .mappers
        .first(),
    )

    verify { secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(eq(mapperConfigJson), any()) }
  }

  @Test
  fun `test without secrets in spec does not try to hydrate secrets`() {
    val mapperConfig =
      HashingMapperConfig(config = HashingConfig(targetField = "target", method = HashingMethods.SHA256, fieldNameSuffix = "_hashed"))
    val catalog = generateCatalogWithMapper(mapperConfig)

    val resultingCatalog = mapperSecretHydrationHelper.hydrateMapperSecrets(catalog, true, ORGANIZATION_ID)
    Assertions.assertEquals(
      mapperConfig,
      resultingCatalog.streams
        .first()
        .mappers
        .first(),
    )

    verify(exactly = 0) {
      airbyteApiClient.secretPersistenceConfigApi.getSecretsPersistenceConfig(any())
      secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(any(), any())
      secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(any())
    }
  }

  private fun generateCatalogWithMapper(mapperConfig: MapperConfig): ConfiguredAirbyteCatalog =
    ConfiguredAirbyteCatalog(
      listOf(
        ConfiguredAirbyteStream
          .Builder()
          .stream(
            mockk<AirbyteStream>(),
          ).syncMode(SyncMode.FULL_REFRESH)
          .destinationSyncMode(DestinationSyncMode.OVERWRITE)
          .mappers(listOf(mapperConfig))
          .build(),
      ),
    )
}
