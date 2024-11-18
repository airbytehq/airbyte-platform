package io.airbyte.commons.server.handlers.helpers

import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.helpers.ConnectionHelpers
import io.airbyte.config.AirbyteSecret
import io.airbyte.config.AirbyteStream
import io.airbyte.config.Configs.DeploymentMode
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.MapperConfig
import io.airbyte.config.ScopeType
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.SyncMode
import io.airbyte.config.mapper.configs.AesEncryptionConfig
import io.airbyte.config.mapper.configs.AesMode
import io.airbyte.config.mapper.configs.AesPadding
import io.airbyte.config.mapper.configs.EncryptionConfig
import io.airbyte.config.mapper.configs.EncryptionMapperConfig
import io.airbyte.config.mapper.configs.HashingConfig
import io.airbyte.config.mapper.configs.HashingMapperConfig
import io.airbyte.config.mapper.configs.HashingMethods
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.mappers.transformations.EncryptionMapper
import io.airbyte.mappers.transformations.HashingMapper
import io.airbyte.mappers.transformations.Mapper
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

internal class MapperSecretHelperTest {
  companion object {
    private const val SECRET_VALUE = "my_secret_value"
    private const val SECRET_COORDINATE = "airbyte_coordinate"
    private val WORKSPACE_ID = UUID.randomUUID()
    private val ORGANIZATION_ID = UUID.randomUUID()
  }

  private val workspaceService = mockk<WorkspaceService>()
  private val secretPersistenceConfigService = mockk<SecretPersistenceConfigService>()
  private val secretsRepositoryWriter = mockk<SecretsRepositoryWriter>()
  private val secretsProcessor = mockk<JsonSecretsProcessor>()
  private val featureFlagClient = mockk<TestClient>()

  private val hashingMapper = HashingMapper()
  private val encryptionMapper = EncryptionMapper()

  private val mapperSecretHelper =
    MapperSecretHelper(
      mappers = listOf(encryptionMapper as Mapper<MapperConfig>, hashingMapper as Mapper<MapperConfig>),
      workspaceService = workspaceService,
      secretPersistenceConfigService = secretPersistenceConfigService,
      secretsRepositoryWriter = secretsRepositoryWriter,
      featureFlagClient = featureFlagClient,
      secretsProcessor = secretsProcessor,
      deploymentMode = DeploymentMode.CLOUD,
    )

  @BeforeEach
  fun setup() {
    every { workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID) } returns Optional.of(ORGANIZATION_ID)
    every { featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(ORGANIZATION_ID)) } returns true
    every { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, ORGANIZATION_ID) } returns mockk<SecretPersistenceConfig>()
  }

  @Test
  fun `test create mapper secrets`() {
    val mapperConfig =
      EncryptionMapperConfig(
        config =
          AesEncryptionConfig(
            algorithm = EncryptionConfig.ALGO_AES,
            targetField = "target",
            mode = AesMode.CBC,
            padding = AesPadding.NoPadding,
            key = AirbyteSecret.Hydrated(SECRET_VALUE),
          ),
      )
    val catalogWithSecrets = generateCatalogWithMapper(mapperConfig)

    val configSpec = encryptionMapper.spec().jsonSchema().get("properties").get("config")
    val configNoSecrets =
      Jsons.jsonNode(
        mapOf(
          "algorithm" to "AES",
          "targetField" to "target",
          "mode" to "CBC",
          "padding" to "NoPadding",
          "key" to mapOf("_secret" to SECRET_COORDINATE),
        ),
      )

    every {
      secretsRepositoryWriter.createFromConfig(eq(WORKSPACE_ID), eq(Jsons.jsonNode(mapperConfig.config())), eq(configSpec), any())
    } returns configNoSecrets

    val catalogWithoutSecrets = mapperSecretHelper.createAndReplaceMapperSecrets(WORKSPACE_ID, catalogWithSecrets)

    val expectedConfig =
      mapperConfig.copy(
        config =
          AesEncryptionConfig(
            algorithm = EncryptionConfig.ALGO_AES,
            targetField = "target",
            mode = AesMode.CBC,
            padding = AesPadding.NoPadding,
            key = AirbyteSecret.Reference(SECRET_COORDINATE),
          ),
      )

    assertEquals(expectedConfig, catalogWithoutSecrets.streams.first().mappers.first())

    verify { secretsRepositoryWriter.createFromConfig(eq(WORKSPACE_ID), eq(Jsons.jsonNode(mapperConfig.config())), eq(configSpec), any()) }
  }

  @Test
  fun `test without secrets in spec does not try to use secret persistence`() {
    val mapperConfig =
      HashingMapperConfig(config = HashingConfig(targetField = "target", method = HashingMethods.SHA256, fieldNameSuffix = "_hashed"))
    val catalog = generateCatalogWithMapper(mapperConfig)

    val resultingCatalog = mapperSecretHelper.createAndReplaceMapperSecrets(WORKSPACE_ID, catalog)
    assertEquals(mapperConfig, resultingCatalog.streams.first().mappers.first())

    verify(exactly = 0) {
      secretPersistenceConfigService.get(any(), any())
      secretsRepositoryWriter.createFromConfig(any(), any(), any(), any())
    }
  }

  @Test
  fun `test mask secrets for output`() {
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

    val configSpec = encryptionMapper.spec().jsonSchema().get("properties").get("config")
    val maskedConfig =
      Jsons.jsonNode(
        mapOf(
          "algorithm" to "AES",
          "targetField" to "target",
          "mode" to "CBC",
          "padding" to "NoPadding",
          "key" to AirbyteSecretConstants.SECRETS_MASK,
        ),
      )

    every { secretsProcessor.prepareSecretsForOutput(Jsons.jsonNode(mapperConfig.config()), configSpec) } returns maskedConfig

    val catalog = generateCatalogWithMapper(mapperConfig)
    val maskedCatalog = mapperSecretHelper.maskMapperSecrets(catalog)

    val expectedConfig =
      mapperConfig.copy(
        config =
          AesEncryptionConfig(
            algorithm = EncryptionConfig.ALGO_AES,
            targetField = "target",
            mode = AesMode.CBC,
            padding = AesPadding.NoPadding,
            key = AirbyteSecret.Hydrated(AirbyteSecretConstants.SECRETS_MASK),
          ),
      )

    assertEquals(expectedConfig, maskedCatalog.streams.first().mappers.first())

    verify { secretsProcessor.prepareSecretsForOutput(Jsons.jsonNode(mapperConfig.config()), configSpec) }
  }

  @Test
  fun `test update with masked secrets and no mapper changes resolves old secrets`() {
    val maskedExistingMapperConfig =
      EncryptionMapperConfig(
        config =
          AesEncryptionConfig(
            algorithm = EncryptionConfig.ALGO_AES,
            targetField = "target",
            mode = AesMode.CBC,
            padding = AesPadding.NoPadding,
            key = AirbyteSecret.Hydrated(AirbyteSecretConstants.SECRETS_MASK),
          ),
      )
    val catalogWithMaskedSecrets = generateCatalogWithMapper(maskedExistingMapperConfig)
    val addedConfiguredStream =
      ConfiguredAirbyteStream.Builder()
        .stream(AirbyteStream("new_stream", Jsons.emptyObject(), listOf()))
        .syncMode(SyncMode.FULL_REFRESH)
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .mappers(listOf())
        .build()
    catalogWithMaskedSecrets.withStreams(listOf(catalogWithMaskedSecrets.streams.first(), addedConfiguredStream))

    val referencedMapperConfig =
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
    val persistedCatalog = generateCatalogWithMapper(referencedMapperConfig)

    val maskedConfig =
      Jsons.jsonNode(
        mapOf(
          "algorithm" to "AES",
          "targetField" to "target",
          "mode" to "CBC",
          "padding" to "NoPadding",
          "key" to AirbyteSecretConstants.SECRETS_MASK,
        ),
      )

    val configSpec = encryptionMapper.spec().jsonSchema().get("properties").get("config")
    every { secretsProcessor.prepareSecretsForOutput(Jsons.jsonNode(referencedMapperConfig.config()), configSpec) } returns maskedConfig

    val catalogWithoutSecrets = mapperSecretHelper.updateAndReplaceMapperSecrets(WORKSPACE_ID, persistedCatalog, catalogWithMaskedSecrets)
    assertEquals(2, catalogWithoutSecrets.streams.size)
    assertEquals(addedConfiguredStream, catalogWithoutSecrets.streams.last())

    // The important part: return the persisted catalog's mappers (with coordinates) rather than the mask
    assertEquals(referencedMapperConfig, catalogWithoutSecrets.streams.first().mappers.first())
  }

  @Test
  fun `test updating mapper config with masked secrets is not supported`() {
    val maskedUpdatedMapperConfig =
      EncryptionMapperConfig(
        config =
          AesEncryptionConfig(
            algorithm = EncryptionConfig.ALGO_AES,
            targetField = "some_other_target",
            mode = AesMode.CBC,
            padding = AesPadding.NoPadding,
            key = AirbyteSecret.Hydrated(AirbyteSecretConstants.SECRETS_MASK),
          ),
      )
    val catalogWithMaskedSecrets = generateCatalogWithMapper(maskedUpdatedMapperConfig)

    val referencedMapperConfig =
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
    val persistedCatalog = generateCatalogWithMapper(referencedMapperConfig)

    val maskedConfig =
      Jsons.jsonNode(
        mapOf(
          "algorithm" to "AES",
          "targetField" to "target",
          "mode" to "CBC",
          "padding" to "NoPadding",
          "key" to AirbyteSecretConstants.SECRETS_MASK,
        ),
      )

    val configSpec = encryptionMapper.spec().jsonSchema().get("properties").get("config")
    every { secretsProcessor.prepareSecretsForOutput(Jsons.jsonNode(referencedMapperConfig.config()), configSpec) } returns maskedConfig

    assertThrows<IllegalArgumentException> {
      mapperSecretHelper.updateAndReplaceMapperSecrets(WORKSPACE_ID, persistedCatalog, catalogWithMaskedSecrets)
    }
  }

  private fun generateCatalogWithMapper(mapperConfig: MapperConfig): ConfiguredAirbyteCatalog {
    val catalog = ConnectionHelpers.generateBasicConfiguredAirbyteCatalog()
    catalog.streams.first().mappers = listOf(mapperConfig)
    return catalog
  }
}
