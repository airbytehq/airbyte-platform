/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.model.generated.ProblemMapperIdData
import io.airbyte.api.problems.throwable.generated.MapperSecretNotFoundProblem
import io.airbyte.api.problems.throwable.generated.RuntimeSecretsManagerRequiredProblem
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperConfig
import io.airbyte.config.ScopeType
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.mapper.configs.EncryptionConfig.Companion.ALGO_AES
import io.airbyte.config.mapper.configs.EncryptionMapperConfig
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretsHelpers
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.AllowMappersDefaultSecretPersistence
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.mappers.transformations.Mapper
import io.airbyte.mappers.transformations.MapperSpec
import io.airbyte.metrics.MetricClient
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import secrets.persistence.SecretCoordinateException
import java.util.UUID

@Singleton
class MapperSecretHelper(
  private val mappers: Map<String, Mapper<MapperConfig>>,
  private val workspaceService: WorkspaceService,
  private val secretPersistenceConfigService: SecretPersistenceConfigService,
  private val secretsRepositoryWriter: SecretsRepositoryWriter,
  private val secretsRepositoryReader: SecretsRepositoryReader,
  @Named("jsonSecretsProcessorWithCopy") private val secretsProcessor: JsonSecretsProcessor,
  private val featureFlagClient: FeatureFlagClient,
  private val airbyteEdition: AirbyteEdition,
  private val metricClient: MetricClient,
) {
  @Inject
  constructor(
    mappers: List<Mapper<MapperConfig>>,
    workspaceService: WorkspaceService,
    secretPersistenceConfigService: SecretPersistenceConfigService,
    secretsRepositoryWriter: SecretsRepositoryWriter,
    secretsRepositoryReader: SecretsRepositoryReader,
    @Named("jsonSecretsProcessorWithCopy") secretsProcessor: JsonSecretsProcessor,
    featureFlagClient: FeatureFlagClient,
    airbyteEdition: AirbyteEdition,
    metricClient: MetricClient,
  ) : this(
    mappers.associateBy { it.name },
    workspaceService,
    secretPersistenceConfigService,
    secretsRepositoryWriter,
    secretsRepositoryReader,
    secretsProcessor,
    featureFlagClient,
    airbyteEdition,
    metricClient,
  )

  private fun getMapper(name: String): Mapper<MapperConfig> = mappers[name] ?: throw IllegalArgumentException("Mapper $name not found")

  private fun specHasSecrets(spec: JsonNode): Boolean = SecretsHelpers.getSortedSecretPaths(spec).isNotEmpty()

  internal fun shouldRequireRuntimePersistence(
    mapperConfig: MapperConfig,
    organizationId: UUID,
  ): Boolean {
    if (airbyteEdition != AirbyteEdition.CLOUD) {
      return false
    }

    if (featureFlagClient.boolVariation(AllowMappersDefaultSecretPersistence, Organization(organizationId))) {
      return false
    }

    return when (mapperConfig) {
      is EncryptionMapperConfig -> ALGO_AES == mapperConfig.config.algorithm
      else -> false
    }
  }

  private fun getRuntimeSecretPersistence(organizationId: UUID): RuntimeSecretPersistence? {
    val isRuntimePersistenceEnabled = featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId))
    if (!isRuntimePersistenceEnabled) {
      return null
    }
    val secretPersistenceConfig = secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId)
    return RuntimeSecretPersistence(secretPersistenceConfig, metricClient)
  }

  private fun assertConfigHasNoMaskedSecrets(
    config: JsonNode,
    mapperId: UUID?,
    mapperType: String,
  ) {
    val configAsString = Jsons.serialize(config)
    if (configAsString.contains(AirbyteSecretConstants.SECRETS_MASK)) {
      throw MapperSecretNotFoundProblem(ProblemMapperIdData().mapperId(mapperId).mapperType(mapperType))
    }
  }

  private fun handleMapperConfigSecrets(
    mapperConfig: MapperConfig,
    workspaceId: UUID,
    organizationId: UUID,
  ): MapperConfig = handleMapperConfigSecrets(mapperConfig, existingMapperConfig = null, workspaceId, organizationId)

  private fun handleMapperConfigSecrets(
    mapperConfig: MapperConfig,
    existingMapperConfig: MapperConfig?,
    workspaceId: UUID,
    organizationId: UUID,
  ): MapperConfig {
    val mapperName = mapperConfig.name()
    val mapperInstance = getMapper(mapperName)
    val mapperConfigSchema = getConfigSchema(mapperInstance.spec())

    if (!specHasSecrets(mapperConfigSchema)) {
      // Nothing to do, no secrets in spec
      return mapperConfig
    }

    val secretPersistence = getRuntimeSecretPersistence(organizationId)
    val requireRuntimePersistence = shouldRequireRuntimePersistence(mapperConfig, organizationId)
    if (requireRuntimePersistence && secretPersistence == null) {
      throw RuntimeSecretsManagerRequiredProblem()
    }

    val persistedConfigAsJson = existingMapperConfig?.let { Jsons.jsonNode(it.config()) }
    val hydratedPersistedConfig = tryHydrateConfigJson(persistedConfigAsJson, secretPersistence)

    val newConfigJson =
      if (hydratedPersistedConfig != null) {
        // copy any necessary secrets from the current mapper to the incoming updated mapper
        val configWithSecrets =
          secretsProcessor.copySecrets(
            hydratedPersistedConfig,
            Jsons.jsonNode(mapperConfig.config()),
            mapperConfigSchema,
          )
        secretsRepositoryWriter.updateFromConfigLegacy(
          workspaceId,
          persistedConfigAsJson!!,
          configWithSecrets,
          mapperConfigSchema,
          secretPersistence,
        )
      } else {
        val configWithSecrets = Jsons.jsonNode(mapperConfig.config())
        assertConfigHasNoMaskedSecrets(configWithSecrets, mapperConfig.id(), mapperName)
        secretsRepositoryWriter.createFromConfigLegacy(
          workspaceId,
          configWithSecrets,
          mapperConfigSchema,
          secretPersistence,
        )
      }

    return mapperInstance.spec().deserialize(ConfiguredMapper(mapperName, newConfigJson, mapperConfig.id()))
  }

  /**
   * Detects secrets in mapper configurations and writes them to the secrets store. It returns the
   * connection configuration stripped of secrets (replaced with pointers to the secrets store).
   */
  fun createAndReplaceMapperSecrets(
    workspaceId: UUID,
    catalog: ConfiguredAirbyteCatalog,
  ): ConfiguredAirbyteCatalog {
    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()
    return catalog.copy(
      streams =
        catalog.streams.map { stream ->
          stream.copy(
            mappers =
              stream.mappers.map {
                handleMapperConfigSecrets(it, workspaceId, organizationId)
              },
          )
        },
    )
  }

  private fun getStreamUpdatedMappers(
    stream: ConfiguredAirbyteStream,
    oldStream: ConfiguredAirbyteStream?,
    workspaceId: UUID,
    organizationId: UUID,
  ): List<MapperConfig> {
    val oldMappersById = oldStream?.mappers?.filter { it.id() != null }?.associateBy { it.id() } ?: emptyMap()
    return stream.mappers.map {
      val existingConfig = it.id().let { id -> oldMappersById[id] }
      handleMapperConfigSecrets(it, existingConfig, workspaceId, organizationId)
    }
  }

  fun updateAndReplaceMapperSecrets(
    workspaceId: UUID,
    oldCatalog: ConfiguredAirbyteCatalog,
    newCatalog: ConfiguredAirbyteCatalog,
  ): ConfiguredAirbyteCatalog {
    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()
    val oldStreams = oldCatalog.streams.associateBy { StreamDescriptor().withName(it.stream.name).withNamespace(it.stream.namespace) }
    return newCatalog.copy(
      streams =
        newCatalog.streams.map { stream ->
          val streamDescriptor =
            StreamDescriptor()
              .withName(stream.stream.name)
              .withNamespace(stream.stream.namespace)
          val oldStream = oldStreams[streamDescriptor]
          stream.copy(
            mappers = getStreamUpdatedMappers(stream, oldStream, workspaceId, organizationId),
          )
        },
    )
  }

  private fun getConfigSchema(mapperSpec: MapperSpec<*>): JsonNode {
    val mapperSpecSchema = mapperSpec.jsonSchema()
    if (!mapperSpecSchema.has("properties") || !mapperSpecSchema.get("properties").has("config")) {
      throw IllegalStateException("Mapper spec schema does not have a config property")
    }
    return mapperSpecSchema.get("properties").get("config")
  }

  private fun maskMapperConfigSecrets(mapperConfig: MapperConfig): MapperConfig {
    val mapperName = mapperConfig.name()
    val mapperInstance = getMapper(mapperName)
    val mapperConfigSchema = getConfigSchema(mapperInstance.spec())
    val maskedConfig = secretsProcessor.prepareSecretsForOutput(Jsons.jsonNode(mapperConfig.config()), mapperConfigSchema)
    return mapperInstance.spec().deserialize(ConfiguredMapper(mapperName, maskedConfig, mapperConfig.id()))
  }

  private fun maskMapperSecretsForStream(stream: ConfiguredAirbyteStream): ConfiguredAirbyteStream =
    stream.copy(
      mappers =
        stream.mappers.map {
          maskMapperConfigSecrets(it)
        },
    )

  /**
   * Given a catalog with mapper configurations, mask the secrets in the configurations.
   */
  fun maskMapperSecrets(catalog: ConfiguredAirbyteCatalog): ConfiguredAirbyteCatalog =
    catalog.copy(
      streams =
        catalog.streams.map {
          maskMapperSecretsForStream(it)
        },
    )

  private fun tryHydrateConfigJson(
    persistedConfigJson: JsonNode?,
    runtimeSecretPersistence: RuntimeSecretPersistence?,
  ): JsonNode? {
    if (persistedConfigJson == null) {
      return null
    }

    return try {
      if (runtimeSecretPersistence != null) {
        secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(
          persistedConfigJson,
          runtimeSecretPersistence,
        )
      } else {
        secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(persistedConfigJson)
      }
    } catch (e: SecretCoordinateException) {
      // Some secret is missing, treat as a new config
      null
    }
  }
}
