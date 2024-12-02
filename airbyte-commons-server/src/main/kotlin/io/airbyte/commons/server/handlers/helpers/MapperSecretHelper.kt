package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.throwable.generated.RuntimeSecretsManagerRequiredProblem
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs.DeploymentMode
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperConfig
import io.airbyte.config.ScopeType
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretsHelpers
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
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class MapperSecretHelper(
  private val mappers: Map<String, Mapper<MapperConfig>>,
  private val workspaceService: WorkspaceService,
  private val secretPersistenceConfigService: SecretPersistenceConfigService,
  private val secretsRepositoryWriter: SecretsRepositoryWriter,
  @Named("jsonSecretsProcessorWithCopy") private val secretsProcessor: JsonSecretsProcessor,
  private val featureFlagClient: FeatureFlagClient,
  private val deploymentMode: DeploymentMode,
) {
  @Inject
  constructor(
    mappers: List<Mapper<MapperConfig>>,
    workspaceService: WorkspaceService,
    secretPersistenceConfigService: SecretPersistenceConfigService,
    secretsRepositoryWriter: SecretsRepositoryWriter,
    @Named("jsonSecretsProcessorWithCopy") secretsProcessor: JsonSecretsProcessor,
    featureFlagClient: FeatureFlagClient,
    deploymentMode: DeploymentMode,
  ) : this(
    mappers.associateBy { it.name },
    workspaceService,
    secretPersistenceConfigService,
    secretsRepositoryWriter,
    secretsProcessor,
    featureFlagClient,
    deploymentMode,
  )

  private fun getMapper(name: String): Mapper<MapperConfig> {
    return mappers[name] ?: throw IllegalArgumentException("Mapper $name not found")
  }

  private fun specHasSecrets(spec: JsonNode): Boolean {
    return SecretsHelpers.getSortedSecretPaths(spec).isNotEmpty()
  }

  private fun getSecretPersistence(organizationId: UUID): RuntimeSecretPersistence? {
    val isRuntimePersistenceEnabled = featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId))
    if (!isRuntimePersistenceEnabled) {
      if (deploymentMode == DeploymentMode.CLOUD &&
        !featureFlagClient.boolVariation(
          AllowMappersDefaultSecretPersistence,
          Organization(organizationId),
        )
      ) {
        throw RuntimeSecretsManagerRequiredProblem()
      }
      return null
    }
    val secretPersistenceConfig = secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId)
    return RuntimeSecretPersistence(secretPersistenceConfig)
  }

  private fun handleMapperConfigSecrets(
    mapperConfig: MapperConfig,
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

    val configAsJson = Jsons.jsonNode(mapperConfig.config())
    val secretPersistence = getSecretPersistence(organizationId)

    val configAsString = Jsons.serialize(configAsJson)
    val doesConfigHaveSecretMask = configAsString.contains(AirbyteSecretConstants.SECRETS_MASK)
    if (doesConfigHaveSecretMask) {
      // TODO(pedro): This is here to prevent the secret mask from overwriting actual secrets. If we hit this codepath, there's either a bug or an API user is attempting to save a secret mask.
      // Updates without specifying the secret value are not currently supported.
      throw IllegalArgumentException("Attempting to store masked secrets")
    }

    val newConfigJson =
      secretsRepositoryWriter.createFromConfig(
        workspaceId,
        configAsJson,
        mapperConfigSchema,
        secretPersistence,
      )

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
    // TODO(pedro): implement updates correctly once mapper have IDs so we can keep secrets that have been set before

    if (oldStream != null && maskMapperSecretsForStream(oldStream).mappers == stream.mappers) {
      // HACK: If the old catalog (masked)'s mappers are the same as the incoming catalog's mappers, don't try to do anything to mappers
      // This is a workaround so that internal calls with masked mappers continue to function properly before implementing mapper IDs
      return oldStream.mappers
    }

    return stream.mappers.map {
      handleMapperConfigSecrets(it, workspaceId, organizationId)
    }
  }

  fun updateAndReplaceMapperSecrets(
    workspaceId: UUID,
    oldCatalog: ConfiguredAirbyteCatalog,
    newCatalog: ConfiguredAirbyteCatalog,
  ): ConfiguredAirbyteCatalog {
    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()
    val oldStreams = oldCatalog.streams.associateBy { it.stream.name }
    return newCatalog.copy(
      streams =
        newCatalog.streams.map { stream ->
          val oldStream = oldStreams[stream.stream.name]
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

  private fun maskMapperSecretsForStream(stream: ConfiguredAirbyteStream): ConfiguredAirbyteStream {
    return stream.copy(
      mappers =
        stream.mappers.map {
          maskMapperConfigSecrets(it)
        },
    )
  }

  /**
   * Given a catalog with mapper configurations, mask the secrets in the configurations.
   */
  fun maskMapperSecrets(catalog: ConfiguredAirbyteCatalog): ConfiguredAirbyteCatalog {
    return catalog.copy(
      streams =
        catalog.streams.map {
          maskMapperSecretsForStream(it)
        },
    )
  }
}
