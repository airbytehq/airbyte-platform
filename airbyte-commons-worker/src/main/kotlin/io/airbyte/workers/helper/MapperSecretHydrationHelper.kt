package io.airbyte.workers.helper

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.api.client.model.generated.SecretPersistenceConfig
import io.airbyte.api.client.model.generated.SecretPersistenceConfigGetRequestBody
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperConfig
import io.airbyte.config.secrets.SecretsHelpers
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.mappers.transformations.Mapper
import io.airbyte.mappers.transformations.MapperSpec
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class MapperSecretHydrationHelper(
  private val mappers: List<Mapper<MapperConfig>>,
  private val secretsRepositoryReader: SecretsRepositoryReader,
  private val airbyteApiClient: AirbyteApiClient,
) {
  private fun getMapper(name: String): Mapper<MapperConfig> {
    return mappers.first { it.name == name }
  }

  private fun specHasSecrets(spec: JsonNode): Boolean {
    return SecretsHelpers.getSortedSecretPaths(spec).isNotEmpty()
  }

  private fun getConfigSchema(mapperSpec: MapperSpec<*>): JsonNode {
    val mapperSpecSchema = mapperSpec.jsonSchema()
    if (!mapperSpecSchema.has("properties") || !mapperSpecSchema.get("properties").has("config")) {
      throw IllegalStateException("Mapper spec schema does not have a config property")
    }
    return mapperSpecSchema.get("properties").get("config")
  }

  private fun getRuntimeSecretPersistence(organizationId: UUID): RuntimeSecretPersistence {
    val secretPersistenceConfig: SecretPersistenceConfig =
      airbyteApiClient.secretPersistenceConfigApi.getSecretsPersistenceConfig(
        SecretPersistenceConfigGetRequestBody(ScopeType.ORGANIZATION, organizationId),
      )
    return RuntimeSecretPersistence(
      io.airbyte.config.SecretPersistenceConfig().withScopeType(
        Enums.convertTo(secretPersistenceConfig.scopeType, io.airbyte.config.ScopeType::class.java),
      )
        .withScopeId(secretPersistenceConfig.scopeId)
        .withConfiguration(Jsons.deserializeToStringMap(secretPersistenceConfig.configuration))
        .withSecretPersistenceType(
          Enums.convertTo(
            secretPersistenceConfig.secretPersistenceType,
            io.airbyte.config.SecretPersistenceConfig.SecretPersistenceType::class.java,
          ),
        ),
    )
  }

  private fun hydrateMapperConfigSecrets(
    mapperConfig: MapperConfig,
    organizationId: UUID?,
    useRuntimePersistence: Boolean,
  ): MapperConfig {
    val mapperName = mapperConfig.name()
    val mapperInstance = getMapper(mapperName)
    val mapperConfigSchema = getConfigSchema(mapperInstance.spec())

    if (!specHasSecrets(mapperConfigSchema)) {
      // Nothing to do, no secrets in spec
      return mapperConfig
    }

    val configAsJson = Jsons.jsonNode(mapperConfig.config())

    val hydratedConfigJson =
      if (useRuntimePersistence && organizationId != null) {
        val secretPersistence = getRuntimeSecretPersistence(organizationId)
        secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(
          configAsJson,
          secretPersistence,
        )
      } else {
        secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(configAsJson)
      }

    val hydratedConfig: MapperConfig = mapperInstance.spec().deserialize(ConfiguredMapper(mapperName, hydratedConfigJson!!))
    return hydratedConfig
  }

  /**
   * Given a catalog with mapper configurations, hydrate the secrets in the configurations and return the hydrated catalog.
   */
  fun hydrateMapperSecrets(
    catalog: ConfiguredAirbyteCatalog,
    useRuntimePersistence: Boolean,
    organizationId: UUID?,
  ): ConfiguredAirbyteCatalog {
    return catalog.copy(
      streams =
        catalog.streams.map { stream ->
          stream.copy(
            mappers =
              stream.mappers.map {
                hydrateMapperConfigSecrets(it, organizationId, useRuntimePersistence)
              },
          )
        },
    )
  }
}
