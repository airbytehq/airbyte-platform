/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs

import com.google.common.io.Resources
import io.airbyte.commons.constants.AirbyteCatalogConstants
import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorRegistry
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.charset.StandardCharsets
import java.util.ArrayList
import java.util.UUID
import java.util.stream.Collectors

private val logger = KotlinLogging.logger {}

/**
 * This provider contains all definitions according to the local registry json files.
 */
class LocalDefinitionsProvider : DefinitionsProvider {
  companion object {
    private val LOCAL_CONNECTOR_REGISTRY_PATH: String =
      EnvVar.LOCAL_CONNECTOR_CATALOG_PATH.fetch(AirbyteCatalogConstants.DEFAULT_LOCAL_CONNECTOR_CATALOG_PATH)
        ?: throw IllegalStateException("LOCAL_CONNECTOR_REGISTRY_PATH cannot be null")
  }

  /**
   * Get connector registry.
   *
   * @return connector registry
   */
  fun getLocalConnectorRegistry(): ConnectorRegistry {
    try {
      val url = Resources.getResource(LOCAL_CONNECTOR_REGISTRY_PATH)
      logger.info { "Loading $LOCAL_CONNECTOR_REGISTRY_PATH definitions from local connector registry $url" }

      val jsonString = Resources.toString(url, StandardCharsets.UTF_8)
      return Jsons.deserialize(jsonString, ConnectorRegistry::class.java)
    } catch (e: Exception) {
      throw RuntimeException("Failed to fetch local connector registry", e)
    }
  }

  /**
   * Get map of source definition ids to the definition.
   *
   * @return map
   */
  fun getSourceDefinitionsMap(): Map<UUID, ConnectorRegistrySourceDefinition> {
    val registry = getLocalConnectorRegistry()
    return registry.sources.stream().collect(
      Collectors.toMap(
        { it.sourceDefinitionId },
        { source ->
          source.withProtocolVersion(
            AirbyteProtocolVersion.getWithDefault(source.spec?.protocolVersion).serialize(),
          )
        },
      ),
    )
  }

  /**
   * Get map of destination definition ids to the definition.
   *
   * @return map
   */
  fun getDestinationDefinitionsMap(): Map<UUID, ConnectorRegistryDestinationDefinition> {
    val registry = getLocalConnectorRegistry()
    return registry.destinations.stream().collect(
      Collectors.toMap(
        { it.destinationDefinitionId },
        { destination ->
          destination.withProtocolVersion(
            AirbyteProtocolVersion
              .getWithDefault(
                destination.spec?.protocolVersion,
              ).serialize(),
          )
        },
      ),
    )
  }

  override fun getSourceDefinition(definitionId: UUID): ConnectorRegistrySourceDefinition {
    val definition =
      getSourceDefinitionsMap()[definitionId]
        ?: throw RegistryDefinitionNotFoundException(ActorType.SOURCE, definitionId)
    return definition
  }

  override fun getSourceDefinitions(): List<ConnectorRegistrySourceDefinition> = ArrayList(getSourceDefinitionsMap().values)

  override fun getDestinationDefinition(definitionId: UUID): ConnectorRegistryDestinationDefinition {
    val definition =
      getDestinationDefinitionsMap()[definitionId]
        ?: throw RegistryDefinitionNotFoundException(ActorType.DESTINATION, definitionId)
    return definition
  }

  override fun getDestinationDefinitions(): List<ConnectorRegistryDestinationDefinition> = ArrayList(getDestinationDefinitionsMap().values)
}
