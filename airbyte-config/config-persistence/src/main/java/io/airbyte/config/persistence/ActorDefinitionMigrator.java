/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AirbyteConfig;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes in most up-to-date source and destination definitions from the Airbyte connector catalog
 * and merges them with those already present in the database. See javadocs on methods for rules.
 */
@Singleton
@Requires(bean = ConfigRepository.class)
public class ActorDefinitionMigrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActorDefinitionMigrator.class);

  private static final String UNKNOWN_CONFIG_TYPE = "Unknown Config Type ";

  private final ConfigRepository configRepository;

  private final FeatureFlagClient featureFlagClient;

  public ActorDefinitionMigrator(final ConfigRepository configRepository, final FeatureFlagClient featureFlagClient) {
    this.configRepository = configRepository;
    this.featureFlagClient = featureFlagClient;
  }

  /**
   * Migrate to update to newest definitions.
   *
   * @param latestSources latest sources
   * @param latestDestinations latest destinations
   * @param updateAll whether to update all definitions regardless of if they are being used
   * @throws IOException exception while interacting with the db.
   */
  public void migrate(final List<ConnectorRegistrySourceDefinition> latestSources,
                      final List<ConnectorRegistryDestinationDefinition> latestDestinations,
                      final boolean updateAll)
      throws IOException, JsonValidationException {
    LOGGER.info("Updating connector definitions from the seed if necessary...");

    final Set<String> connectorRepositoriesInUse = configRepository.getConnectorRepositoriesInUse();
    LOGGER.info("Connectors in use: {}", connectorRepositoriesInUse);

    final Map<String, ConnectorInfo> connectorRepositoryToInfoMap = getConnectorRepositoryToInfoMap();
    LOGGER.info("Current connector versions: {}", connectorRepositoryToInfoMap.values());

    int newConnectorCount = 0;
    int updatedConnectorCount = 0;

    final ConnectorCounter sourceConnectorCounter = updateConnectorDefinitions(ConfigSchema.STANDARD_SOURCE_DEFINITION,
        latestSources, connectorRepositoriesInUse, connectorRepositoryToInfoMap, updateAll);
    newConnectorCount += sourceConnectorCounter.newCount;
    updatedConnectorCount += sourceConnectorCounter.updateCount;

    final ConnectorCounter destinationConnectorCounter = updateConnectorDefinitions(ConfigSchema.STANDARD_DESTINATION_DEFINITION,
        latestDestinations, connectorRepositoriesInUse, connectorRepositoryToInfoMap, updateAll);
    newConnectorCount += destinationConnectorCounter.newCount;
    updatedConnectorCount += destinationConnectorCounter.updateCount;

    LOGGER.info("Connector definitions have been updated ({} new connectors, and {} version updates)", newConnectorCount, updatedConnectorCount);
  }

  /**
   * Get connector docker image to connector definition info.
   *
   * @return A map about current connectors (both source and destination). It maps from connector
   *         repository to its definition id and docker image tag. We identify a connector by its
   *         repository name instead of definition id because connectors can be added manually by
   *         users, and are not always the same as those in the seed.
   */
  @VisibleForTesting
  Map<String, ConnectorInfo> getConnectorRepositoryToInfoMap() throws IOException {
    return configRepository.getCurrentConnectorInfo()
        .stream()
        .collect(Collectors.toMap(
            connectorInfo -> connectorInfo.dockerRepository,
            connectorInfo -> connectorInfo,
            (c1, c2) -> {
              final AirbyteVersion v1 = new AirbyteVersion(c1.dockerImageTag);
              final AirbyteVersion v2 = new AirbyteVersion(c2.dockerImageTag);
              LOGGER.warn("Duplicated connector version found for {}: {} ({}) vs {} ({})",
                  c1.dockerRepository, c1.dockerImageTag, c1.definitionId, c2.dockerImageTag, c2.definitionId);
              final int comparison = v1.versionCompareTo(v2);
              if (comparison >= 0) {
                return c1;
              } else {
                return c2;
              }
            }));
  }

  /**
   * The custom connector are not present in the seed and thus it is not relevant to validate their
   * latest version. This method allows to filter them out.
   *
   * @param connectorRepositoryToIdVersionMap connector docker image to connector info
   * @param configType airbyte config type
   * @return map of docker image to connector info
   */
  @VisibleForTesting
  Map<String, ConnectorInfo> filterCustomConnector(final Map<String, ConnectorInfo> connectorRepositoryToIdVersionMap,
                                                   final AirbyteConfig configType) {
    return connectorRepositoryToIdVersionMap.entrySet().stream()
        // The validation is based on the of the connector name is based on the seed which doesn't contain
        // any custom connectors. They can thus be
        // filtered out.
        .filter(entry -> {
          if (configType == ConfigSchema.STANDARD_SOURCE_DEFINITION) {
            return !Jsons.object(entry.getValue().definition, StandardSourceDefinition.class).getCustom();
          } else if (configType == ConfigSchema.STANDARD_DESTINATION_DEFINITION) {
            return !Jsons.object(entry.getValue().definition, StandardDestinationDefinition.class).getCustom();
          } else {
            return true;
          }
        })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Update connector definitions with new batch.
   *
   * @param ctx db context
   * @param configType airbyte config type
   * @param latestDefinitions latest definitions
   * @param connectorRepositoriesInUse when a connector is used in any standard sync, its definition
   *        will not be updated. This is necessary because the new connector version may not be
   *        backward compatible.
   * @param connectorRepositoryToIdVersionMap map of connector docker image to connector info
   * @param <T> type of definition ConnectorRegistrySourceDefinition or
   *        ConnectorRegistryDestinationDefiniton
   * @return connector counts
   * @throws IOException exception when interacting with the db
   */
  @VisibleForTesting
  <T> ConnectorCounter updateConnectorDefinitions(final AirbyteConfig configType,
                                                  final List<T> latestDefinitions,
                                                  final Set<String> connectorRepositoriesInUse,
                                                  final Map<String, ConnectorInfo> connectorRepositoryToIdVersionMap,
                                                  final boolean updateAll)
      throws IOException {
    int newCount = 0;
    int updatedCount = 0;

    for (final T latestRegistryDefinition : latestDefinitions) {
      final JsonNode latestRegistryDefinitionJson = Jsons.jsonNode(latestRegistryDefinition);
      final String repository = latestRegistryDefinitionJson.get("dockerRepository").asText();

      final Map<String, ConnectorInfo> connectorRepositoryToIdVersionMapWithoutCustom = filterCustomConnector(connectorRepositoryToIdVersionMap,
          configType);

      // Add new connector
      if (!connectorRepositoryToIdVersionMapWithoutCustom.containsKey(repository)) {
        LOGGER.info("Adding new connector {}: {}", repository, latestRegistryDefinitionJson);
        writeOrUpdateConnectorRegistryDefinition(configType, latestRegistryDefinition);
        newCount++;
        continue;
      }

      // Handle existing connectors
      final ConnectorInfo connectorInfo = connectorRepositoryToIdVersionMapWithoutCustom.get(repository);
      final String latestImageTag = latestRegistryDefinitionJson.get("dockerImageTag").asText();
      final boolean connectorIsInUse = connectorRepositoriesInUse.contains(repository);

      if (updateIsAvailable(connectorInfo.dockerImageTag, latestImageTag)) {
        if (updateIsPatchOnly(connectorInfo.dockerImageTag, latestImageTag)) {
          // Always update the connector to a new patch version if available
          LOGGER.info("Connector {} needs update: {} -> {}", repository, connectorInfo.dockerImageTag, latestImageTag);
          writeOrUpdateConnectorRegistryDefinition(configType, latestRegistryDefinition);
          updatedCount++;
        } else if (updateAll || !connectorIsInUse) {
          // If not update all, only update the connector to new major/minor versions if it's not in use
          LOGGER.info("Connector {} needs update: {} -> {}", repository, connectorInfo.dockerImageTag, latestImageTag);
          writeOrUpdateConnectorRegistryDefinition(configType, latestRegistryDefinition);
          updatedCount++;
        }
      } else {
        // If no new version, still upsert in case something changed in the definition
        // without the version being updated. We won't count that toward updatedCount though.
        writeOrUpdateConnectorRegistryDefinition(configType, latestRegistryDefinition);
      }
    }

    return new ConnectorCounter(newCount, updatedCount);
  }

  private <T> void writeOrUpdateConnectorRegistryDefinition(final AirbyteConfig configType,
                                                            final T definition)
      throws IOException {
    if (configType == ConfigSchema.STANDARD_SOURCE_DEFINITION) {
      final ConnectorRegistrySourceDefinition registryDef = (ConnectorRegistrySourceDefinition) definition;
      registryDef.withProtocolVersion(getProtocolVersion(registryDef.getSpec()));

      final StandardSourceDefinition stdSourceDef = ConnectorRegistryConverters.toStandardSourceDefinition(registryDef);

      final ActorDefinitionVersion actorDefinitionVersion = ConnectorRegistryConverters.toActorDefinitionVersion(registryDef);
      configRepository.writeSourceDefinitionAndDefaultVersion(stdSourceDef, actorDefinitionVersion);
    } else if (configType == ConfigSchema.STANDARD_DESTINATION_DEFINITION) {
      final ConnectorRegistryDestinationDefinition registryDef = (ConnectorRegistryDestinationDefinition) definition;
      registryDef.withProtocolVersion(getProtocolVersion(registryDef.getSpec()));

      final StandardDestinationDefinition stdDestDef = ConnectorRegistryConverters.toStandardDestinationDefinition(registryDef);

      final ActorDefinitionVersion actorDefinitionVersion = ConnectorRegistryConverters.toActorDefinitionVersion(registryDef);
      configRepository.writeDestinationDefinitionAndDefaultVersion(stdDestDef, actorDefinitionVersion);
    } else {
      throw new IllegalArgumentException(UNKNOWN_CONFIG_TYPE + configType);
    }
  }

  private static String getProtocolVersion(final ConnectorSpecification spec) {
    return AirbyteProtocolVersion.getWithDefault(spec != null ? spec.getProtocolVersion() : null).serialize();
  }

  @VisibleForTesting
  static boolean updateIsAvailable(final String currentVersion, final String latestVersion) {
    try {
      return new AirbyteVersion(latestVersion).versionCompareTo(new AirbyteVersion(currentVersion)) > 0;
    } catch (final Exception e) {
      LOGGER.error("Failed to check version: {} vs {}", currentVersion, latestVersion);
      return false;
    }
  }

  @VisibleForTesting
  static boolean updateIsPatchOnly(final String currentVersion, final String latestVersion) {
    try {
      return new AirbyteVersion(latestVersion).checkOnlyPatchVersionIsUpdatedComparedTo(new AirbyteVersion(currentVersion));
    } catch (final Exception e) {
      LOGGER.error("Failed to check version: {} vs {}", currentVersion, latestVersion);
      return false;
    }
  }

  static class ConnectorInfo {

    final String definitionId;
    final JsonNode definition;
    final String dockerRepository;
    final String dockerImageTag;

    ConnectorInfo(final String definitionId, final JsonNode definition) {
      this.definitionId = definitionId;
      this.definition = definition;
      dockerRepository = definition.get("dockerRepository").asText();
      dockerImageTag = definition.get("dockerImageTag").asText();
    }

    @Override
    public String toString() {
      return String.format("%s: %s (%s)", dockerRepository, dockerImageTag, definitionId);
    }

  }

  private static class ConnectorCounter {

    private final int newCount;
    private final int updateCount;

    private ConnectorCounter(final int newCount, final int updateCount) {
      this.newCount = newCount;
      this.updateCount = updateCount;
    }

  }

}
