/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static org.jooq.impl.DSL.asterisk;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.AirbyteConfig;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes in most up-to-date source and destination definitions from the Airbyte connector catalog
 * and merges them with those already present in the database. See javadocs on methods for rules.
 */
public class ActorDefinitionMigrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActorDefinitionMigrator.class);

  private static final String UNKNOWN_CONFIG_TYPE = "Unknown Config Type ";

  private final ExceptionWrappingDatabase database;

  public ActorDefinitionMigrator(final ExceptionWrappingDatabase database) {
    this.database = database;
  }

  /**
   * Migrate to update to newest definitions.
   *
   * @param latestSources latest sources
   * @param latestDestinations latest destinations
   * @throws IOException exception while interacting with the db.
   */
  public void migrate(final List<StandardSourceDefinition> latestSources, final List<StandardDestinationDefinition> latestDestinations)
      throws IOException {
    database.transaction(ctx -> {
      try {
        updateConfigsFromSeed(ctx, latestSources, latestDestinations);
      } catch (final IOException e) {
        throw new SQLException(e);
      }
      return null;
    });
  }

  @VisibleForTesting
  void updateConfigsFromSeed(final DSLContext ctx,
                             final List<StandardSourceDefinition> latestSources,
                             final List<StandardDestinationDefinition> latestDestinations)
      throws IOException {
    LOGGER.info("Updating connector definitions from the seed if necessary...");

    final Set<String> connectorRepositoriesInUse = ConfigWriter.getConnectorRepositoriesInUse(ctx);
    LOGGER.info("Connectors in use: {}", connectorRepositoriesInUse);

    final Map<String, ConnectorInfo> connectorRepositoryToInfoMap = getConnectorRepositoryToInfoMap(ctx);
    LOGGER.info("Current connector versions: {}", connectorRepositoryToInfoMap.values());

    int newConnectorCount = 0;
    int updatedConnectorCount = 0;

    final ConnectorCounter sourceConnectorCounter = updateConnectorDefinitions(ctx, ConfigSchema.STANDARD_SOURCE_DEFINITION,
        latestSources, connectorRepositoriesInUse, connectorRepositoryToInfoMap);
    newConnectorCount += sourceConnectorCounter.newCount;
    updatedConnectorCount += sourceConnectorCounter.updateCount;

    final ConnectorCounter destinationConnectorCounter = updateConnectorDefinitions(ctx, ConfigSchema.STANDARD_DESTINATION_DEFINITION,
        latestDestinations, connectorRepositoriesInUse, connectorRepositoryToInfoMap);
    newConnectorCount += destinationConnectorCounter.newCount;
    updatedConnectorCount += destinationConnectorCounter.updateCount;

    LOGGER.info("Connector definitions have been updated ({} new connectors, and {} version updates)", newConnectorCount, updatedConnectorCount);
  }

  /**
   * Get connector docker image to connector definition info.
   *
   * @param ctx db context
   * @return A map about current connectors (both source and destination). It maps from connector
   *         repository to its definition id and docker image tag. We identify a connector by its
   *         repository name instead of definition id because connectors can be added manually by
   *         users, and are not always the same as those in the seed.
   */
  @VisibleForTesting
  Map<String, ConnectorInfo> getConnectorRepositoryToInfoMap(final DSLContext ctx) {
    return ctx.select(asterisk())
        .from(ACTOR_DEFINITION)
        .where(ACTOR_DEFINITION.RELEASE_STAGE.isNull()
            .or(ACTOR_DEFINITION.RELEASE_STAGE.ne(ReleaseStage.custom).or(ACTOR_DEFINITION.CUSTOM)))
        .fetch()
        .stream()
        .collect(Collectors.toMap(
            row -> row.getValue(ACTOR_DEFINITION.DOCKER_REPOSITORY),
            row -> {
              final JsonNode jsonNode;
              if (row.get(ACTOR_DEFINITION.ACTOR_TYPE) == ActorType.source) {
                jsonNode = Jsons.jsonNode(DbConverter.buildStandardSourceDefinition(row, 10800L));
              } else if (row.get(ACTOR_DEFINITION.ACTOR_TYPE) == ActorType.destination) {
                jsonNode = Jsons.jsonNode(DbConverter.buildStandardDestinationDefinition(row));
              } else {
                throw new RuntimeException("Unknown Actor Type " + row.get(ACTOR_DEFINITION.ACTOR_TYPE));
              }
              return new ConnectorInfo(row.getValue(ACTOR_DEFINITION.ID).toString(), jsonNode);
            },
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
   * @param <T> type of definition Source Definition or Destination Definition
   * @return connector counts
   * @throws IOException exception when interacting with the db
   */
  @VisibleForTesting
  <T> ConnectorCounter updateConnectorDefinitions(final DSLContext ctx,
                                                  final AirbyteConfig configType,
                                                  final List<T> latestDefinitions,
                                                  final Set<String> connectorRepositoriesInUse,
                                                  final Map<String, ConnectorInfo> connectorRepositoryToIdVersionMap) {
    int newCount = 0;
    int updatedCount = 0;

    for (final T latestDefinition : latestDefinitions) {
      final JsonNode latestDefinitionJson = Jsons.jsonNode(latestDefinition);
      final String repository = latestDefinitionJson.get("dockerRepository").asText();
      final boolean connectorIsInUse = connectorRepositoriesInUse.contains(repository);

      final Map<String, ConnectorInfo> connectorRepositoryToIdVersionMapWithoutCustom = filterCustomConnector(connectorRepositoryToIdVersionMap,
          configType);

      // Add new connector
      if (!connectorRepositoryToIdVersionMapWithoutCustom.containsKey(repository)) {
        LOGGER.info("Adding new connector {}: {}", repository, latestDefinitionJson);
        writeOrUpdateStandardDefinition(ctx, configType, latestDefinition);
        newCount++;
        continue;
      }

      // Handle existing connectors
      final ConnectorInfo connectorInfo = connectorRepositoryToIdVersionMapWithoutCustom.get(repository);
      final String latestImageTag = latestDefinitionJson.get("dockerImageTag").asText();

      if (updateIsAvailable(connectorInfo.dockerImageTag, latestImageTag)) {
        if (updateIsPatchOnly(connectorInfo.dockerImageTag, latestImageTag)) {
          // Always update the connector to a new patch version if available
          LOGGER.info("Connector {} needs update: {} vs {}", repository, connectorInfo.dockerImageTag, latestImageTag);
          writeOrUpdateStandardDefinition(ctx, configType, latestDefinition);
          updatedCount++;
        } else if (!connectorIsInUse) {
          // Only update the connector to new major/minor versions if it's not in use
          LOGGER.info("Connector {} needs update: {} vs {}", repository, connectorInfo.dockerImageTag, latestImageTag);
          writeOrUpdateStandardDefinition(ctx, configType, latestDefinition);
          updatedCount++;
        }
      } else {
        // If no new version, still upsert in case something changed in the definition
        // without the version being updated. We won't count that toward updatedCount though.
        writeOrUpdateStandardDefinition(ctx, configType, latestDefinition);
      }
    }

    return new ConnectorCounter(newCount, updatedCount);
  }

  private <T> void writeOrUpdateStandardDefinition(final DSLContext ctx,
                                                   final AirbyteConfig configType,
                                                   final T definition) {
    if (configType == ConfigSchema.STANDARD_SOURCE_DEFINITION) {
      final StandardSourceDefinition sourceDef = (StandardSourceDefinition) definition;
      sourceDef.withProtocolVersion(getProtocolVersion(sourceDef.getSpec()));
      ConfigWriter.writeStandardSourceDefinition(Collections.singletonList(sourceDef), ctx);
    } else if (configType == ConfigSchema.STANDARD_DESTINATION_DEFINITION) {
      final StandardDestinationDefinition destDef = (StandardDestinationDefinition) definition;
      destDef.withProtocolVersion(getProtocolVersion(destDef.getSpec()));
      ConfigWriter.writeStandardDestinationDefinition(Collections.singletonList(destDef), ctx);
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
