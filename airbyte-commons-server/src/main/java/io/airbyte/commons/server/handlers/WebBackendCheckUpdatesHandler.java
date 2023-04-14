/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.WebBackendCheckUpdatesRead;
import io.airbyte.commons.server.services.AirbyteRemoteOssCatalog;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.persistence.ConfigRepository;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * The web backend is an abstraction that allows the frontend to structure data in such a way that
 * it is easier for a react frontend to consume. It should NOT have direct access to the database.
 * It should operate exclusively by calling other endpoints that are exposed in the API.
 *
 * Javadocs suppressed because api docs should be used as source of truth.
 **/
@SuppressWarnings("MissingJavadocMethod")
@Slf4j
@Singleton
public class WebBackendCheckUpdatesHandler {

  private static final int NO_CHANGES_FOUND = 0;

  // todo (cgardens) - this handler should NOT have access to the db. only access via handler.
  @Deprecated
  final ConfigRepository configRepositoryDoNotUse;
  final AirbyteRemoteOssCatalog remoteOssCatalog;

  public WebBackendCheckUpdatesHandler(final ConfigRepository configRepositoryDoNotUse, final AirbyteRemoteOssCatalog remoteOssCatalog) {
    this.configRepositoryDoNotUse = configRepositoryDoNotUse;
    this.remoteOssCatalog = remoteOssCatalog;
  }

  public WebBackendCheckUpdatesRead checkUpdates() {

    final int destinationDiffCount = getDestinationDiffCount();
    final int sourceDiffCount = getSourceDiffCount();

    return new WebBackendCheckUpdatesRead()
        .destinationDefinitions(destinationDiffCount)
        .sourceDefinitions(sourceDiffCount);
  }

  private int getDestinationDiffCount() {
    final List<Entry<UUID, String>> currentActorDefToDockerImageTag;
    final Map<UUID, String> newActorDefToDockerImageTag;

    try {
      currentActorDefToDockerImageTag = configRepositoryDoNotUse.listStandardDestinationDefinitions(false)
          .stream()
          .map(def -> Map.entry(def.getDestinationDefinitionId(), def.getDockerImageTag()))
          .toList();
    } catch (final IOException e) {
      log.error("Failed to get current list of standard destination definitions", e);
      return NO_CHANGES_FOUND;
    }

    newActorDefToDockerImageTag = remoteOssCatalog.getDestinationDefinitions()
        .stream()
        .collect(Collectors.toMap(ConnectorRegistryDestinationDefinition::getDestinationDefinitionId,
            ConnectorRegistryDestinationDefinition::getDockerImageTag));

    return getDiffCount(currentActorDefToDockerImageTag, newActorDefToDockerImageTag);
  }

  private int getSourceDiffCount() {
    final List<Entry<UUID, String>> currentActorDefToDockerImageTag;
    final Map<UUID, String> newActorDefToDockerImageTag;

    try {
      currentActorDefToDockerImageTag = configRepositoryDoNotUse.listStandardSourceDefinitions(false)
          .stream()
          .map(def -> Map.entry(def.getSourceDefinitionId(), def.getDockerImageTag()))
          .toList();
    } catch (final IOException e) {
      log.error("Failed to get current list of standard source definitions", e);
      return NO_CHANGES_FOUND;
    }

    newActorDefToDockerImageTag = remoteOssCatalog.getSourceDefinitions()
        .stream()
        .collect(Collectors.toMap(ConnectorRegistrySourceDefinition::getSourceDefinitionId, ConnectorRegistrySourceDefinition::getDockerImageTag));

    return getDiffCount(currentActorDefToDockerImageTag, newActorDefToDockerImageTag);
  }

  private int getDiffCount(final List<Entry<UUID, String>> initialSet, final Map<UUID, String> newSet) {
    int diffCount = 0;
    for (final Entry<UUID, String> kvp : initialSet) {
      final String newDockerImageTag = newSet.get(kvp.getKey());
      if (newDockerImageTag != null && !kvp.getValue().equals(newDockerImageTag)) {
        ++diffCount;
      }
    }
    return diffCount;
  }

}
