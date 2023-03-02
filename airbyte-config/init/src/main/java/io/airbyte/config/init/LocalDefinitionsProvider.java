/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import com.google.common.io.Resources;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.config.CatalogDefinitionsConfig;
import io.airbyte.config.CombinedConnectorCatalog;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * This provider contains all definitions according to the local catalog json files.
 */
public final class LocalDefinitionsProvider implements DefinitionsProvider {

  private static final String LOCAL_CONNECTOR_CATALOG_PATH = CatalogDefinitionsConfig.getLocalConnectorCatalogPath();

  /**
   * Get combined catalog.
   *
   * @return combined catalog
   */
  public CombinedConnectorCatalog getLocalDefinitionCatalog() {
    try {
      final URL url = Resources.getResource(LOCAL_CONNECTOR_CATALOG_PATH);
      final String jsonString = Resources.toString(url, StandardCharsets.UTF_8);
      final CombinedConnectorCatalog catalog = Jsons.deserialize(jsonString, CombinedConnectorCatalog.class);
      return catalog;

    } catch (final Exception e) {
      throw new RuntimeException("Failed to fetch local catalog definitions", e);
    }
  }

  /**
   * Get map of source definition ids to the definition.
   *
   * @return map
   */
  public Map<UUID, StandardSourceDefinition> getSourceDefinitionsMap() {
    final CombinedConnectorCatalog catalog = getLocalDefinitionCatalog();
    return catalog.getSources().stream().collect(Collectors.toMap(
        StandardSourceDefinition::getSourceDefinitionId,
        source -> source.withProtocolVersion(
            AirbyteProtocolVersion.getWithDefault(source.getSpec() != null ? source.getSpec().getProtocolVersion() : null).serialize())));
  }

  /**
   * Get map of destination definition ids to the definition.
   *
   * @return map
   */
  public Map<UUID, StandardDestinationDefinition> getDestinationDefinitionsMap() {
    final CombinedConnectorCatalog catalog = getLocalDefinitionCatalog();
    return catalog.getDestinations().stream().collect(
        Collectors.toMap(
            StandardDestinationDefinition::getDestinationDefinitionId,
            destination -> destination.withProtocolVersion(
                AirbyteProtocolVersion.getWithDefault(
                    destination.getSpec() != null
                        ? destination.getSpec().getProtocolVersion()
                        : null)
                    .serialize())));
  }

  @Override
  public StandardSourceDefinition getSourceDefinition(final UUID definitionId) throws ConfigNotFoundException {
    final StandardSourceDefinition definition = getSourceDefinitionsMap().get(definitionId);
    if (definition == null) {
      throw new ConfigNotFoundException("local_catalog:source_def", definitionId.toString());
    }
    return definition;
  }

  @Override
  public List<StandardSourceDefinition> getSourceDefinitions() {
    return new ArrayList<>(getSourceDefinitionsMap().values());
  }

  @Override
  public StandardDestinationDefinition getDestinationDefinition(final UUID definitionId) throws ConfigNotFoundException {
    final StandardDestinationDefinition definition = getDestinationDefinitionsMap().get(definitionId);
    if (definition == null) {
      throw new ConfigNotFoundException("local_catalog:destination_def", definitionId.toString());
    }
    return definition;
  }

  @Override
  public List<StandardDestinationDefinition> getDestinationDefinitions() {
    return new ArrayList<>(getDestinationDefinitionsMap().values());
  }

}
