/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.config.ConnectorRegistry;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.micronaut.cache.annotation.CacheConfig;
import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * This provider pulls the definitions from a remotely hosted connector registry.
 */
@Singleton
@Primary
@Requires(property = "airbyte.platform.remote-connector-catalog.url",
          notEquals = "")
@CacheConfig("remote-definitions-provider")
@Slf4j
public class RemoteDefinitionsProvider implements DefinitionsProvider {

  private static final HttpClient httpClient = HttpClient.newHttpClient();
  private final URI remoteDefinitionCatalogUrl;
  private final Duration timeout;

  public RemoteDefinitionsProvider(@Value("${airbyte.platform.remote-connector-catalog.url}") final String remoteCatalogUrl,
                                   @Value("${airbyte.platform.remote-connector-catalog.timeout-ms}") final long remoteCatalogTimeoutMs) {
    log.info("Creating remote definitions provider for URL '{}'...", remoteCatalogUrl);
    if (remoteCatalogUrl == null || remoteCatalogUrl.isEmpty()) {
      throw new IllegalArgumentException("Remote catalog URL cannot be null or empty.");
    }

    try {
      this.remoteDefinitionCatalogUrl = new URI(remoteCatalogUrl);
      this.timeout = Duration.ofMillis(remoteCatalogTimeoutMs);
    } catch (final URISyntaxException e) {
      log.error("Invalid remote catalog URL: {}", remoteCatalogUrl);
      throw new IllegalArgumentException("Remote catalog URL Must be a valid URI.", e);
    }
  }

  private Map<UUID, ConnectorRegistrySourceDefinition> getSourceDefinitionsMap() {
    final ConnectorRegistry registry = getRemoteConnectorRegistry();
    return registry.getSources().stream().collect(Collectors.toMap(
        ConnectorRegistrySourceDefinition::getSourceDefinitionId,
        source -> source.withProtocolVersion(
            AirbyteProtocolVersion.getWithDefault(source.getSpec() != null ? source.getSpec().getProtocolVersion() : null).serialize())));
  }

  private Map<UUID, ConnectorRegistryDestinationDefinition> getDestinationDefinitionsMap() {
    final ConnectorRegistry catalog = getRemoteConnectorRegistry();
    return catalog.getDestinations().stream().collect(Collectors.toMap(
        ConnectorRegistryDestinationDefinition::getDestinationDefinitionId,
        destination -> destination.withProtocolVersion(
            AirbyteProtocolVersion.getWithDefault(destination.getSpec() != null ? destination.getSpec().getProtocolVersion() : null).serialize())));
  }

  @Override
  public ConnectorRegistrySourceDefinition getSourceDefinition(final UUID definitionId) throws ConfigNotFoundException {
    final ConnectorRegistrySourceDefinition definition = getSourceDefinitionsMap().get(definitionId);
    if (definition == null) {
      throw new ConfigNotFoundException("remote_registry:source_def", definitionId.toString());
    }
    return definition;
  }

  @Override
  public List<ConnectorRegistrySourceDefinition> getSourceDefinitions() {
    return new ArrayList<>(getSourceDefinitionsMap().values());
  }

  @Override
  public ConnectorRegistryDestinationDefinition getDestinationDefinition(final UUID definitionId) throws ConfigNotFoundException {
    final ConnectorRegistryDestinationDefinition definition = getDestinationDefinitionsMap().get(definitionId);
    if (definition == null) {
      throw new ConfigNotFoundException("remote_registry:destination_def", definitionId.toString());
    }
    return definition;
  }

  @Override
  public List<ConnectorRegistryDestinationDefinition> getDestinationDefinitions() {
    return new ArrayList<>(getDestinationDefinitionsMap().values());
  }

  /**
   * Get remote definition catalog.
   *
   * @return combined catalog
   */
  @Cacheable
  public ConnectorRegistry getRemoteConnectorRegistry() {
    try {
      final HttpRequest request = HttpRequest.newBuilder(remoteDefinitionCatalogUrl).timeout(timeout).header("accept", "application/json").build();

      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (errorStatusCode(response)) {
        throw new IOException(
            "getRemoteConnectorRegistry request ran into status code error: " + response.statusCode() + " with message: " + response.getClass());
      }

      log.info("Fetched latest remote definitions ({})", response.body().hashCode());
      return Jsons.deserialize(response.body(), ConnectorRegistry.class);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to fetch remote connector registry", e);
    }
  }

  private static Boolean errorStatusCode(final HttpResponse<String> response) {
    return response.statusCode() >= 400;
  }

}
