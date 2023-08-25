/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.client.http.HttpStatusCodes;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.constants.AirbyteCatalogConstants;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.ConnectorRegistry;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.micronaut.cache.annotation.CacheConfig;
import io.micronaut.cache.annotation.Cacheable;
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
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This provider pulls the definitions from a remotely hosted connector registry.
 */
@Singleton
@CacheConfig("remote-definitions-provider")
public class RemoteDefinitionsProvider implements DefinitionsProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteDefinitionsProvider.class);

  private static final HttpClient httpClient = HttpClient.newHttpClient();
  private final URI remoteRegistryBaseUrl;
  private final DeploymentMode deploymentMode;
  private final Duration timeout;

  private URI parsedRemoteRegistryBaseUrlOrDefault(final String remoteRegistryBaseUrl) {
    try {
      if (remoteRegistryBaseUrl == null || remoteRegistryBaseUrl.isEmpty()) {
        return new URI(AirbyteCatalogConstants.REMOTE_REGISTRY_BASE_URL);
      } else {
        return new URI(remoteRegistryBaseUrl);
      }
    } catch (final URISyntaxException e) {
      LOGGER.error("Invalid remote registry base URL: {}", remoteRegistryBaseUrl);
      throw new IllegalArgumentException("Remote connector registry base URL must be a valid URI.", e);
    }
  }

  public RemoteDefinitionsProvider(@Value("${airbyte.connector-registry.remote.base-url}") final String remoteRegistryBaseUrl,
                                   final DeploymentMode deploymentMode,
                                   @Value("${airbyte.connector-registry.remote.timeout-ms}") final long remoteCatalogTimeoutMs) {
    final URI remoteRegistryBaseUrlUri = parsedRemoteRegistryBaseUrlOrDefault(remoteRegistryBaseUrl);
    LOGGER.info("Creating remote definitions provider for URL '{}' and registry '{}'...", remoteRegistryBaseUrlUri, deploymentMode);

    this.remoteRegistryBaseUrl = remoteRegistryBaseUrlUri;
    this.timeout = Duration.ofMillis(remoteCatalogTimeoutMs);
    this.deploymentMode = deploymentMode;
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
  public ConnectorRegistrySourceDefinition getSourceDefinition(final UUID definitionId) throws RegistryDefinitionNotFoundException {
    final ConnectorRegistrySourceDefinition definition = getSourceDefinitionsMap().get(definitionId);
    if (definition == null) {
      throw new RegistryDefinitionNotFoundException(ActorType.SOURCE, definitionId);
    }
    return definition;
  }

  /**
   * Gets the registry entry for a given source connector version. If no entry exists for the given
   * connector or version, an empty optional will be returned.
   */
  public Optional<ConnectorRegistrySourceDefinition> getSourceDefinitionByVersion(final String connectorRepository, final String version) {
    final Optional<JsonNode> registryEntryJson = getConnectorRegistryEntryJson(connectorRepository, version);
    return registryEntryJson.map(jsonNode -> Jsons.object(jsonNode, ConnectorRegistrySourceDefinition.class));
  }

  @Override
  public List<ConnectorRegistrySourceDefinition> getSourceDefinitions() {
    return new ArrayList<>(getSourceDefinitionsMap().values());
  }

  @Override
  public ConnectorRegistryDestinationDefinition getDestinationDefinition(final UUID definitionId) throws RegistryDefinitionNotFoundException {
    final ConnectorRegistryDestinationDefinition definition = getDestinationDefinitionsMap().get(definitionId);
    if (definition == null) {
      throw new RegistryDefinitionNotFoundException(ActorType.DESTINATION, definitionId);
    }
    return definition;
  }

  /**
   * Gets the registry entry for a given destination connector version. If no entry exists for the
   * given connector or version, an empty optional will be returned.
   */
  public Optional<ConnectorRegistryDestinationDefinition> getDestinationDefinitionByVersion(final String connectorRepository, final String version) {
    final Optional<JsonNode> registryEntryJson = getConnectorRegistryEntryJson(connectorRepository, version);
    return registryEntryJson.map(jsonNode -> Jsons.object(jsonNode, ConnectorRegistryDestinationDefinition.class));
  }

  @Override
  public List<ConnectorRegistryDestinationDefinition> getDestinationDefinitions() {
    return new ArrayList<>(getDestinationDefinitionsMap().values());
  }

  private String getRegistryName() {
    return switch (deploymentMode) {
      case OSS -> "oss";
      case CLOUD -> "cloud";
    };
  }

  @VisibleForTesting
  URI getRegistryUrl() {
    return remoteRegistryBaseUrl.resolve(String.format("registries/v0/%s_registry.json", getRegistryName()));
  }

  @VisibleForTesting
  URI getRegistryEntryUrl(final String connectorName, final String version) {
    return remoteRegistryBaseUrl.resolve(String.format("metadata/%s/%s/%s.json", connectorName, version, getRegistryName()));
  }

  /**
   * Get remote connector registry.
   *
   * @return ConnectorRegistry
   */
  @Cacheable
  public ConnectorRegistry getRemoteConnectorRegistry() {
    try {
      final HttpRequest request = HttpRequest.newBuilder(getRegistryUrl()).timeout(timeout).header("accept", "application/json").build();

      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (errorStatusCode(response)) {
        throw new IOException(
            "getRemoteConnectorRegistry request ran into status code error: " + response.statusCode() + " with message: " + response.getClass());
      }

      LOGGER.info("Fetched latest remote definitions ({})", response.body().hashCode());
      return Jsons.deserialize(response.body(), ConnectorRegistry.class);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to fetch remote connector registry", e);
    }
  }

  @VisibleForTesting
  Optional<JsonNode> getConnectorRegistryEntryJson(final String connectorName, final String version) {
    try {
      final URI registryEntryPath = getRegistryEntryUrl(connectorName, version);
      final HttpRequest request = HttpRequest.newBuilder(registryEntryPath).timeout(timeout).header("accept", "application/json").build();

      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        // 404s mean we don't have a registry entry for this connector version
        return Optional.empty();
      } else if (errorStatusCode(response)) {
        throw new IOException(
            "getConnectorRegistryEntry request ran into status code error: " + response.statusCode() + " with message: " + response.getClass());
      }

      return Optional.of(Jsons.deserialize(response.body()));
    } catch (final IOException | InterruptedException e) {
      throw new RuntimeException(String.format("Failed to fetch connector registry entry for %s:%s", connectorName, version), e);
    }
  }

  private static Boolean errorStatusCode(final HttpResponse<String> response) {
    return response.statusCode() >= 400;
  }

}
