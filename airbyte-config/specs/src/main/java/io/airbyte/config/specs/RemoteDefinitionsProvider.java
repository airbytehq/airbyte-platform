/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs;

import static io.micronaut.http.HttpHeaders.ACCEPT;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.constants.AirbyteCatalogConstants;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.config.ActorType;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.ConnectorRegistry;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.micronaut.cache.annotation.CacheConfig;
import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.MediaType;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This provider pulls the definitions from a remotely hosted connector registry.
 */
@Singleton
@CacheConfig("remote-definitions-provider")
@SuppressWarnings("PMD.ExceptionAsFlowControl")
public class RemoteDefinitionsProvider implements DefinitionsProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteDefinitionsProvider.class);

  private final OkHttpClient okHttpClient;

  private final URI remoteRegistryBaseUrl;
  private final DeploymentMode deploymentMode;
  private static final int NOT_FOUND = 404;

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
    this.deploymentMode = deploymentMode;
    this.okHttpClient = new OkHttpClient.Builder()
        .callTimeout(Duration.ofMillis(remoteCatalogTimeoutMs))
        .build();
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
  URL getRemoteRegistryUrlForPath(final String path) {
    try {
      return remoteRegistryBaseUrl.resolve(path).toURL();
    } catch (final MalformedURLException e) {
      throw new RuntimeException("Invalid URL format", e);
    }
  }

  @VisibleForTesting
  String getRegistryPath() {
    return String.format("registries/v0/%s_registry.json", getRegistryName());
  }

  @VisibleForTesting
  String getRegistryEntryPath(final String connectorRepository, final String version) {
    return String.format("metadata/%s/%s/%s.json", connectorRepository, version, getRegistryName());
  }

  @VisibleForTesting
  static String getDocPath(final String connectorRepository, final String version) {
    return String.format("metadata/%s/%s/doc.md", connectorRepository, version);
  }

  @VisibleForTesting
  static String getManifestPath(final String connectorRepository, final String version) {
    return String.format("metadata/%s/%s/manifest.yaml", connectorRepository, version);
  }

  /**
   * Get remote connector registry.
   *
   * @return ConnectorRegistry
   */
  @Cacheable
  public ConnectorRegistry getRemoteConnectorRegistry() {
    final URL registryUrl = getRemoteRegistryUrlForPath(getRegistryPath());
    final Request request = new Request.Builder()
        .url(registryUrl)
        .header(ACCEPT, MediaType.APPLICATION_JSON)
        .build();

    try (final Response response = okHttpClient.newCall(request).execute()) {
      if (response.isSuccessful() && response.body() != null) {
        final String responseBody = response.body().string();
        LOGGER.info("Fetched latest remote definitions ({})", responseBody.hashCode());
        return Jsons.deserialize(responseBody, ConnectorRegistry.class);
      } else {
        throw new IOException(formatStatusCodeException("getRemoteConnectorRegistry", response));
      }
    } catch (final Exception e) {
      throw new RuntimeException("Failed to fetch remote connector registry", e);
    }
  }

  @VisibleForTesting
  Optional<JsonNode> getConnectorRegistryEntryJson(final String connectorName, final String version) {
    final URL registryEntryPath = getRemoteRegistryUrlForPath(getRegistryEntryPath(connectorName, version));
    final Request request = new Request.Builder()
        .url(registryEntryPath)
        .header(ACCEPT, MediaType.APPLICATION_JSON)
        .build();

    try (final Response response = okHttpClient.newCall(request).execute()) {
      if (response.code() == NOT_FOUND) {
        return Optional.empty();
      } else if (response.isSuccessful() && response.body() != null) {
        return Optional.of(Jsons.deserialize(response.body().string()));
      } else {
        throw new IOException(formatStatusCodeException("getConnectorRegistryEntryJson", response));
      }
    } catch (final IOException e) {
      throw new RuntimeException(String.format("Failed to fetch connector registry entry for %s:%s", connectorName, version), e);
    }
  }

  /**
   * Retrieves the full or inapp documentation for the specified connector repo and version.
   *
   * @return Optional containing the connector doc if it can be found, or empty otherwise.
   */
  public Optional<String> getConnectorDocumentation(final String connectorRepository, final String version) {
    final URL docUrl = getRemoteRegistryUrlForPath(getDocPath(connectorRepository, version));
    final Request request = new Request.Builder()
        .url(docUrl)
        .header(ACCEPT, MediaType.APPLICATION_JSON)
        .build();

    try (final Response response = okHttpClient.newCall(request).execute()) {
      if (response.code() == NOT_FOUND) {
        return Optional.empty();
      } else if (response.isSuccessful() && response.body() != null) {
        return Optional.of(response.body().string());
      } else {
        throw new IOException(formatStatusCodeException("getConnectorDocumentation", response));
      }
    } catch (final IOException e) {
      throw new RuntimeException(
          String.format("Failed to fetch connector documentation for %s:%s", connectorRepository, version), e);
    }
  }

  /**
   * Retrieves the manifest for the specified connector repo and version.
   *
   * @return Optional containing the connector manifest if it can be found, or empty otherwise.
   */
  public Optional<JsonNode> getConnectorManifest(final String connectorRepository, final String version) {
    final URL manifestUrl = getRemoteRegistryUrlForPath(getManifestPath(connectorRepository, version));
    final Request request = new Request.Builder()
        .url(manifestUrl)
        .header(ACCEPT, MediaType.APPLICATION_JSON)
        .build();

    try (final Response response = okHttpClient.newCall(request).execute()) {
      if (response.code() == NOT_FOUND) {
        return Optional.empty();
      } else if (response.isSuccessful() && response.body() != null) {
        final String manifestYamlContent = response.body().string();
        return Optional.of(Yamls.deserialize(manifestYamlContent));
      } else {
        throw new IOException(formatStatusCodeException("getConnectorManifest", response));
      }
    } catch (final IOException e) {
      throw new RuntimeException(
          String.format("Failed to fetch manifest file for %s:%s", connectorRepository, version), e);
    }
  }

  private String formatStatusCodeException(final String operationName, final Response response) {
    return String.format("%s request ran into status code error: %d with message: %s", operationName, response.code(), response.message());
  }

}
