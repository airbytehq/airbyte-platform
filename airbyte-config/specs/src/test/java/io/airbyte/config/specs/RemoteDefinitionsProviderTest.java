/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Resources;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.MoreIterators;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.SupportLevel;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RemoteDefinitionsProviderTest {

  private MockWebServer webServer;
  private MockResponse validCatalogResponse;
  private String baseUrl;
  private static final DeploymentMode DEPLOYMENT_MODE = DeploymentMode.OSS;
  private static final String CONNECTOR_REPOSITORY = "airbyte/source-stripe";
  private static final String CONNECTOR_VERSION = "0.2.1";
  private JsonNode jsonCatalog;

  @BeforeEach
  void setup() throws IOException {
    webServer = new MockWebServer();
    baseUrl = webServer.url("/").toString();

    final URL testCatalog = Resources.getResource("connector_catalog.json");
    final String jsonBody = Resources.toString(testCatalog, Charset.defaultCharset());
    jsonCatalog = Jsons.deserialize(jsonBody);
    validCatalogResponse = new MockResponse().setResponseCode(200)
        .addHeader("Content-Type", "application/json; charset=utf-8")
        .addHeader("Cache-Control", "no-cache")
        .setBody(jsonBody);
  }

  static MockResponse makeResponse(final int statusCode, final String body) {
    return new MockResponse().setResponseCode(statusCode)
        .addHeader("Content-Type", "application/json; charset=utf-8")
        .addHeader("Cache-Control", "no-cache")
        .setBody(body);
  }

  @Test
  @SuppressWarnings({"PMD.AvoidDuplicateLiterals"})
  void testGetSourceDefinition() throws Exception {
    webServer.enqueue(validCatalogResponse);
    final RemoteDefinitionsProvider remoteDefinitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(30));
    final UUID stripeSourceId = UUID.fromString("e094cb9a-26de-4645-8761-65c0c425d1de");
    final ConnectorRegistrySourceDefinition stripeSource = remoteDefinitionsProvider.getSourceDefinition(stripeSourceId);
    assertEquals(stripeSourceId, stripeSource.getSourceDefinitionId());
    assertEquals("Stripe", stripeSource.getName());
    assertEquals("airbyte/source-stripe", stripeSource.getDockerRepository());
    assertEquals("https://docs.airbyte.io/integrations/sources/stripe", stripeSource.getDocumentationUrl());
    assertEquals("stripe.svg", stripeSource.getIcon());
    assertEquals(URI.create("https://docs.airbyte.io/integrations/sources/stripe"), stripeSource.getSpec().getDocumentationUrl());
    assertEquals(false, stripeSource.getTombstone());
    assertEquals("0.2.1", stripeSource.getProtocolVersion());
    assertEquals(SupportLevel.COMMUNITY, stripeSource.getSupportLevel());
  }

  @Test
  @SuppressWarnings({"PMD.AvoidDuplicateLiterals"})
  void testGetDestinationDefinition() throws Exception {
    webServer.enqueue(validCatalogResponse);
    final RemoteDefinitionsProvider remoteDefinitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(30));
    final UUID s3DestinationId = UUID.fromString("4816b78f-1489-44c1-9060-4b19d5fa9362");
    final ConnectorRegistryDestinationDefinition s3Destination = remoteDefinitionsProvider
        .getDestinationDefinition(s3DestinationId);
    assertEquals(s3DestinationId, s3Destination.getDestinationDefinitionId());
    assertEquals("S3", s3Destination.getName());
    assertEquals("airbyte/destination-s3", s3Destination.getDockerRepository());
    assertEquals("https://docs.airbyte.io/integrations/destinations/s3", s3Destination.getDocumentationUrl());
    assertEquals(URI.create("https://docs.airbyte.io/integrations/destinations/s3"), s3Destination.getSpec().getDocumentationUrl());
    assertEquals(false, s3Destination.getTombstone());
    assertEquals("0.2.2", s3Destination.getProtocolVersion());
    assertEquals(SupportLevel.COMMUNITY, s3Destination.getSupportLevel());
  }

  @Test
  void testGetInvalidDefinitionId() {
    webServer.enqueue(validCatalogResponse);
    webServer.enqueue(validCatalogResponse);

    final RemoteDefinitionsProvider remoteDefinitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(30));
    final UUID invalidDefinitionId = UUID.fromString("1a7c360c-1289-4b96-a171-2ac1c86fb7ca");

    assertThrows(
        RegistryDefinitionNotFoundException.class,
        () -> remoteDefinitionsProvider.getSourceDefinition(invalidDefinitionId));
    assertThrows(
        RegistryDefinitionNotFoundException.class,
        () -> remoteDefinitionsProvider.getDestinationDefinition(invalidDefinitionId));
  }

  @Test
  void testGetSourceDefinitions() {
    webServer.enqueue(validCatalogResponse);
    final RemoteDefinitionsProvider remoteDefinitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(30));
    final List<ConnectorRegistrySourceDefinition> sourceDefinitions = remoteDefinitionsProvider.getSourceDefinitions();
    final int expectedNumberOfSources = MoreIterators.toList(jsonCatalog.get("sources").elements()).size();
    assertEquals(expectedNumberOfSources, sourceDefinitions.size());
    assertTrue(sourceDefinitions.stream().allMatch(sourceDef -> sourceDef.getProtocolVersion().length() > 0));
  }

  @Test
  void testGetDestinationDefinitions() {
    webServer.enqueue(validCatalogResponse);
    final RemoteDefinitionsProvider remoteDefinitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(30));
    final List<ConnectorRegistryDestinationDefinition> destinationDefinitions = remoteDefinitionsProvider.getDestinationDefinitions();
    final int expectedNumberOfDestinations = MoreIterators.toList(jsonCatalog.get("destinations").elements()).size();
    assertEquals(expectedNumberOfDestinations, destinationDefinitions.size());
    assertTrue(destinationDefinitions.stream().allMatch(destDef -> destDef.getProtocolVersion().length() > 0));
  }

  @Test
  void testBadResponseStatus() {
    webServer.enqueue(new MockResponse().setResponseCode(404));
    final RuntimeException ex = assertThrows(RuntimeException.class, () -> {
      new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(1)).getDestinationDefinitions();
    });

    assertTrue(ex.getMessage().contains("Failed to fetch remote connector registry"));
    assertTrue(ex.getCause() instanceof IOException);
  }

  @Test
  void testTimeOut() {
    // No request enqueued -> Timeout
    final RuntimeException ex = assertThrows(RuntimeException.class, () -> {
      new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(1)).getDestinationDefinitions();
    });

    assertTrue(ex.getMessage().contains("Failed to fetch remote connector registry"));
    assertEquals(ex.getCause().getClass(), InterruptedIOException.class);
  }

  @Test
  void testNonJson() {
    final MockResponse notJson = makeResponse(200, "not json");
    webServer.enqueue(notJson);
    assertThrows(RuntimeException.class, () -> {
      new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(1)).getDestinationDefinitions();
    });
  }

  @ParameterizedTest
  @CsvSource({"OSS", "CLOUD"})
  void testGetRegistryUrl(final String deploymentMode) {
    final String baseUrl = "https://connectors.airbyte.com/files/";
    final RemoteDefinitionsProvider definitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DeploymentMode.valueOf(deploymentMode), TimeUnit.SECONDS.toMillis(1));
    final URL registryUrl = definitionsProvider.getRegistryUrl();
    assertEquals(String.format("https://connectors.airbyte.com/files/registries/v0/%s_registry.json", deploymentMode.toLowerCase()),
        registryUrl.toString());
  }

  @ParameterizedTest
  @CsvSource({"OSS", "CLOUD"})
  void testGetRegistryEntryUrl(final String deploymentMode) {
    final String baseUrl = "https://connectors.airbyte.com/files/";
    final String connectorName = "airbyte/source-github";
    final String version = "1.0.0";
    final RemoteDefinitionsProvider definitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DeploymentMode.valueOf(deploymentMode), TimeUnit.SECONDS.toMillis(1));
    final URL registryEntryUrl = definitionsProvider.getRegistryEntryUrl(connectorName, version);
    assertEquals(String.format("https://connectors.airbyte.com/files/metadata/airbyte/source-github/1.0.0/%s.json", deploymentMode.toLowerCase()),
        registryEntryUrl.toString());
  }

  @Test
  void testGetMissingEntryByVersion() {
    webServer.enqueue(makeResponse(404, "not found"));
    final RemoteDefinitionsProvider remoteDefinitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(30));
    final Optional<JsonNode> definition = remoteDefinitionsProvider.getConnectorRegistryEntryJson(CONNECTOR_REPOSITORY, CONNECTOR_VERSION);
    assertTrue(definition.isEmpty());
  }

  @Test
  void testGetEntryByVersion() {
    final ConnectorRegistrySourceDefinition sourceDef = new ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("source")
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(new ConnectorSpecification().withConnectionSpecification(Jsons.emptyObject()));

    final MockResponse validResponse = makeResponse(200, Jsons.serialize(sourceDef));

    webServer.enqueue(validResponse);
    final RemoteDefinitionsProvider remoteDefinitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(30));
    final Optional<JsonNode> definition = remoteDefinitionsProvider.getConnectorRegistryEntryJson(CONNECTOR_REPOSITORY, CONNECTOR_VERSION);
    assertTrue(definition.isPresent());
    assertEquals(Jsons.jsonNode(sourceDef), definition.get());
  }

  @Test
  void testGetSourceDefinitionByVersion() {
    final ConnectorRegistrySourceDefinition sourceDef = new ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("source")
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(new ConnectorSpecification().withConnectionSpecification(Jsons.emptyObject()));

    final MockResponse validResponse = makeResponse(200, Jsons.serialize(sourceDef));
    webServer.enqueue(validResponse);

    final RemoteDefinitionsProvider remoteDefinitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(30));
    final Optional<ConnectorRegistrySourceDefinition> definition =
        remoteDefinitionsProvider.getSourceDefinitionByVersion(CONNECTOR_REPOSITORY, CONNECTOR_VERSION);
    assertTrue(definition.isPresent());
    assertEquals(sourceDef, definition.get());
  }

  @Test
  void testGetDestinationDefinitionByVersion() {
    final ConnectorRegistryDestinationDefinition destinationDef = new ConnectorRegistryDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("destination")
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(new ConnectorSpecification().withConnectionSpecification(Jsons.emptyObject()));

    final MockResponse validResponse = makeResponse(200, Jsons.serialize(destinationDef));
    webServer.enqueue(validResponse);

    final RemoteDefinitionsProvider remoteDefinitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(30));
    final Optional<ConnectorRegistryDestinationDefinition> definition =
        remoteDefinitionsProvider.getDestinationDefinitionByVersion(CONNECTOR_REPOSITORY, CONNECTOR_VERSION);
    assertTrue(definition.isPresent());
    assertEquals(destinationDef, definition.get());
  }

  @Test
  void testGetConnectorDocumentation() {
    final String connectorDocumentationBody = "The documentation contents";

    final MockResponse validResponse = makeResponse(200, connectorDocumentationBody);
    webServer.enqueue(validResponse);

    final RemoteDefinitionsProvider remoteDefinitionsProvider =
        new RemoteDefinitionsProvider(baseUrl, DEPLOYMENT_MODE, TimeUnit.SECONDS.toMillis(30));
    final Optional<String> documentationResult = remoteDefinitionsProvider.getConnectorDocumentation(CONNECTOR_REPOSITORY, CONNECTOR_VERSION, false);
    assertTrue(documentationResult.isPresent());
    assertEquals(documentationResult.get(), connectorDocumentationBody);
  }

}
