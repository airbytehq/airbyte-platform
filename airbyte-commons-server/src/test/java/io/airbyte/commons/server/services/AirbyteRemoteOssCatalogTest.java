/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.io.Resources;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AirbyteRemoteOssCatalogTest {

  private MockWebServer webServer;
  private MockResponse validCatalogResponse;
  private MockResponse invalidCatalogResponse;
  private String catalogUrl;

  @BeforeEach
  void setup() throws IOException {
    webServer = new MockWebServer();
    catalogUrl = webServer.url("json/remote_oss_catalog.json").toString();

    final URL testCatalog = Resources.getResource("json/remote_oss_catalog.json");
    final String jsonBody = Resources.toString(testCatalog, Charset.defaultCharset());
    validCatalogResponse = new MockResponse().setResponseCode(200)
        .addHeader("Content-Type", "application/json; charset=utf-8")
        .addHeader("Cache-Control", "no-cache")
        .setBody(jsonBody);

    invalidCatalogResponse = new MockResponse().setResponseCode(404)
        .addHeader("Content-Type", "application/json; charset=utf-8")
        .addHeader("Cache-Control", "no-cache")
        .setBody("not found");
  }

  @Test
  @SuppressWarnings({"PMD.AvoidDuplicateLiterals"})
  void testReceivedData() throws Exception {
    webServer.enqueue(validCatalogResponse);

    final AirbyteRemoteOssCatalog remoteDefinitionsProvider = new AirbyteRemoteOssCatalog(catalogUrl);
    final UUID stripeSourceId = UUID.fromString("e094cb9a-26de-4645-8761-65c0c425d1de");
    final StandardSourceDefinition stripeSource = remoteDefinitionsProvider.getSourceDefinition(stripeSourceId);
    assertEquals(stripeSourceId, stripeSource.getSourceDefinitionId());
    assertEquals("Stripe", stripeSource.getName());
    assertEquals("airbyte/source-stripe", stripeSource.getDockerRepository());
    assertEquals("https://docs.airbyte.io/integrations/sources/stripe", stripeSource.getDocumentationUrl());
    assertEquals("stripe.svg", stripeSource.getIcon());
    assertEquals(URI.create("https://docs.airbyte.io/integrations/sources/stripe"), stripeSource.getSpec().getDocumentationUrl());
    assertEquals(false, stripeSource.getTombstone());
    assertEquals("0.2.1", stripeSource.getProtocolVersion());
  }

  @Test
  @SuppressWarnings({"PMD.AvoidDuplicateLiterals"})
  void testNoCache() throws Exception {
    webServer.enqueue(validCatalogResponse);
    webServer.enqueue(validCatalogResponse);

    final AirbyteRemoteOssCatalog remoteDefinitionsProvider = new AirbyteRemoteOssCatalog(catalogUrl);

    assertEquals(0, webServer.getRequestCount());

    remoteDefinitionsProvider.getRemoteDefinitionCatalog();
    assertEquals(1, webServer.getRequestCount());

    remoteDefinitionsProvider.getRemoteDefinitionCatalog();
    assertEquals(2, webServer.getRequestCount());
  }

  @Test
  @SuppressWarnings({"PMD.AvoidDuplicateLiterals"})
  void testHandlesNotAvailable() throws Exception {
    webServer.enqueue(invalidCatalogResponse);
    webServer.enqueue(invalidCatalogResponse);

    final AirbyteRemoteOssCatalog remoteDefinitionsProvider = new AirbyteRemoteOssCatalog(catalogUrl);
    final List<StandardDestinationDefinition> definitions = remoteDefinitionsProvider.getDestinationDefinitions();
    assertEquals(0, definitions.size());

    final List<StandardSourceDefinition> sources = remoteDefinitionsProvider.getSourceDefinitions();
    assertEquals(0, sources.size());

  }

}
