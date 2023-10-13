/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorType;
import io.airbyte.config.StandardWorkspace;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.http.HttpHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomerioNotificationClientTest {

  private static final String API_ENDPOINT = "v1/send";
  private static final String API_KEY = "api-key";
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final StandardWorkspace WORKSPACE = new StandardWorkspace()
      .withWorkspaceId(WORKSPACE_ID)
      .withName("workspace-name")
      .withEmail("test@airbyte.io");
  private static final String RANDOM_INPUT = "input";
  private static final List<String> EMAIL_LIST = List.of(
      "email1@airbyte.com",
      "email2@airbyte.com");

  private MockWebServer mockWebServer;
  private CustomerioNotificationClient customerioNotificationClient;

  @BeforeEach
  void setUp() {
    mockWebServer = new MockWebServer();

    final String baseUrl = mockWebServer.url("/").toString();
    customerioNotificationClient = new CustomerioNotificationClient(API_KEY, baseUrl);
  }

  @Test
  void testSendNotifyRequest() throws IOException, InterruptedException {
    mockWebServer.enqueue(new MockResponse());

    final boolean result =
        customerioNotificationClient.sendNotifyRequest(API_ENDPOINT, "{}");

    assertTrue(result);

    final RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertEquals("POST", recordedRequest.getMethod());
    assertEquals("/" + API_ENDPOINT, recordedRequest.getPath());
    assertEquals("Bearer " + API_KEY, recordedRequest.getHeader(HttpHeaders.AUTHORIZATION));
    assertEquals("application/json; charset=utf-8", recordedRequest.getHeader(HttpHeaders.CONTENT_TYPE));
    assertEquals("{}", recordedRequest.getBody().readUtf8());
  }

  @Test
  void testSendNotifyRequestFailureThrowsException() {
    mockWebServer.enqueue(new MockResponse().setResponseCode(500));
    assertThrows(IOException.class, () -> customerioNotificationClient.sendNotifyRequest(API_ENDPOINT, ""));
  }

  @Test
  void testNotifyByEmailBroadcast() throws IOException, InterruptedException {
    mockWebServer.enqueue(new MockResponse());

    final boolean result = customerioNotificationClient.notifyByEmailBroadcast("123", EMAIL_LIST, Map.of("key", "value"));
    assertTrue(result);

    final RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertEquals("/v1/campaigns/123/triggers", recordedRequest.getPath());
    assertEquals("Bearer " + API_KEY, recordedRequest.getHeader(HttpHeaders.AUTHORIZATION));

    final JsonNode reqBody = Jsons.deserialize(recordedRequest.getBody().readUtf8());
    reqBody.get("emails").forEach(email -> assertTrue(EMAIL_LIST.contains(email.asText())));
    assertEquals("value", reqBody.get("data").get("key").asText());
    assertTrue(reqBody.get("email_add_duplicates").asBoolean());
    assertTrue(reqBody.get("email_ignore_missing").asBoolean());
    assertTrue(reqBody.get("id_ignore_missing").asBoolean());
  }

  @Test
  void testNotifyBreakingChangeWarning() throws IOException, InterruptedException {
    mockWebServer.enqueue(new MockResponse());

    final String connectorName = "MyConnector";
    final ActorDefinitionBreakingChange breakingChange = new ActorDefinitionBreakingChange()
        .withUpgradeDeadline("2021-01-01")
        .withMessage("my **breaking** change message [link](https://airbyte.io/)")
        .withVersion(new Version("2.0.0"))
        .withMigrationDocumentationUrl("https://airbyte.io/docs/migration-guide");

    final boolean result = customerioNotificationClient.notifyBreakingChangeWarning(EMAIL_LIST, connectorName, ActorType.SOURCE, breakingChange);
    assertTrue(result);

    final RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertEquals("/v1/campaigns/32/triggers", recordedRequest.getPath());

    final Map<String, String> expectedData = Map.of(
        "connector_type", "source",
        "connector_name", connectorName,
        "connector_version_new", breakingChange.getVersion().serialize(),
        "connector_version_change_description", "<p>my <strong>breaking</strong> change message <a href=\"https://airbyte.io/\">link</a></p>\n",
        "connector_version_upgrade_deadline", "January 1, 2021",
        "connector_version_migration_url", breakingChange.getMigrationDocumentationUrl());

    final JsonNode reqBody = Jsons.deserialize(recordedRequest.getBody().readUtf8());
    assertEquals(expectedData, Jsons.object(reqBody.get("data"), Map.class));
  }

  @Test
  void testNotifyBreakingChangeSyncsDisabled() throws IOException, InterruptedException {
    mockWebServer.enqueue(new MockResponse().setResponseCode(429));
    mockWebServer.enqueue(new MockResponse().setResponseCode(429));
    mockWebServer.enqueue(new MockResponse());

    final String connectorName = "MyConnector";
    final ActorDefinitionBreakingChange breakingChange = new ActorDefinitionBreakingChange()
        .withUpgradeDeadline("2021-01-01")
        .withMessage("my breaking change message")
        .withVersion(new Version("2.0.0"))
        .withMigrationDocumentationUrl("https://airbyte.io/docs/migration-guide");

    final boolean result =
        customerioNotificationClient.notifyBreakingChangeSyncsDisabled(EMAIL_LIST, connectorName, ActorType.DESTINATION, breakingChange);
    assertTrue(result);

    final RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertEquals("/v1/campaigns/33/triggers", recordedRequest.getPath());

    final Map<String, String> expectedData = Map.of(
        "connector_type", "destination",
        "connector_name", connectorName,
        "connector_version_new", breakingChange.getVersion().serialize(),
        "connector_version_change_description", "<p>my breaking change message</p>\n",
        "connector_version_migration_url", breakingChange.getMigrationDocumentationUrl());

    final JsonNode reqBody = Jsons.deserialize(recordedRequest.getBody().readUtf8());
    assertEquals(expectedData, Jsons.object(reqBody.get("data"), Map.class));
  }

  // this only tests that the headers are set correctly and that a http post request is sent to the
  // correct URI
  // this test does _not_ check the body of the request.
  @Test
  void testNotifyConnectionDisabled() throws IOException, InterruptedException {
    mockWebServer.enqueue(new MockResponse());

    final boolean result =
        customerioNotificationClient.notifyConnectionDisabled(WORKSPACE.getEmail(), RANDOM_INPUT, RANDOM_INPUT, RANDOM_INPUT, WORKSPACE_ID,
            CONNECTION_ID);

    assertTrue(result);

    final RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertEquals("/v1/send/email", recordedRequest.getPath());
    assertEquals("Bearer " + API_KEY, recordedRequest.getHeader(HttpHeaders.AUTHORIZATION));
  }

}
