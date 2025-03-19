/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.StreamAttributePrimaryKeyUpdate;
import io.airbyte.api.model.generated.StreamAttributeTransform;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum;
import io.airbyte.api.model.generated.StreamTransformUpdateStream;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.notification.messages.ConnectionInfo;
import io.airbyte.notification.messages.DestinationInfo;
import io.airbyte.notification.messages.SchemaUpdateNotification;
import io.airbyte.notification.messages.SourceInfo;
import io.airbyte.notification.messages.SyncSummary;
import io.airbyte.notification.messages.WorkspaceInfo;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.http.HttpHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
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

    SyncSummary summary = new SyncSummary(
        new WorkspaceInfo(WORKSPACE_ID, null, null),
        new ConnectionInfo(CONNECTION_ID, null, null),
        new SourceInfo(null, RANDOM_INPUT, null),
        new DestinationInfo(null, RANDOM_INPUT, null),
        10L,
        false,
        Instant.ofEpochSecond(1000000),
        Instant.ofEpochSecond(2000000),
        123240L,
        9000L,
        780,
        600,
        0,
        0,
        RANDOM_INPUT);
    final boolean result =
        customerioNotificationClient.notifyConnectionDisabled(summary, WORKSPACE.getEmail());

    assertTrue(result);

    final RecordedRequest recordedRequest = mockWebServer.takeRequest();
    assertEquals("/v1/send/email", recordedRequest.getPath());
    assertEquals("Bearer " + API_KEY, recordedRequest.getHeader(HttpHeaders.AUTHORIZATION));
  }

  @Test
  void testBuildSchemaNotificationMessageData() {
    UUID workspaceId = UUID.randomUUID();
    String workspaceName = "test_workspace";
    UUID connectionId = UUID.randomUUID();
    String connectionName = "connection foo";
    UUID sourceId = UUID.randomUUID();
    String sourceName = "facebook marketing";
    CatalogDiff diff = new CatalogDiff()
        .addTransformsItem(
            new StreamTransform().transformType(TransformTypeEnum.UPDATE_STREAM)
                .streamDescriptor(new io.airbyte.api.model.generated.StreamDescriptor().name("updatedStream"))
                .updateStream(new StreamTransformUpdateStream().addFieldTransformsItem(new FieldTransform().transformType(
                    FieldTransform.TransformTypeEnum.REMOVE_FIELD).breaking(true))))
        .addTransformsItem(
            new StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM).streamDescriptor(new StreamDescriptor().name("foo")))
        .addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
            .streamDescriptor(new StreamDescriptor().name("removed")))
        .addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .updateStream(new StreamTransformUpdateStream()
                .streamAttributeTransforms(List.of(
                    new StreamAttributeTransform()
                        .transformType(StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY)
                        .updatePrimaryKey(
                            new StreamAttributePrimaryKeyUpdate()
                                .newPrimaryKey(List.of(List.of("new_pk")))))))
            .streamDescriptor(new StreamDescriptor().name("stream_with_added_pk")));
    String recipient = "airbyte@airbyte.io";
    String transactionMessageId = "455";
    SchemaUpdateNotification notification = new SchemaUpdateNotification(
        new WorkspaceInfo(workspaceId, workspaceName, null),
        new ConnectionInfo(connectionId, connectionName, null),
        new SourceInfo(sourceId, sourceName, null),
        false,
        diff);

    ObjectNode node =
        CustomerioNotificationClient.buildSchemaChangeJson(notification, recipient, transactionMessageId);

    assertEquals(transactionMessageId, node.get("transactional_message_id").asText());
    assertEquals(recipient, node.get("to").asText());
    assertEquals(sourceName, node.get("message_data").get("source_name").asText());
    assertEquals(connectionName, node.get("message_data").get("connection_name").asText());

    assertTrue(node.get("message_data").get("changes").get("new_streams").isArray());
    assertEquals(1, node.get("message_data").get("changes").get("new_streams").size());
    assertTrue(node.get("message_data").get("changes").get("deleted_streams").isArray());
    assertEquals(1, node.get("message_data").get("changes").get("deleted_streams").size());
    assertTrue(node.get("message_data").get("changes").get("modified_streams").isObject());
    assertEquals(1, node.get("message_data").get("changes").get("deleted_streams").size());
    assertEquals(1, node.get("message_data").get("changes").get("modified_streams").get("updatedStream").get("deleted").size());
  }

  @Test
  void testBuildJobSuccessNotificationMessageData() throws IOException {
    UUID workspaceId = UUID.fromString("a39591af-6872-41e3-836d-984e35554324");
    String workspaceName = "workspace-name";
    UUID sourceId = UUID.fromString("6d006cc5-d050-4f1b-9c22-ebd6b7d54b25");
    String sourceName = "source";
    UUID destinationId = UUID.fromString("26548b2d-78d3-4e4c-a824-3f7d0bb45a0e");
    String destinationName = "destination";
    UUID connectionId = UUID.fromString("a2e110d4-f196-4fa5-a866-b18ad90e81aa");
    String connectionName = "connection";
    Instant startedAt = Instant.ofEpochSecond(1000000);
    Instant finishedAt = Instant.ofEpochSecond(1070000);

    SyncSummary syncSummary = new SyncSummary(
        new WorkspaceInfo(workspaceId, workspaceName, "http://workspace"),
        new ConnectionInfo(connectionId, connectionName, "http://connection"),
        new SourceInfo(sourceId, sourceName, "http://source"),
        new DestinationInfo(destinationId, destinationName, "http://source"),
        100L,
        false,
        startedAt,
        finishedAt,
        1000L,
        9000L,
        50,
        48,
        0L,
        0L,
        "Connection to the source failed");
    String email = "joe@foobar.com";
    String transactionId = "201";

    String jsonContent = MoreResources.readResource("customerio/job_failure_notification.json");
    ObjectMapper mapper = new ObjectMapper();
    /*
     * SyncSummary contains Long fields, jackson serialize them into LongNode. A default mapper
     * deserializes any number in the json text into IntNode and somehow IntNode(5) != LongNode(5).
     * Forcing the use of long here ensures that the test works properly
     */
    mapper.enable(DeserializationFeature.USE_LONG_FOR_INTS);
    JsonNode expected = mapper.readTree(jsonContent);
    ObjectNode node = CustomerioNotificationClient.buildSyncCompletedJson(syncSummary, email, transactionId);
    assertEquals(expected.get("message_data").get("bytesEmitted"), node.get("message_data").get("bytesEmitted"));
    assertEquals(expected.size(), node.size());
    assertEquals(expected, node);
  }

}
