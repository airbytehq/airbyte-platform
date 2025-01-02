/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.StreamAttributePrimaryKeyUpdate;
import io.airbyte.api.model.generated.StreamAttributeTransform;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransformUpdateStream;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.SlackNotificationConfiguration;
import io.airbyte.notification.messages.ConnectionInfo;
import io.airbyte.notification.messages.DestinationInfo;
import io.airbyte.notification.messages.SchemaUpdateNotification;
import io.airbyte.notification.messages.SourceInfo;
import io.airbyte.notification.messages.SyncSummary;
import io.airbyte.notification.messages.WorkspaceInfo;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class SlackNotificationClientTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SlackNotificationClientTest.class);
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final String TEST_PATH = "/test";
  private static final String DESTINATION_TEST = "destination-test";
  private static final String JOB_DESCRIPTION = "job description";
  private static final String LOG_URL = "logUrl";
  private static final String SOURCE_TEST = "source-test";
  private static final String CONNECTION_NAME = "connectionName";
  private static final Long JOB_ID = 1L;

  public static final String WEBHOOK_URL = "http://localhost:";
  private static final String EXPECTED_FAIL_MESSAGE = "Your connection connectionName from source-test to destination-test just failed...\n"
      + "This happened with job description\n"
      + "\n"
      + "You can access its logs here: logUrl\n"
      + "\n"
      + "Job ID: 1";
  private static final String EXPECTED_SUCCESS_MESSAGE = "Your connection connectionName from source-test to destination-test succeeded\n"
      + "This was for job description\n"
      + "\n"
      + "You can access its logs here: logUrl\n"
      + "\n"
      + "Job ID: 1";
  private HttpServer server;

  @BeforeEach
  void setup() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.setExecutor(null); // creates a default executor
    server.start();
  }

  @AfterEach
  void tearDown() {
    server.stop(1);
  }

  @Test
  void testBadWebhookUrl() {
    final SlackNotificationClient client =
        new SlackNotificationClient(new SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.getAddress().getPort() + "/bad"));
    final SyncSummary summary = new SyncSummary(
        new WorkspaceInfo(null, null, null),
        new ConnectionInfo(UUID.randomUUID(), CONNECTION_NAME, LOG_URL),
        new SourceInfo(UUID.randomUUID(), SOURCE_TEST, "http://source"),
        new DestinationInfo(UUID.randomUUID(), DESTINATION_TEST, "http://destination"),
        JOB_ID,
        true,
        Instant.MIN,
        Instant.MAX,
        0, 0, 0, 0, 0, 0,
        "");
    assertFalse(client.notifyJobFailure(summary, null));
  }

  @Test
  void testEmptyWebhookUrl() throws IOException, InterruptedException {
    final SlackNotificationClient client =
        new SlackNotificationClient(new SlackNotificationConfiguration());
    final SyncSummary summary = new SyncSummary(
        new WorkspaceInfo(null, null, null),
        new ConnectionInfo(UUID.randomUUID(), CONNECTION_NAME, LOG_URL),
        new SourceInfo(UUID.randomUUID(), SOURCE_TEST, "http://source"),
        new DestinationInfo(UUID.randomUUID(), DESTINATION_TEST, "http://destination"),
        JOB_ID,
        false,
        null, null,
        0, 0, 0, 0, 0, 0,
        JOB_DESCRIPTION);
    assertFalse(client.notifyJobFailure(summary, null));
  }

  @Test
  void testNotifyJobFailure() throws IOException, InterruptedException {
    server.createContext(TEST_PATH, new ServerHandler(EXPECTED_FAIL_MESSAGE));
    final SyncSummary summary = new SyncSummary(
        new WorkspaceInfo(null, null, null),
        new ConnectionInfo(UUID.randomUUID(), CONNECTION_NAME, LOG_URL),
        new SourceInfo(UUID.randomUUID(), SOURCE_TEST, "http://source"),
        new DestinationInfo(UUID.randomUUID(), DESTINATION_TEST, "http://destination"),
        JOB_ID,
        false,
        null, null,
        0, 0, 0, 0, 0, 0,
        JOB_DESCRIPTION);
    final SlackNotificationClient client =
        new SlackNotificationClient(new SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.getAddress().getPort() + TEST_PATH));
    assertTrue(client.notifyJobFailure(summary, null));
  }

  @Test
  void testNotifyJobSuccess() throws IOException, InterruptedException {
    server.createContext(TEST_PATH, new ServerHandler(EXPECTED_SUCCESS_MESSAGE));
    final SyncSummary summary = new SyncSummary(
        new WorkspaceInfo(null, null, null),
        new ConnectionInfo(UUID.randomUUID(), CONNECTION_NAME, LOG_URL),
        new SourceInfo(UUID.randomUUID(), SOURCE_TEST, "http://source"),
        new DestinationInfo(UUID.randomUUID(), DESTINATION_TEST, "http://destination"),
        JOB_ID,
        false,
        null, null,
        0, 0, 0, 0, 0, 0,
        JOB_DESCRIPTION);
    final SlackNotificationClient client =
        new SlackNotificationClient(new SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.getAddress().getPort() + TEST_PATH));
    assertTrue(client.notifyJobSuccess(summary, null));
  }

  @SuppressWarnings("LineLength")
  @Test
  void testNotifyConnectionDisabled() throws IOException, InterruptedException {
    final String expectedNotificationMessage = String.format(
        """
        Your connection from source-test to destination-test was automatically disabled because it failed 20 times consecutively or has been failing for 14 days in a row.

        Please address the failing issues to ensure your syncs continue to run. The most recent attempted job description.

        Workspace ID: %s
        Connection ID: %s
        """,
        WORKSPACE_ID, CONNECTION_ID);

    server.createContext(TEST_PATH, new ServerHandler(expectedNotificationMessage));
    final SlackNotificationClient client =
        new SlackNotificationClient(new SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.getAddress().getPort() + TEST_PATH));
    final SyncSummary summary = new SyncSummary(
        new WorkspaceInfo(WORKSPACE_ID, null, null),
        new ConnectionInfo(CONNECTION_ID, CONNECTION_NAME, "http://connection"),
        new SourceInfo(null, SOURCE_TEST, null),
        new DestinationInfo(null, DESTINATION_TEST, null),
        0,
        false,
        null,
        null,
        0, 0, 0, 0, 0, 0,
        "job description.");
    assertTrue(client.notifyConnectionDisabled(summary, ""));
  }

  @SuppressWarnings("LineLength")
  @Test
  void testNotifyConnectionDisabledWarning() throws IOException, InterruptedException {
    final String expectedNotificationWarningMessage = String.format(
        """
        Your connection from source-test to destination-test is scheduled to be automatically disabled because it either failed 10 times consecutively or there were only failed jobs in the past 7 days. Once it has failed 20 times consecutively or has been failing for 14 days in a row, the connection will be automatically disabled.

        Please address the failing issues to ensure your syncs continue to run. The most recent attempted job description.

        Workspace ID: %s
        Connection ID: %s
        """,
        WORKSPACE_ID, CONNECTION_ID);

    server.createContext(TEST_PATH, new ServerHandler(expectedNotificationWarningMessage));
    final SlackNotificationClient client =
        new SlackNotificationClient(new SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.getAddress().getPort() + TEST_PATH));
    final SyncSummary summary = new SyncSummary(
        new WorkspaceInfo(WORKSPACE_ID, null, null),
        new ConnectionInfo(CONNECTION_ID, CONNECTION_NAME, "http://connection"),
        new SourceInfo(null, SOURCE_TEST, null),
        new DestinationInfo(null, DESTINATION_TEST, null),
        0L,
        false,
        null,
        null,
        0, 0, 0, 0, 0, 0,
        "job description.");

    assertTrue(client.notifyConnectionDisableWarning(summary, ""));
  }

  @Test
  void testNotifySchemaPropagated() throws IOException, InterruptedException {
    final UUID connectionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final CatalogDiff diff = new CatalogDiff();
    final String workspaceName = "";
    final String workspaceUrl = "http://airbyte.io/workspaces/123";
    final String connectionName = "PSQL ->> BigQuery";
    final String sourceName = "";
    final String sourceUrl = "http://airbyte.io/workspaces/123/source/456";
    final boolean isBreaking = false;
    final String connectionUrl = "http://airbyte.io/your_connection";
    final String recipient = "";

    final String expectedNotificationMessage = "The schema of '<http://airbyte.io/your_connection|PSQL -&gt;&gt; BigQuery>' has changed.";
    server.createContext(TEST_PATH, new ServerHandler(expectedNotificationMessage));
    final SlackNotificationClient client =
        new SlackNotificationClient(new SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.getAddress().getPort() + TEST_PATH));

    final UUID workpaceId = UUID.randomUUID();
    final SchemaUpdateNotification notification = new SchemaUpdateNotification(
        new WorkspaceInfo(workpaceId, workspaceName, workspaceUrl),
        new ConnectionInfo(connectionId, connectionName, connectionUrl),
        new SourceInfo(sourceId, sourceName, sourceUrl),
        isBreaking,
        diff);
    assertTrue(
        client.notifySchemaPropagated(notification, recipient));

  }

  @Test
  void testNotifySchemaDiffToApply() {
    final UUID connectionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final CatalogDiff diff = new CatalogDiff();
    final String workspaceName = "";
    final String workspaceUrl = "http://airbyte.io/workspaces/123";
    final String connectionName = "PSQL ->> BigQuery";
    final String sourceName = "";
    final String sourceUrl = "http://airbyte.io/workspaces/123/source/456";
    final boolean isBreaking = false;
    final String connectionUrl = "http://airbyte.io/your_connection";
    final String recipient = "";

    final String expectedNotificationMessage = "Airbyte detected schema changes for '<http://airbyte.io/your_connection|PSQL -&gt;&gt; BigQuery>'.";
    server.createContext(TEST_PATH, new ServerHandler(expectedNotificationMessage));
    final SlackNotificationClient client =
        new SlackNotificationClient(new SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.getAddress().getPort() + TEST_PATH));

    final UUID workpaceId = UUID.randomUUID();
    final SchemaUpdateNotification notification = new SchemaUpdateNotification(
        new WorkspaceInfo(workpaceId, workspaceName, workspaceUrl),
        new ConnectionInfo(connectionId, connectionName, connectionUrl),
        new SourceInfo(sourceId, sourceName, sourceUrl),
        isBreaking,
        diff);

    assertTrue(client.notifySchemaDiffToApply(notification, recipient));
  }

  @Test
  void buildSummaryNewStreamTest() {
    final CatalogDiff diff = new CatalogDiff();
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("ns")));
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(new StreamDescriptor().name("invoices")));

    final String expected = """
                             • Streams (+2/-0/~0)
                               ＋ invoices
                               ＋ ns.foo
                            """;
    assertEquals(expected, SlackNotificationClient.buildSummary(diff));
  }

  @Test
  void buildSummaryDeletedStreamTest() {
    final CatalogDiff diff = new CatalogDiff();
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("deprecated")));
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("also_removed").namespace("schema1")));

    final String expected = """
                             • Streams (+0/-2/~0)
                               － deprecated
                               － schema1.also_removed
                            """;
    assertEquals(expected, SlackNotificationClient.buildSummary(diff));

  }

  @Test
  void buildSummaryUpdatedPkTest() {
    final CatalogDiff diff = new CatalogDiff();
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .updateStream(new StreamTransformUpdateStream()
            .streamAttributeTransforms(List.of(
                new StreamAttributeTransform()
                    .transformType(StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY)
                    .updatePrimaryKey(
                        new StreamAttributePrimaryKeyUpdate()
                            .newPrimaryKey(List.of(List.of("new_pk")))))))
        .streamDescriptor(new StreamDescriptor().name("stream_with_added_pk")));

    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .updateStream(new StreamTransformUpdateStream()
            .streamAttributeTransforms(List.of(
                new StreamAttributeTransform()
                    .transformType(StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY)
                    .updatePrimaryKey(
                        new StreamAttributePrimaryKeyUpdate()
                            .oldPrimaryKey(List.of(List.of("also_old_pk")))
                            .newPrimaryKey(List.of(List.of("new_pk"), List.of("this_one_is_compound")))))))
        .streamDescriptor(new StreamDescriptor().name("another_stream_with_new_pk")));
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .updateStream(new StreamTransformUpdateStream()
            .streamAttributeTransforms(List.of(
                new StreamAttributeTransform()
                    .transformType(StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY)
                    .updatePrimaryKey(
                        new StreamAttributePrimaryKeyUpdate()
                            .oldPrimaryKey(List.of(List.of("this_pk_is_removed")))))))
        .streamDescriptor(new StreamDescriptor().name("stream_with_pk_removed")));

    final String expected = """
                             • Streams (+0/-0/~3)
                               ~ another_stream_with_new_pk
                                 • Primary key changed (also_old_pk -> [new_pk, this_one_is_compound])
                               ~ stream_with_added_pk
                                 • new_pk added as primary key
                               ~ stream_with_pk_removed
                                 • this_pk_is_removed removed as primary key
                            """;

    assertEquals(expected, SlackNotificationClient.buildSummary(diff));

  }

  @Test
  void buildSummaryAlteredStreamTest() {
    final CatalogDiff diff = new CatalogDiff();
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("users").namespace("main"))
        .updateStream(new StreamTransformUpdateStream().fieldTransforms(List.of(
            new FieldTransform().transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD)
                .fieldName(List.of("alpha", "beta", "delta")),
            new FieldTransform().transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD)
                .fieldName(List.of("another_removal")),
            new FieldTransform().transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                .fieldName(List.of("new", "field")),
            new FieldTransform().transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                .fieldName(List.of("added_too")),
            new FieldTransform().transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA)
                .fieldName(List.of("cow"))))));

    final String expected = """
                             • Fields (+2/~1/-2)
                               • main.users
                                 ＋ added_too
                                 ＋ new.field
                                 － alpha.beta.delta
                                 － another_removal
                                 ～ cow
                            """;
    assertEquals(expected, SlackNotificationClient.buildSummary(diff));
  }

  @Test
  void buildSummaryComplexChangeTest() {
    final CatalogDiff diff = new CatalogDiff();
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("ns")));
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("deprecated")));
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("also_removed").namespace("schema1")));
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .updateStream(new StreamTransformUpdateStream()
            .streamAttributeTransforms(List.of(
                new StreamAttributeTransform()
                    .transformType(StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY)
                    .updatePrimaryKey(
                        new StreamAttributePrimaryKeyUpdate()
                            .newPrimaryKey(List.of(List.of("new_pk")))))))
        .streamDescriptor(new StreamDescriptor().name("stream_with_added_pk")));
    diff.addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("users").namespace("main"))
        .updateStream(new StreamTransformUpdateStream().fieldTransforms(List.of(
            new FieldTransform().transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                .fieldName(List.of("new", "field")),
            new FieldTransform().transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                .fieldName(List.of("added_too")),
            new FieldTransform().transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA)
                .fieldName(List.of("cow"))))));

    final String expected = """
                             • Streams (+1/-2/~1)
                               ＋ ns.foo
                               － deprecated
                               － schema1.also_removed
                               ~ stream_with_added_pk
                                 • new_pk added as primary key
                             • Fields (+2/~1/-0)
                               • main.users
                                 ＋ added_too
                                 ＋ new.field
                                 ～ cow
                            """;
    assertEquals(expected, SlackNotificationClient.buildSummary(diff));
  }

  static class ServerHandler implements HttpHandler {

    private final String expectedMessage;

    public ServerHandler(final String expectedMessage) {
      this.expectedMessage = expectedMessage;
    }

    @Override
    public void handle(final HttpExchange t) throws IOException {
      final InputStream is = t.getRequestBody();
      final String body = IOUtils.toString(is, Charset.defaultCharset());
      LOGGER.info("Received: '{}'", body);
      JsonNode message = null;
      try {
        message = Jsons.deserialize(body);
      } catch (final RuntimeException e) {
        LOGGER.error("Failed to parse JSON from body {}", body, e);
      }
      final String response;
      if (message != null && message.has("text") && expectedMessage.equals(message.get("text").asText())) {
        response = "Notification acknowledged!";
        t.sendResponseHeaders(200, response.length());
      } else if (message == null || !message.has("text")) {
        response = "No notification message or message missing `text` node";
        t.sendResponseHeaders(500, response.length());
      } else {
        response = String.format("Wrong notification message: %s", message.get("text").asText());
        t.sendResponseHeaders(500, response.length());
      }
      final OutputStream os = t.getResponseBody();
      os.write(response.getBytes(StandardCharsets.UTF_8));
      os.close();
    }

  }

}
