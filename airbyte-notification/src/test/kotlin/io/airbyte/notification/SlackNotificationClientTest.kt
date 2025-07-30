/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import com.fasterxml.jackson.databind.JsonNode
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.FieldTransform
import io.airbyte.api.model.generated.StreamAttributePrimaryKeyUpdate
import io.airbyte.api.model.generated.StreamAttributeTransform
import io.airbyte.api.model.generated.StreamDescriptor
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.api.model.generated.StreamTransformUpdateStream
import io.airbyte.commons.json.Jsons
import io.airbyte.config.FailureReason
import io.airbyte.config.SlackNotificationConfiguration
import io.airbyte.notification.SlackNotificationClient.Companion.buildSummary
import io.airbyte.notification.messages.ConnectionInfo
import io.airbyte.notification.messages.DestinationInfo
import io.airbyte.notification.messages.SchemaUpdateNotification
import io.airbyte.notification.messages.SourceInfo
import io.airbyte.notification.messages.SyncSummary
import io.airbyte.notification.messages.WorkspaceInfo
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID

internal class SlackNotificationClientTest {
  private lateinit var server: HttpServer

  @BeforeEach
  @Throws(IOException::class)
  fun setup() {
    server = HttpServer.create(InetSocketAddress(0), 0)
    server.setExecutor(null) // creates a default executor
    server.start()
  }

  @AfterEach
  fun tearDown() {
    server.stop(1)
  }

  @Test
  fun testBadWebhookUrl() {
    val client =
      SlackNotificationClient(SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.address.port + "/bad"))
    val summary =
      SyncSummary(
        WorkspaceInfo(null, null, null),
        ConnectionInfo(UUID.randomUUID(), CONNECTION_NAME, LOG_URL),
        SourceInfo(UUID.randomUUID(), SOURCE_TEST, "http://source"),
        DestinationInfo(UUID.randomUUID(), DESTINATION_TEST, "http://destination"),
        JOB_ID,
        true,
        Instant.MIN,
        Instant.MAX,
        0,
        0,
        0,
        0,
        0,
        0,
        "",
        null,
        null,
      )
    Assertions.assertFalse(client.notifyJobFailure(summary, ""))
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testEmptyWebhookUrl() {
    val client =
      SlackNotificationClient(SlackNotificationConfiguration())
    val summary =
      SyncSummary(
        WorkspaceInfo(null, null, null),
        ConnectionInfo(UUID.randomUUID(), CONNECTION_NAME, LOG_URL),
        SourceInfo(UUID.randomUUID(), SOURCE_TEST, "http://source"),
        DestinationInfo(UUID.randomUUID(), DESTINATION_TEST, "http://destination"),
        JOB_ID,
        false,
        null,
        null,
        0,
        0,
        0,
        0,
        0,
        0,
        JOB_DESCRIPTION,
        null,
        null,
      )
    Assertions.assertFalse(client.notifyJobFailure(summary, ""))
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testNotifyJobFailure() {
    server.createContext(TEST_PATH, ServerHandler(EXPECTED_FAIL_MESSAGE))
    val summary =
      SyncSummary(
        WorkspaceInfo(null, null, null),
        ConnectionInfo(UUID.randomUUID(), CONNECTION_NAME, LOG_URL),
        SourceInfo(UUID.randomUUID(), SOURCE_TEST, "http://source"),
        DestinationInfo(UUID.randomUUID(), DESTINATION_TEST, "http://destination"),
        JOB_ID,
        false,
        null,
        null,
        0,
        0,
        0,
        0,
        0,
        0,
        JOB_DESCRIPTION,
        FailureReason.FailureType.CONFIG_ERROR,
        FailureReason.FailureOrigin.DESTINATION,
      )
    val client =
      SlackNotificationClient(SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.address.port + TEST_PATH))
    Assertions.assertTrue(client.notifyJobFailure(summary, ""))
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testNotifyJobSuccess() {
    server.createContext(TEST_PATH, ServerHandler(EXPECTED_SUCCESS_MESSAGE))
    val summary =
      SyncSummary(
        WorkspaceInfo(null, null, null),
        ConnectionInfo(UUID.randomUUID(), CONNECTION_NAME, LOG_URL),
        SourceInfo(UUID.randomUUID(), SOURCE_TEST, "http://source"),
        DestinationInfo(UUID.randomUUID(), DESTINATION_TEST, "http://destination"),
        JOB_ID,
        false,
        null,
        null,
        0,
        0,
        0,
        0,
        0,
        0,
        JOB_DESCRIPTION,
        null,
        null,
      )
    val client =
      SlackNotificationClient(SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.address.port + TEST_PATH))
    Assertions.assertTrue(client.notifyJobSuccess(summary, ""))
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testNotifyConnectionDisabled() {
    val expectedNotificationMessage =
      String.format(
        """
        Your connection from source-test to destination-test was automatically disabled because it failed 20 times consecutively or has been failing for 14 days in a row.

        Please address the failing issues to ensure your syncs continue to run. The most recent attempted job description.

        Workspace ID: %s
        Connection ID: %s
        
        """.trimIndent(),
        WORKSPACE_ID,
        CONNECTION_ID,
      )

    server.createContext(TEST_PATH, ServerHandler(expectedNotificationMessage))
    val client =
      SlackNotificationClient(SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.address.port + TEST_PATH))
    val summary =
      SyncSummary(
        WorkspaceInfo(WORKSPACE_ID, null, null),
        ConnectionInfo(CONNECTION_ID, CONNECTION_NAME, "http://connection"),
        SourceInfo(SOURCE_ID, SOURCE_TEST, "http://connection"),
        DestinationInfo(DESTINATION_ID, DESTINATION_TEST, "http://connection"),
        0,
        false,
        null,
        null,
        0,
        0,
        0,
        0,
        0,
        0,
        "job description.",
        null,
        null,
      )
    Assertions.assertTrue(client.notifyConnectionDisabled(summary, ""))
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testNotifyConnectionDisabledWarning() {
    val expectedNotificationWarningMessage =
      String.format(
        """
        Your connection from source-test to destination-test is scheduled to be automatically disabled because it either failed 10 times consecutively or there were only failed jobs in the past 7 days. Once it has failed 20 times consecutively or has been failing for 14 days in a row, the connection will be automatically disabled.

        Please address the failing issues to ensure your syncs continue to run. The most recent attempted job description.

        Workspace ID: %s
        Connection ID: %s
        
        """.trimIndent(),
        WORKSPACE_ID,
        CONNECTION_ID,
      )

    server.createContext(TEST_PATH, ServerHandler(expectedNotificationWarningMessage))
    val client =
      SlackNotificationClient(SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.address.port + TEST_PATH))
    val summary =
      SyncSummary(
        WorkspaceInfo(WORKSPACE_ID, null, null),
        ConnectionInfo(CONNECTION_ID, CONNECTION_NAME, "http://connection"),
        SourceInfo(SOURCE_ID, SOURCE_TEST, "http://connection"),
        DestinationInfo(DESTINATION_ID, DESTINATION_TEST, "http://connection"),
        0L,
        false,
        null,
        null,
        0,
        0,
        0,
        0,
        0,
        0,
        "job description.",
        null,
        null,
      )

    Assertions.assertTrue(client.notifyConnectionDisableWarning(summary, ""))
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testNotifySchemaPropagated() {
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val diff = CatalogDiff()
    val workspaceName = ""
    val workspaceUrl = "http://airbyte.io/workspaces/123"
    val connectionName = "PSQL ->> BigQuery"
    val sourceName = ""
    val sourceUrl = "http://airbyte.io/workspaces/123/source/456"
    val isBreaking = false
    val connectionUrl = "http://airbyte.io/your_connection"
    val recipient = ""

    val expectedNotificationMessage = "The schema of '<http://airbyte.io/your_connection|PSQL -&gt;&gt; BigQuery>' has changed."
    server.createContext(TEST_PATH, ServerHandler(expectedNotificationMessage))
    val client =
      SlackNotificationClient(SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.address.port + TEST_PATH))

    val workpaceId = UUID.randomUUID()
    val notification =
      SchemaUpdateNotification(
        WorkspaceInfo(workpaceId, workspaceName, workspaceUrl),
        ConnectionInfo(connectionId, connectionName, connectionUrl),
        SourceInfo(sourceId, sourceName, sourceUrl),
        isBreaking,
        diff,
      )
    Assertions.assertTrue(
      client.notifySchemaPropagated(notification, recipient, UUID.randomUUID()),
    )
  }

  @Test
  fun testNotifySchemaDiffToApply() {
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val diff = CatalogDiff()
    val workspaceName = ""
    val workspaceUrl = "http://airbyte.io/workspaces/123"
    val connectionName = "PSQL ->> BigQuery"
    val sourceName = ""
    val sourceUrl = "http://airbyte.io/workspaces/123/source/456"
    val isBreaking = false
    val connectionUrl = "http://airbyte.io/your_connection"
    val recipient = ""

    val expectedNotificationMessage = "Airbyte detected schema changes for '<http://airbyte.io/your_connection|PSQL -&gt;&gt; BigQuery>'."
    server.createContext(TEST_PATH, ServerHandler(expectedNotificationMessage))
    val client =
      SlackNotificationClient(SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.address.port + TEST_PATH))

    val workpaceId = UUID.randomUUID()
    val notification =
      SchemaUpdateNotification(
        WorkspaceInfo(workpaceId, workspaceName, workspaceUrl),
        ConnectionInfo(connectionId, connectionName, connectionUrl),
        SourceInfo(sourceId, sourceName, sourceUrl),
        isBreaking,
        diff,
      )

    Assertions.assertTrue(client.notifySchemaDiffToApply(notification, recipient, UUID.randomUUID()))
  }

  @Test
  fun testNotifySchemaDiffToApplyWhenPropagationDisabled() {
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val diff = CatalogDiff()
    val workspaceName = ""
    val workspaceUrl = "http://airbyte.io/workspaces/123"
    val connectionName = "PSQL ->> BigQuery"
    val sourceName = ""
    val sourceUrl = "http://airbyte.io/workspaces/123/source/456"
    val isBreaking = false
    val connectionUrl = "http://airbyte.io/your_connection"
    val recipient = ""

    val expectedNotificationMessage = "Airbyte detected schema changes for '<http://airbyte.io/your_connection|PSQL -&gt;&gt; BigQuery>'."
    server.createContext(TEST_PATH, ServerHandler(expectedNotificationMessage))
    val client =
      SlackNotificationClient(SlackNotificationConfiguration().withWebhook(WEBHOOK_URL + server.address.port + TEST_PATH))

    val workpaceId = UUID.randomUUID()
    val notification =
      SchemaUpdateNotification(
        WorkspaceInfo(workpaceId, workspaceName, workspaceUrl),
        ConnectionInfo(connectionId, connectionName, connectionUrl),
        SourceInfo(sourceId, sourceName, sourceUrl),
        isBreaking,
        diff,
      )

    Assertions.assertTrue(client.notifySchemaDiffToApplyWhenPropagationDisabled(notification, recipient, UUID.randomUUID()))
  }

  @Test
  fun buildSummaryNewStreamTest() {
    val diff = CatalogDiff()
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(StreamDescriptor().name("foo").namespace("ns")),
    )
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(StreamDescriptor().name("invoices")),
    )

    val expected =
      """
      • Streams (+2/-0/~0)
        ＋ invoices
        ＋ ns.foo
      """.trimIndent()
    Assertions.assertEquals(expected, buildSummary(diff).trimIndent())
  }

  @Test
  fun buildSummaryDeletedStreamTest() {
    val diff = CatalogDiff()
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(StreamDescriptor().name("deprecated")),
    )
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(StreamDescriptor().name("also_removed").namespace("schema1")),
    )

    val expected =
      """
      • Streams (+0/-2/~0)
        － deprecated
        － schema1.also_removed
      """.trimIndent()
    Assertions.assertEquals(expected, buildSummary(diff).trimIndent())
  }

  @Test
  fun buildSummaryUpdatedPkTest() {
    val diff = CatalogDiff()
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .updateStream(
          StreamTransformUpdateStream()
            .streamAttributeTransforms(
              listOf(
                StreamAttributeTransform()
                  .transformType(StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY)
                  .updatePrimaryKey(
                    StreamAttributePrimaryKeyUpdate()
                      .newPrimaryKey(listOf(listOf("new_pk"))),
                  ),
              ),
            ),
        ).streamDescriptor(StreamDescriptor().name("stream_with_added_pk")),
    )

    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .updateStream(
          StreamTransformUpdateStream()
            .streamAttributeTransforms(
              listOf(
                StreamAttributeTransform()
                  .transformType(StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY)
                  .updatePrimaryKey(
                    StreamAttributePrimaryKeyUpdate()
                      .oldPrimaryKey(listOf(listOf("also_old_pk")))
                      .newPrimaryKey(
                        listOf(
                          listOf("new_pk"),
                          listOf("this_one_is_compound"),
                        ),
                      ),
                  ),
              ),
            ),
        ).streamDescriptor(StreamDescriptor().name("another_stream_with_new_pk")),
    )
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .updateStream(
          StreamTransformUpdateStream()
            .streamAttributeTransforms(
              listOf(
                StreamAttributeTransform()
                  .transformType(StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY)
                  .updatePrimaryKey(
                    StreamAttributePrimaryKeyUpdate()
                      .oldPrimaryKey(listOf(listOf("this_pk_is_removed"))),
                  ),
              ),
            ),
        ).streamDescriptor(StreamDescriptor().name("stream_with_pk_removed")),
    )

    val expected =
      """
      • Streams (+0/-0/~3)
        ~ another_stream_with_new_pk
          • Primary key changed (also_old_pk -> [new_pk, this_one_is_compound])
        ~ stream_with_added_pk
          • new_pk added as primary key
        ~ stream_with_pk_removed
          • this_pk_is_removed removed as primary key
      """.trimIndent()

    Assertions.assertEquals(expected, buildSummary(diff).trimIndent())
  }

  @Test
  fun buildSummaryAlteredStreamTest() {
    val diff = CatalogDiff()
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(StreamDescriptor().name("users").namespace("main"))
        .updateStream(
          StreamTransformUpdateStream().fieldTransforms(
            listOf(
              FieldTransform()
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD)
                .fieldName(listOf("alpha", "beta", "delta")),
              FieldTransform()
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD)
                .fieldName(listOf("another_removal")),
              FieldTransform()
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                .fieldName(listOf("new", "field")),
              FieldTransform()
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                .fieldName(listOf("added_too")),
              FieldTransform()
                .transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA)
                .fieldName(listOf("cow")),
            ),
          ),
        ),
    )

    val expected =
      """
      • Fields (+2/~1/-2)
        • main.users
          ＋ added_too
          ＋ new.field
          － alpha.beta.delta
          － another_removal
          ～ cow
      """.trimIndent()
    Assertions.assertEquals(expected, buildSummary(diff).trimIndent())
  }

  @Test
  fun buildSummaryComplexChangeTest() {
    val diff = CatalogDiff()
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(StreamDescriptor().name("foo").namespace("ns")),
    )
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(StreamDescriptor().name("deprecated")),
    )
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(StreamDescriptor().name("also_removed").namespace("schema1")),
    )
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .updateStream(
          StreamTransformUpdateStream()
            .streamAttributeTransforms(
              listOf(
                StreamAttributeTransform()
                  .transformType(StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY)
                  .updatePrimaryKey(
                    StreamAttributePrimaryKeyUpdate()
                      .newPrimaryKey(listOf(listOf("new_pk"))),
                  ),
              ),
            ),
        ).streamDescriptor(StreamDescriptor().name("stream_with_added_pk")),
    )
    diff.addTransformsItem(
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(StreamDescriptor().name("users").namespace("main"))
        .updateStream(
          StreamTransformUpdateStream().fieldTransforms(
            listOf(
              FieldTransform()
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                .fieldName(listOf("new", "field")),
              FieldTransform()
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                .fieldName(listOf("added_too")),
              FieldTransform()
                .transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA)
                .fieldName(listOf("cow")),
            ),
          ),
        ),
    )

    val expected =
      """
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
      """.trimIndent()
    Assertions.assertEquals(expected, buildSummary(diff).trimIndent())
  }

  internal class ServerHandler(
    private val expectedMessage: String,
  ) : HttpHandler {
    @Throws(IOException::class)
    override fun handle(t: HttpExchange) {
      val body = String(t.requestBody.readAllBytes())
      log.info { "Received: '$body'" }
      var message: JsonNode? = null
      try {
        message = Jsons.deserialize(body)
      } catch (e: RuntimeException) {
        log.error(e) { "Failed to parse JSON from body $body" }
      }
      val response: String
      if (message != null && message.has("text") && expectedMessage == message["text"].asText()) {
        response = "Notification acknowledged!"
        t.sendResponseHeaders(200, response.length.toLong())
      } else if (message == null || !message.has("text")) {
        response = "No notification message or message missing `text` node"
        t.sendResponseHeaders(500, response.length.toLong())
      } else {
        response = String.format("Wrong notification message: %s", message["text"].asText())
        t.sendResponseHeaders(500, response.length.toLong())
      }
      val os = t.responseBody
      os.write(response.toByteArray(StandardCharsets.UTF_8))
      os.close()
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val SOURCE_ID: UUID = UUID.randomUUID()
    private val DESTINATION_ID: UUID = UUID.randomUUID()
    private const val TEST_PATH = "/test"
    private const val DESTINATION_TEST = "destination-test"
    private const val JOB_DESCRIPTION = "job description"
    private const val LOG_URL = "logUrl"
    private const val SOURCE_TEST = "source-test"
    private const val CONNECTION_NAME = "connectionName"
    private const val JOB_ID = 1L

    const val WEBHOOK_URL: String = "http://localhost:"
    private const val EXPECTED_FAIL_MESSAGE = (
      "Your connection connectionName from source-test to destination-test just failed...\n" +
        "This happened with job description\n" +
        "\n" +
        "You can access its logs here: logUrl\n" +
        "\n" +
        "Job ID: 1"
    )
    private const val EXPECTED_SUCCESS_MESSAGE = (
      "Your connection connectionName from source-test to destination-test succeeded\n" +
        "This was for job description\n" +
        "\n" +
        "You can access its logs here: logUrl\n" +
        "\n" +
        "Job ID: 1"
    )
  }
}
