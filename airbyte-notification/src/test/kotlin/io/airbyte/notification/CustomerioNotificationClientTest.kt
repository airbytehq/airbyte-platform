/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.FieldTransform
import io.airbyte.api.model.generated.StreamAttributePrimaryKeyUpdate
import io.airbyte.api.model.generated.StreamAttributeTransform
import io.airbyte.api.model.generated.StreamDescriptor
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.api.model.generated.StreamTransformUpdateStream
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.Resources
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorType
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardWorkspace
import io.airbyte.notification.CustomerioNotificationClient.Companion.buildSchemaChangeJson
import io.airbyte.notification.CustomerioNotificationClient.Companion.buildSyncCompletedJson
import io.airbyte.notification.messages.ConnectionInfo
import io.airbyte.notification.messages.DestinationInfo
import io.airbyte.notification.messages.SchemaUpdateNotification
import io.airbyte.notification.messages.SourceInfo
import io.airbyte.notification.messages.SyncSummary
import io.airbyte.notification.messages.WorkspaceInfo
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.apache.http.HttpHeaders
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.time.Instant
import java.util.UUID
import java.util.function.Consumer

internal class CustomerioNotificationClientTest {
  private lateinit var mockWebServer: MockWebServer
  private lateinit var customerioNotificationClient: CustomerioNotificationClient

  @BeforeEach
  fun setUp() {
    mockWebServer = MockWebServer()
    val baseUrl = mockWebServer.url("/").toString()
    customerioNotificationClient = CustomerioNotificationClient(baseUrl, API_KEY)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testSendNotifyRequest() {
    mockWebServer.enqueue(MockResponse())

    val result =
      customerioNotificationClient.sendNotifyRequest(API_ENDPOINT, "{}", UUID.randomUUID())

    Assertions.assertTrue(result)

    val recordedRequest = mockWebServer.takeRequest()
    Assertions.assertEquals("POST", recordedRequest.method)
    Assertions.assertEquals("/" + API_ENDPOINT, recordedRequest.path)
    Assertions.assertEquals("Bearer " + API_KEY, recordedRequest.getHeader(HttpHeaders.AUTHORIZATION))
    Assertions.assertEquals("application/json; charset=utf-8", recordedRequest.getHeader(HttpHeaders.CONTENT_TYPE))
    Assertions.assertEquals("{}", recordedRequest.body.readUtf8())
  }

  @Test
  fun testSendNotifyRequestFailureThrowsException() {
    mockWebServer.enqueue(MockResponse().setResponseCode(500))
    Assertions.assertThrows(
      IOException::class.java,
    ) { customerioNotificationClient.sendNotifyRequest(API_ENDPOINT, "", UUID.randomUUID()) }
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testNotifyByEmailBroadcast() {
    mockWebServer.enqueue(MockResponse())

    val result = customerioNotificationClient.notifyByEmailBroadcast("123", EMAIL_LIST, java.util.Map.of("key", "value"))
    Assertions.assertTrue(result)

    val recordedRequest = mockWebServer.takeRequest()
    Assertions.assertEquals("/v1/campaigns/123/triggers", recordedRequest.path)
    Assertions.assertEquals("Bearer " + API_KEY, recordedRequest.getHeader(HttpHeaders.AUTHORIZATION))

    val reqBody = Jsons.deserialize(recordedRequest.body.readUtf8())
    reqBody["emails"].forEach(
      Consumer { email: JsonNode ->
        Assertions.assertTrue(
          EMAIL_LIST.contains(email.asText()),
        )
      },
    )
    Assertions.assertEquals("value", reqBody["data"]["key"].asText())
    Assertions.assertTrue(reqBody["email_add_duplicates"].asBoolean())
    Assertions.assertTrue(reqBody["email_ignore_missing"].asBoolean())
    Assertions.assertTrue(reqBody["id_ignore_missing"].asBoolean())
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testNotifyBreakingChangeWarning() {
    mockWebServer.enqueue(MockResponse())

    val connectorName = "MyConnector"
    val breakingChange =
      ActorDefinitionBreakingChange()
        .withUpgradeDeadline("2021-01-01")
        .withMessage("my **breaking** change message [link](https://airbyte.io/)")
        .withVersion(Version("2.0.0"))
        .withMigrationDocumentationUrl("https://airbyte.io/docs/migration-guide")

    val result = customerioNotificationClient.notifyBreakingChangeWarning(EMAIL_LIST, connectorName, ActorType.SOURCE, breakingChange)
    Assertions.assertTrue(result)

    val recordedRequest = mockWebServer.takeRequest()
    Assertions.assertEquals("/v1/campaigns/32/triggers", recordedRequest.path)

    val expectedData =
      java.util.Map.of(
        "connector_type",
        "source",
        "connector_name",
        connectorName,
        "connector_version_new",
        breakingChange.version.serialize(),
        "connector_version_change_description",
        "<p>my <strong>breaking</strong> change message <a href=\"https://airbyte.io/\">link</a></p>\n",
        "connector_version_upgrade_deadline",
        "January 1, 2021",
        "connector_version_migration_url",
        breakingChange.migrationDocumentationUrl,
      )

    val reqBody = Jsons.deserialize(recordedRequest.body.readUtf8())
    Assertions.assertEquals(
      expectedData,
      Jsons.`object`(reqBody["data"], Map::class.java),
    )
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testNotifyBreakingChangeSyncsDisabled() {
    mockWebServer.enqueue(MockResponse().setResponseCode(429))
    mockWebServer.enqueue(MockResponse().setResponseCode(429))
    mockWebServer.enqueue(MockResponse())

    val connectorName = "MyConnector"
    val breakingChange =
      ActorDefinitionBreakingChange()
        .withUpgradeDeadline("2021-01-01")
        .withMessage("my breaking change message")
        .withVersion(Version("2.0.0"))
        .withMigrationDocumentationUrl("https://airbyte.io/docs/migration-guide")

    val result =
      customerioNotificationClient.notifyBreakingChangeSyncsDisabled(EMAIL_LIST, connectorName, ActorType.DESTINATION, breakingChange)
    Assertions.assertTrue(result)

    val recordedRequest = mockWebServer.takeRequest()
    Assertions.assertEquals("/v1/campaigns/33/triggers", recordedRequest.path)

    val expectedData =
      java.util.Map.of(
        "connector_type",
        "destination",
        "connector_name",
        connectorName,
        "connector_version_new",
        breakingChange.version.serialize(),
        "connector_version_change_description",
        "<p>my breaking change message</p>\n",
        "connector_version_migration_url",
        breakingChange.migrationDocumentationUrl,
      )

    val reqBody = Jsons.deserialize(recordedRequest.body.readUtf8())
    Assertions.assertEquals(
      expectedData,
      Jsons.`object`(reqBody["data"], Map::class.java),
    )
  }

  // this only tests that the headers are set correctly and that a http post request is sent to the
  // correct URI
  // this test does _not_ check the body of the request.
  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testNotifyConnectionDisabled() {
    mockWebServer.enqueue(MockResponse())

    val summary =
      SyncSummary(
        WorkspaceInfo(WORKSPACE_ID, "", ""),
        ConnectionInfo(CONNECTION_ID, "", ""),
        SourceInfo(SOURCE_ID, RANDOM_INPUT, ""),
        DestinationInfo(DESTINATION_ID, RANDOM_INPUT, ""),
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
        RANDOM_INPUT,
        null,
        null,
      )
    val result =
      customerioNotificationClient.notifyConnectionDisabled(summary, WORKSPACE.email)

    Assertions.assertTrue(result)

    val recordedRequest = mockWebServer.takeRequest()
    Assertions.assertEquals("/v1/send/email", recordedRequest.path)
    Assertions.assertEquals("Bearer " + API_KEY, recordedRequest.getHeader(HttpHeaders.AUTHORIZATION))
  }

  @Test
  fun testBuildSchemaNotificationMessageData() {
    val workspaceId = UUID.randomUUID()
    val workspaceName = "test_workspace"
    val connectionId = UUID.randomUUID()
    val connectionName = "connection foo"
    val sourceId = UUID.randomUUID()
    val sourceName = "facebook marketing"
    val diff =
      CatalogDiff()
        .addTransformsItem(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(StreamDescriptor().name("updatedStream"))
            .updateStream(
              StreamTransformUpdateStream().addFieldTransformsItem(
                FieldTransform()
                  .transformType(
                    FieldTransform.TransformTypeEnum.REMOVE_FIELD,
                  ).breaking(true),
              ),
            ),
        ).addTransformsItem(
          StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM).streamDescriptor(StreamDescriptor().name("foo")),
        ).addTransformsItem(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
            .streamDescriptor(StreamDescriptor().name("removed")),
        ).addTransformsItem(
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
    val recipient = "airbyte@airbyte.io"
    val transactionMessageId = "455"
    val notification =
      SchemaUpdateNotification(
        WorkspaceInfo(workspaceId, workspaceName, ""),
        ConnectionInfo(connectionId, connectionName, ""),
        SourceInfo(sourceId, sourceName, ""),
        false,
        diff,
      )

    val node =
      buildSchemaChangeJson(notification, recipient, transactionMessageId)

    Assertions.assertEquals(transactionMessageId, node["transactional_message_id"].asText())
    Assertions.assertEquals(recipient, node["to"].asText())
    Assertions.assertEquals(sourceName, node["message_data"]["source_name"].asText())
    Assertions.assertEquals(connectionName, node["message_data"]["connection_name"].asText())

    Assertions.assertTrue(node["message_data"]["changes"]["new_streams"].isArray)
    Assertions.assertEquals(1, node["message_data"]["changes"]["new_streams"].size())
    Assertions.assertTrue(node["message_data"]["changes"]["deleted_streams"].isArray)
    Assertions.assertEquals(1, node["message_data"]["changes"]["deleted_streams"].size())
    Assertions.assertTrue(node["message_data"]["changes"]["modified_streams"].isObject)
    Assertions.assertEquals(1, node["message_data"]["changes"]["deleted_streams"].size())
    Assertions.assertEquals(1, node["message_data"]["changes"]["modified_streams"]["updatedStream"]["deleted"].size())
  }

  @Test
  @Throws(IOException::class)
  fun testBuildJobSuccessNotificationMessageData() {
    val workspaceId = UUID.fromString("a39591af-6872-41e3-836d-984e35554324")
    val workspaceName = "workspace-name"
    val sourceId = UUID.fromString("6d006cc5-d050-4f1b-9c22-ebd6b7d54b25")
    val sourceName = "source"
    val destinationId = UUID.fromString("26548b2d-78d3-4e4c-a824-3f7d0bb45a0e")
    val destinationName = "destination"
    val connectionId = UUID.fromString("a2e110d4-f196-4fa5-a866-b18ad90e81aa")
    val connectionName = "connection"
    val startedAt = Instant.ofEpochSecond(1000000)
    val finishedAt = Instant.ofEpochSecond(1070000)

    val syncSummary =
      SyncSummary(
        WorkspaceInfo(workspaceId, workspaceName, "http://workspace"),
        ConnectionInfo(connectionId, connectionName, "http://connection"),
        SourceInfo(sourceId, sourceName, "http://source"),
        DestinationInfo(destinationId, destinationName, "http://source"),
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
        "Connection to the source failed",
        FailureReason.FailureType.CONFIG_ERROR,
        FailureReason.FailureOrigin.SOURCE,
      )
    val email = "joe@foobar.com"
    val transactionId = "201"

    val jsonContent = Resources.read("customerio/job_failure_notification.json")
    val mapper = ObjectMapper()
        /*
         * SyncSummary contains Long fields, jackson serialize them into LongNode. A default mapper
         * deserializes any number in the json text into IntNode and somehow IntNode(5) != LongNode(5).
         * Forcing the use of long here ensures that the test works properly
         */
    mapper.enable(DeserializationFeature.USE_LONG_FOR_INTS)
    val expected = mapper.readTree(jsonContent)
    val node = buildSyncCompletedJson(syncSummary, email, transactionId)
    Assertions.assertEquals(expected["message_data"]["bytesEmitted"], node["message_data"]["bytesEmitted"])
    Assertions.assertEquals(expected.size(), node.size())
    Assertions.assertEquals(expected, node)
  }

  companion object {
    private const val API_ENDPOINT = "v1/send"
    private const val API_KEY = "api-key"
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val SOURCE_ID: UUID = UUID.randomUUID()
    private val DESTINATION_ID: UUID = UUID.randomUUID()
    private val WORKSPACE: StandardWorkspace =
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("workspace-name")
        .withEmail("test@airbyte.io")
    private const val RANDOM_INPUT = "input"
    private val EMAIL_LIST =
      listOf(
        "email1@airbyte.com",
        "email2@airbyte.com",
      )
  }
}
