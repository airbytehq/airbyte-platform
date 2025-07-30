/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.common.StreamDescriptorUtils.buildFieldName
import io.airbyte.api.common.StreamDescriptorUtils.buildFullyQualifiedName
import io.airbyte.api.model.generated.FieldTransform
import io.airbyte.api.model.generated.StreamAttributeTransform
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.lang.Exceptions
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorType
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.notification.messages.SchemaUpdateNotification
import io.airbyte.notification.messages.SyncSummary
import io.github.oshai.kotlinlogging.KotlinLogging
import okhttp3.Interceptor
import okhttp3.MediaType
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.apache.http.HttpHeaders
import org.commonmark.parser.Parser
import org.commonmark.renderer.html.HtmlRenderer
import java.io.IOException
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID

private val log = KotlinLogging.logger { }

/**
 * Notification client that uses customer.io API send emails.
 *
 * These notifications rely on `TRANSACTION_MESSAGE_ID`, which are basically templates you create
 * through customer.io. These IDs are specific to a user's account on customer.io, so they will be
 * different for every user. For now, they are stored as variables here, but in the future they may
 * be stored in as a notification config in the database.
 *
 * For Airbyte Cloud, Airbyte engineers may use `DEFAULT_TRANSACTION_MESSAGE_ID = "6"` as a generic
 * template for notifications.
 */
class CustomerioNotificationClient(
  private val baseUrl: String = CUSTOMERIO_BASE_URL,
  private val apiToken: String? = System.getenv(EnvVar.CUSTOMERIO_API_KEY.name),
  private val metricClient: MetricClient? = null,
) : NotificationClient() {
  private val okHttpClient: OkHttpClient =
    OkHttpClient
      .Builder()
      .addInterceptor(CampaignsRateLimitInterceptor())
      .build()

  /**
   * Customer.io has a rate limit of 10 requests per second for broadcasts. This interceptor will
   * sleep for 10 seconds if a broadcast request fails with a 429 error.
   */
  private class CampaignsRateLimitInterceptor : Interceptor {
    @Throws(IOException::class)
    override fun intercept(chain: Interceptor.Chain): Response {
      val request = chain.request()
      var response = chain.proceed(request)

      val maxRetries = 5
      var retryCount = 0
      while (retryCount < maxRetries &&
        !response.isSuccessful &&
        response.code == 429 &&
        request.url.pathSegments.contains(
          CAMPAIGNS_PATH_SEGMENT,
        )
      ) {
        log.info { "sleeping for 10s due to rate limit hit when sending broadcast..." }
        Exceptions.swallow { Thread.sleep(10000) }
        response = chain.proceed(request)
        retryCount++
      }

      return response
    }
  }

  override fun notifyJobFailure(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean {
    val node = buildSyncCompletedJson(summary, receiverEmail, SYNC_FAILURE_MESSAGE_ID)
    val payload = Jsons.serialize(node)
    return try {
      notifyByEmail(payload, summary.workspace.id)
    } catch (e: IOException) {
      log.error(e) { "Error sending job failure email notification for workspaceId ${summary.workspace.id}" }
      false
    }
  }

  override fun notifyJobSuccess(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean {
    val node = buildSyncCompletedJson(summary, receiverEmail, SYNC_SUCCEED_MESSAGE_ID)
    val payload = Jsons.serialize(node)
    return try {
      notifyByEmail(payload, summary.workspace.id)
    } catch (e: IOException) {
      log.error(e) { "Error sending job succcess email notification for workspaceId ${summary.workspace.id}" }
      false
    }
  }

  // Once the configs are editable through the UI, the reciever email should be stored in
  // airbyte-config/models/src/main/resources/types/CustomerioNotificationConfiguration.yaml
  // instead of being passed in
  override fun notifyConnectionDisabled(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean {
    val node = buildSyncCompletedJson(summary, receiverEmail, AUTO_DISABLE_TRANSACTION_MESSAGE_ID)
    val payload = Jsons.serialize(node)
    return try {
      notifyByEmail(payload, summary.workspace.id)
    } catch (e: IOException) {
      log.error(e) { "Error sending connection disabled warning email notification for workspaceId ${summary.workspace.id}" }
      false
    }
  }

  override fun notifyConnectionDisableWarning(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean {
    val node = buildSyncCompletedJson(summary, receiverEmail, AUTO_DISABLE_WARNING_TRANSACTION_MESSAGE_ID)
    val payload = Jsons.serialize(node)
    return try {
      notifyByEmail(payload, summary.workspace.id)
    } catch (e: IOException) {
      log.error(e) { "Error sending connection disabled warning email notification for workspaceId ${summary.workspace.id}" }
      false
    }
  }

  override fun notifyBreakingChangeWarning(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean {
    try {
      return notifyByEmailBroadcast(
        BREAKING_CHANGE_WARNING_BROADCAST_ID,
        receiverEmails,
        java.util.Map.of(
          CONNECTOR_NAME,
          connectorName,
          CONNECTOR_TYPE,
          actorType.value(),
          CONNECTOR_VERSION_NEW,
          breakingChange.version.serialize(),
          CONNECTOR_VERSION_UPGRADE_DEADLINE,
          formatDate(breakingChange.upgradeDeadline),
          CONNECTOR_VERSION_CHANGE_DESCRIPTION,
          convertMarkdownToHtml(breakingChange.message),
          CONNECTOR_VERSION_MIGRATION_URL,
          breakingChange.migrationDocumentationUrl,
        ),
      )
    } catch (e: IOException) {
      log.error(e) { "Failed to dispatch breaking change - sync warning notifications" }
      return false
    }
  }

  override fun notifyBreakingChangeSyncsDisabled(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean {
    try {
      return notifyByEmailBroadcast(
        BREAKING_CHANGE_SYNCS_DISABLED_BROADCAST_ID,
        receiverEmails,
        java.util.Map.of(
          CONNECTOR_NAME,
          connectorName,
          CONNECTOR_TYPE,
          actorType.value(),
          CONNECTOR_VERSION_NEW,
          breakingChange.version.serialize(),
          CONNECTOR_VERSION_CHANGE_DESCRIPTION,
          convertMarkdownToHtml(breakingChange.message),
          CONNECTOR_VERSION_MIGRATION_URL,
          breakingChange.migrationDocumentationUrl,
        ),
      )
    } catch (e: IOException) {
      log.error(e) { "Failed to dispatch breaking change - sync disabled notifications" }
      return false
    }
  }

  override fun notifyBreakingUpcomingAutoUpgrade(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean {
    try {
      return notifyByEmailBroadcast(
        BREAKING_CHANGE_SYNCS_UPCOMING_UPGRADE_BROADCAST_ID,
        receiverEmails,
        java.util.Map.of(
          CONNECTOR_NAME,
          connectorName,
          CONNECTOR_TYPE,
          actorType.value(),
          CONNECTOR_VERSION_NEW,
          breakingChange.version.serialize(),
          CONNECTOR_VERSION_UPGRADE_DEADLINE,
          formatDate(breakingChange.upgradeDeadline),
          CONNECTOR_VERSION_CHANGE_DESCRIPTION,
          convertMarkdownToHtml(breakingChange.message),
          CONNECTOR_VERSION_MIGRATION_URL,
          breakingChange.migrationDocumentationUrl,
        ),
      )
    } catch (e: IOException) {
      log.error(e) { "Failed to dispatch breaking change - sync upcoming auto-upgrade notifications" }
      return false
    }
  }

  override fun notifyBreakingChangeSyncsUpgraded(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean {
    try {
      return notifyByEmailBroadcast(
        BREAKING_CHANGE_SYNCS_UPGRADED_BROADCAST_ID,
        receiverEmails,
        java.util.Map.of(
          CONNECTOR_NAME,
          connectorName,
          CONNECTOR_TYPE,
          actorType.value(),
          CONNECTOR_VERSION_NEW,
          breakingChange.version.serialize(),
          CONNECTOR_VERSION_CHANGE_DESCRIPTION,
          convertMarkdownToHtml(breakingChange.message),
          CONNECTOR_VERSION_MIGRATION_URL,
          breakingChange.migrationDocumentationUrl,
        ),
      )
    } catch (e: IOException) {
      log.error(e) { "Failed to dispatch breaking change - sync auto upgraded notifications" }
      return false
    }
  }

  override fun notifySchemaPropagated(
    notification: SchemaUpdateNotification,
    recipient: String?,
    workspaceId: UUID?,
  ): Boolean {
    if (recipient == null) return false

    val transactionalMessageId = if (notification.isBreakingChange) SCHEMA_BREAKING_CHANGE_TRANSACTION_ID else SCHEMA_CHANGE_TRANSACTION_ID

    val node =
      buildSchemaChangeJson(notification, recipient, transactionalMessageId)

    val payload = Jsons.serialize(node)
    return try {
      notifyByEmail(payload, workspaceId)
    } catch (e: IOException) {
      log.error(e) { "Error sending schema propagation email notification for workspaceId $workspaceId" }
      false
    }
  }

  override fun notifySchemaDiffToApply(
    notification: SchemaUpdateNotification,
    recipient: String?,
    workspaceId: UUID?,
  ): Boolean {
    if (recipient == null) return false

    val node =
      buildSchemaChangeJson(notification, recipient, SCHEMA_CHANGE_DETECTED_TRANSACTION_ID)
    val payload = Jsons.serialize(node)
    return try {
      notifyByEmail(payload, workspaceId)
    } catch (e: IOException) {
      log.error(e) { "Error sending schema diff email notification for workspaceId $workspaceId" }
      false
    }
  }

  override fun notifySchemaDiffToApplyWhenPropagationDisabled(
    notification: SchemaUpdateNotification,
    recipient: String?,
    workspaceId: UUID?,
  ): Boolean {
    if (recipient == null) return false

    val node =
      buildSchemaChangeJson(notification, recipient, SCHEMA_CHANGE_DETECTED_AND_PROPAGATION_DISABLED_TRANSACTION_ID)
    val payload = Jsons.serialize(node)
    return try {
      notifyByEmail(payload, workspaceId)
    } catch (e: IOException) {
      log.error(e) { "Error sending schema diff when propagation disabled email notification for workspaceId $workspaceId" }
      false
    }
  }

  override fun getNotificationClientType(): String = CUSTOMERIO_TYPE

  @Throws(IOException::class)
  private fun notifyByEmail(
    requestPayload: String,
    workspaceId: UUID?,
  ): Boolean = sendNotifyRequest(CUSTOMERIO_EMAIL_API_ENDPOINT, requestPayload, workspaceId)

  @VisibleForTesting
  @Throws(IOException::class)
  fun notifyByEmailBroadcast(
    broadcastId: String?,
    emails: List<String>,
    data: Map<String, String>,
  ): Boolean {
    if (emails.isEmpty()) {
      log.info { "No emails to notify. Skipping email notification." }
      return false
    }

    val broadcastTriggerUrl = String.format(CUSTOMERIO_BROADCAST_API_ENDPOINT_TEMPLATE, broadcastId)

    val payload =
      Jsons.serialize(
        java.util.Map.of(
          "emails",
          emails,
          "data",
          data,
          "email_add_duplicates",
          true,
          "email_ignore_missing",
          true,
          "id_ignore_missing",
          true,
        ),
      )

    return sendNotifyRequest(broadcastTriggerUrl, payload, null)
  }

  @VisibleForTesting
  @Throws(IOException::class)
  fun sendNotifyRequest(
    urlEndpoint: String,
    payload: String,
    workspaceId: UUID?,
  ): Boolean {
    if (apiToken == null || apiToken.isEmpty()) {
      log.info { "Customer.io API token is empty. Skipping email notification. workspaceId: $workspaceId" }
      return false
    }

    val url = baseUrl + urlEndpoint
    val requestBody: RequestBody = payload.toRequestBody(JSON)

    val request =
      Request
        .Builder()
        .addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        .addHeader(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", apiToken))
        .url(url)
        .post(requestBody)
        .build()

    okHttpClient.newCall(request).execute().use { response ->
      if (response.isSuccessful) {
        return handleSendSuccess(workspaceId, response)
      } else {
        val errorMessage = handleSendFailure(workspaceId, response)
        throw IOException(errorMessage)
      }
    }
  }

  private fun convertMarkdownToHtml(message: String): String {
    val markdownParser = Parser.builder().build()
    val document = markdownParser.parse(message)
    val renderer = HtmlRenderer.builder().build()
    return renderer.render(document)
  }

  private fun formatDate(dateString: String): String {
    val date = LocalDate.parse(dateString)
    val formatter = DateTimeFormatter.ofPattern("MMMM d, yyyy")
    return date.format(formatter)
  }

  private fun handleSendSuccess(
    workspaceId: UUID?,
    response: Response,
  ): Boolean {
    log.info { "Successful notification workspaceId $workspaceId (${response.code}): ${response.body}" }
    metricClient?.count(
      OssMetricsRegistry.CUSTOMER_IO_EMAIL_NOTIFICATION_SEND,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.WORKSPACE_ID, workspaceId.toString()),
          MetricAttribute(MetricTags.SUCCESS, "true"),
        ),
    )

    return true
  }

  private fun handleSendFailure(
    workspaceId: UUID?,
    response: Response,
  ): String {
    val body = if (response.body != null) response.body!!.string() else ""
    val errorMessage = String.format("Failed to deliver notification (%s): %s", response.code, body)
    log.info { "Error sending notification workspaceId $workspaceId (${response.code}): $errorMessage" }
    metricClient?.count(
      OssMetricsRegistry.CUSTOMER_IO_EMAIL_NOTIFICATION_SEND,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.WORKSPACE_ID, workspaceId.toString()),
          MetricAttribute(MetricTags.SUCCESS, "false"),
        ),
    )
    return errorMessage
  }

  companion object {
    private val MAPPER: ObjectMapper =
      ObjectMapper().apply {
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        setDateFormat(SimpleDateFormat("yyyy-MM-dd hh:mm:ss"))
        registerModule(JavaTimeModule())
      }

    private val JSON: MediaType = "application/json; charset=utf-8".toMediaType()

    // Email templates created in Customer.io
    private const val AUTO_DISABLE_TRANSACTION_MESSAGE_ID = "29"
    private const val AUTO_DISABLE_WARNING_TRANSACTION_MESSAGE_ID = "30"
    private const val BREAKING_CHANGE_WARNING_BROADCAST_ID = "32"
    private const val BREAKING_CHANGE_SYNCS_DISABLED_BROADCAST_ID = "33"
    private const val BREAKING_CHANGE_SYNCS_UPCOMING_UPGRADE_BROADCAST_ID = "48"
    private const val BREAKING_CHANGE_SYNCS_UPGRADED_BROADCAST_ID = "47"
    private const val SCHEMA_CHANGE_TRANSACTION_ID = "25"
    private const val SCHEMA_BREAKING_CHANGE_TRANSACTION_ID = "24"
    private const val SCHEMA_CHANGE_DETECTED_TRANSACTION_ID = "31"
    private const val SCHEMA_CHANGE_DETECTED_AND_PROPAGATION_DISABLED_TRANSACTION_ID = "34"

    private const val SYNC_SUCCEED_MESSAGE_ID = "27"
    private const val SYNC_FAILURE_MESSAGE_ID = "26"

    private const val CUSTOMERIO_BASE_URL = "https://api.customer.io/"
    private const val CUSTOMERIO_EMAIL_API_ENDPOINT = "v1/send/email"
    private const val CAMPAIGNS_PATH_SEGMENT = "campaigns"
    private const val CUSTOMERIO_BROADCAST_API_ENDPOINT_TEMPLATE = "v1/$CAMPAIGNS_PATH_SEGMENT/%s/triggers"

    private const val CUSTOMERIO_TYPE = "customerio"
    const val CONNECTOR_NAME: String = "connector_name"
    const val CONNECTOR_TYPE: String = "connector_type"
    const val CONNECTOR_VERSION_NEW: String = "connector_version_new"
    const val CONNECTOR_VERSION_CHANGE_DESCRIPTION: String = "connector_version_change_description"
    const val CONNECTOR_VERSION_MIGRATION_URL: String = "connector_version_migration_url"
    const val CONNECTOR_VERSION_UPGRADE_DEADLINE: String = "connector_version_upgrade_deadline"

    @JvmStatic
    fun buildSyncCompletedJson(
      syncSummary: SyncSummary,
      recipient: String,
      transactionMessageId: String,
    ): ObjectNode {
      val node = MAPPER.createObjectNode()
      node.put("transactional_message_id", transactionMessageId)
      node.put("to", recipient)

      val identifiersNode = MAPPER.createObjectNode()
      identifiersNode.put("email", recipient)
      node.set<JsonNode>("identifiers", identifiersNode)

      node.set<JsonNode>("message_data", MAPPER.valueToTree(syncSummary))
      node.put("disable_message_retention", false)
      node.put("send_to_unsubscribed", true)
      node.put("tracked", true)
      node.put("queue_draft", false)
      node.put("disable_css_preprocessing", true)

      return node
    }

    @JvmStatic
    @VisibleForTesting
    fun buildSchemaChangeJson(
      notification: SchemaUpdateNotification,
      recipient: String,
      transactionalMessageId: String,
    ): ObjectNode {
      val node = MAPPER.createObjectNode()
      node.put("transactional_message_id", transactionalMessageId)
      node.put("to", recipient)

      val identifiersNode = MAPPER.createObjectNode()
      identifiersNode.put("email", recipient)
      node.set<JsonNode>("identifiers", identifiersNode)

      val messageDataNode = MAPPER.createObjectNode()
      messageDataNode.put("connection_name", notification.connectionInfo.name)
      messageDataNode.put("connection_id", notification.connectionInfo.id.toString())
      messageDataNode.put("workspace_id", notification.workspace.id.toString())
      messageDataNode.put("workspace_name", notification.workspace.name)

      val changesNode = MAPPER.createObjectNode()
      messageDataNode.set<JsonNode>("changes", changesNode)

      val diff = notification.catalogDiff

      val newStreams =
        diff.transforms
          .stream()
          .filter { t: StreamTransform -> t.transformType == StreamTransform.TransformTypeEnum.ADD_STREAM }
          .toList()
      log.info { "Notify schema changes on new streams: $newStreams" }
      val newStreamsNodes = MAPPER.createArrayNode()
      changesNode.set<JsonNode>("new_streams", newStreamsNodes)
      for (stream in newStreams) {
        newStreamsNodes.add(buildFullyQualifiedName(stream.streamDescriptor))
      }

      val deletedStreams =
        diff.transforms
          .stream()
          .filter { t: StreamTransform -> t.transformType == StreamTransform.TransformTypeEnum.REMOVE_STREAM }
          .toList()
      log.info { "Notify schema changes on deleted streams: $deletedStreams" }
      val deletedStreamsNodes = MAPPER.createArrayNode()
      changesNode.set<JsonNode>("deleted_streams", deletedStreamsNodes)
      for (stream in deletedStreams) {
        deletedStreamsNodes.add(buildFullyQualifiedName(stream.streamDescriptor))
      }

      val alteredStreams =
        diff.transforms
          .stream()
          .filter { t: StreamTransform -> t.transformType == StreamTransform.TransformTypeEnum.UPDATE_STREAM }
          .toList()
      log.info { "Notify schema changes on altered streams: $alteredStreams" }
      val modifiedStreamsNodes = MAPPER.createObjectNode()
      changesNode.set<JsonNode>("modified_streams", modifiedStreamsNodes)

      for (stream in alteredStreams) {
        val streamNode = MAPPER.createObjectNode()
        modifiedStreamsNodes.set<JsonNode>(buildFullyQualifiedName(stream.streamDescriptor), streamNode)
        val newFields = MAPPER.createArrayNode()
        val deletedFields = MAPPER.createArrayNode()
        val modifiedFields = MAPPER.createArrayNode()
        val updatedPrimaryKeyInfo = MAPPER.createArrayNode()

        streamNode.set<JsonNode>("new", newFields)
        streamNode.set<JsonNode>("deleted", deletedFields)
        streamNode.set<JsonNode>("altered", modifiedFields)
        streamNode.set<JsonNode>("updated_primary_key", updatedPrimaryKeyInfo)

        val primaryKeyChangeOptional =
          stream.updateStream.streamAttributeTransforms
            .stream()
            .filter { t: StreamAttributeTransform -> t.transformType == StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY }
            .findFirst()

        if (primaryKeyChangeOptional.isPresent) {
          val primaryKeyChange = primaryKeyChangeOptional.get()
          val oldPrimaryKey = primaryKeyChange.updatePrimaryKey.oldPrimaryKey
          val oldPrimaryKeyString = formatPrimaryKeyString(oldPrimaryKey)

          val newPrimaryKey = primaryKeyChange.updatePrimaryKey.newPrimaryKey
          val newPrimaryKeyString = formatPrimaryKeyString(newPrimaryKey)

          if (!oldPrimaryKeyString.isEmpty() && newPrimaryKeyString.isEmpty()) {
            updatedPrimaryKeyInfo.add(String.format("%s removed as primary key", oldPrimaryKeyString))
          } else if (oldPrimaryKeyString.isEmpty() && !newPrimaryKeyString.isEmpty()) {
            updatedPrimaryKeyInfo.add(String.format("%s added as primary key", newPrimaryKeyString))
          } else if (!oldPrimaryKeyString.isEmpty()) {
            updatedPrimaryKeyInfo.add(String.format("Primary key changed (%s -> %s)", oldPrimaryKeyString, newPrimaryKeyString))
          }
        }

        for (fieldChange in stream.updateStream.fieldTransforms) {
          val fieldName = buildFieldName(fieldChange.fieldName)
          when (fieldChange.transformType) {
            FieldTransform.TransformTypeEnum.ADD_FIELD -> newFields.add(fieldName)
            FieldTransform.TransformTypeEnum.REMOVE_FIELD -> deletedFields.add(fieldName)
            FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA -> modifiedFields.add(fieldName)
            else -> log.warn { "Unknown TransformType: 'fieldChange.transformType}'" }
          }
        }
      }
      messageDataNode.put("source", notification.sourceInfo.name)
      messageDataNode.put("source_name", notification.sourceInfo.name)
      messageDataNode.put("source_id", notification.sourceInfo.id.toString())

      node.set<JsonNode>("message_data", messageDataNode)

      node.put("disable_message_retention", false)
      node.put("send_to_unsubscribed", true)
      node.put("tracked", true)
      node.put("queue_draft", false)
      node.put("disable_css_preprocessing", true)
      return node
    }
  }
}
