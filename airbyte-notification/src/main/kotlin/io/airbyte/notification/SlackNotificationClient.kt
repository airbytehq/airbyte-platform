/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.common.StreamDescriptorUtils.buildFieldName
import io.airbyte.api.common.StreamDescriptorUtils.buildFullyQualifiedName
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.FieldTransform
import io.airbyte.api.model.generated.StreamAttributeTransform
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.commons.resources.Resources
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorType
import io.airbyte.config.SlackNotificationConfiguration
import io.airbyte.notification.messages.SchemaUpdateNotification
import io.airbyte.notification.messages.SyncSummary
import io.airbyte.notification.slack.Notification
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.util.StringUtils
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Optional
import java.util.UUID

private val log = KotlinLogging.logger { }

/**
 * Notification client that uses Slack API for Incoming Webhook to send messages. This class also
 * reads a resource YAML file that defines the template message to send. It is stored as a YAML so
 * that we can easily change the structure of the JSON data expected by the API that we are posting
 * to (and we can write multi-line strings more easily). For example, slack API expects some text
 * message in the { "text" : "Hello World" } field...
 */
class SlackNotificationClient : NotificationClient {
  private val config: SlackNotificationConfiguration
  private val tag: String?

  constructor(slackNotificationConfiguration: SlackNotificationConfiguration) {
    this.config = slackNotificationConfiguration
    this.tag = null
  }

  constructor(
    slackNotificationConfiguration: SlackNotificationConfiguration,
    tag: String?,
  ) {
    this.config = slackNotificationConfiguration
    this.tag = tag
  }

  override fun notifyJobFailure(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean {
    val legacyMessage: String
    try {
      legacyMessage =
        renderTemplate(
          "slack/failure_slack_notification_template.txt",
          summary.connection.name,
          summary.source.name,
          summary.destination.name,
          summary.errorMessage,
          summary.connection.url,
          summary.jobId.toString(),
        )
    } catch (e: IOException) {
      log.error(e) {
        "Error rendering slack notification template, job failure notification not sent for workspaceId ${summary.workspace.id} jobId ${summary.jobId}"
      }
      return false
    }
    val notification =
      buildJobCompletedNotification(
        summary,
        "Sync failure occurred",
        legacyMessage,
        Optional.empty(),
        this.tag,
      )
    notification.setData(summary)
    return try {
      notifyJson(notification.toJsonNode(), summary.workspace.id)
    } catch (e: IOException) {
      log.error(e) { "Error sending job failure Slack notification for workspaceId ${summary.workspace.id} jobId ${summary.jobId}" }
      false
    }
  }

  override fun notifyJobSuccess(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean {
    val legacyMessage: String
    try {
      legacyMessage =
        renderTemplate(
          "slack/success_slack_notification_template.txt",
          summary.connection.name,
          summary.source.name,
          summary.destination.name,
          summary.errorMessage,
          summary.connection.url,
          summary.jobId.toString(),
        )
    } catch (e: IOException) {
      log.error(e) {
        "Error rendering slack notification template, job success notification not sent for workspaceId ${summary.workspace.id} jobId ${summary.jobId}"
      }
      return false
    }
    val notification =
      buildJobCompletedNotification(
        summary,
        "Sync completed",
        legacyMessage,
        Optional.empty(),
        this.tag,
      )
    notification.setData(summary)
    return try {
      notifyJson(notification.toJsonNode(), summary.workspace.id)
    } catch (e: IOException) {
      log.error(e) { "Error sending job success Slack notification for workspaceId ${summary.workspace.id} jobId ${summary.jobId}" }
      false
    }
  }

  override fun notifyConnectionDisabled(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean {
    val legacyMessage: String
    try {
      legacyMessage =
        renderTemplate(
          "slack/auto_disable_slack_notification_template.txt",
          summary.source.name,
          summary.destination.name,
          summary.errorMessage,
          summary.workspace.id.toString(),
          summary.connection.id.toString(),
        )
    } catch (e: IOException) {
      log.error(e) {
        "Error rendering slack notification template, connection disabled notification not sent for workspaceId ${summary.workspace.id} jobId ${summary.jobId}"
      }
      return false
    }
    val message =
      """
      Your connection has been repeatedly failing and has been automatically disabled.
      
      """.trimIndent()
    return try {
      notifyJson(
        buildJobCompletedNotification(
          summary,
          "Connection disabled",
          legacyMessage,
          Optional.of(message),
          tag,
        ).toJsonNode(),
        summary.workspace.id,
      )
    } catch (e: IOException) {
      log.error(e) { "Error sending connection disabled Slack notification for workspaceId ${summary.workspace.id} jobId ${summary.jobId}" }
      false
    }
  }

  override fun notifyConnectionDisableWarning(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean {
    val legacyMessage: String
    try {
      legacyMessage =
        renderTemplate(
          "slack/auto_disable_warning_slack_notification_template.txt",
          summary.source.name,
          summary.destination.name,
          summary.errorMessage,
          summary.workspace.id.toString(),
          summary.connection.id.toString(),
        )
    } catch (e: IOException) {
      log.error(e) {
        "Error rendering slack notification template, connection disabled warning notification not sent for workspaceId ${summary.workspace.id} jobId ${summary.jobId}"
      }
      return false
    }
    val message =
      """
      Your connection has been repeatedly failing. Please address any issues to ensure your syncs continue to run.
      
      """.trimIndent()
    return try {
      notifyJson(
        buildJobCompletedNotification(
          summary,
          "Warning - repeated connection failures",
          legacyMessage,
          Optional.of(message),
          tag,
        ).toJsonNode(),
        summary.workspace.id,
      )
    } catch (e: IOException) {
      log.error(e) { "Error sending connection disabled warning Slack notification for workspaceId ${summary.workspace.id} jobId ${summary.jobId}" }
      false
    }
  }

  override fun notifyBreakingChangeWarning(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean {
    // Unsupported for now since we can't reliably send bulk Slack notifications
    throw UnsupportedOperationException("Slack notification is not supported for breaking change warning")
  }

  override fun notifyBreakingChangeSyncsDisabled(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean {
    // Unsupported for now since we can't reliably send bulk Slack notifications
    throw UnsupportedOperationException("Slack notification is not supported for breaking change syncs disabled notification")
  }

  override fun notifyBreakingUpcomingAutoUpgrade(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean = throw UnsupportedOperationException("Slack notification is not supported for breaking change auto upgrade pending")

  override fun notifyBreakingChangeSyncsUpgraded(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean = throw UnsupportedOperationException("Slack notification is not supported for breaking change syncs auto-upgraded notification")

  override fun notifySchemaPropagated(
    notification: SchemaUpdateNotification,
    recipient: String?,
    workspaceId: UUID?,
  ): Boolean {
    val summary = buildSummary(notification.catalogDiff)

    val tagContent = if (this.tag != null) String.format("[%s] ", this.tag) else ""
    val header =
      String.format(
        "%sThe schema of '%s' has changed.",
        tagContent,
        Notification.createLink(notification.connectionInfo.name, notification.connectionInfo.url),
      )
    val slackNotification =
      buildSchemaPropagationNotification(
        notification.workspace.name!!,
        notification.sourceInfo.name!!,
        summary,
        header,
        notification.workspace.url,
        notification.sourceInfo.url,
      )

    val webhookUrl = config.webhook
    if (!StringUtils.isEmpty(webhookUrl)) {
      return try {
        notifyJson(slackNotification.toJsonNode(), workspaceId)
      } catch (e: IOException) {
        log.error(e) { "Error sending schema propagation Slack notification for workspaceId $workspaceId" }
        false
      }
    }
    return false
  }

  override fun notifySchemaDiffToApply(
    notification: SchemaUpdateNotification,
    recipient: String?,
    workspaceId: UUID?,
  ): Boolean {
    log.info { "Sending slack notification to apply schema changes for workspaceId $workspaceId..." }
    val summary = buildSummary(notification.catalogDiff)
    // The following header and message are consistent with the email notification template.
    val header =
      String.format(
        "Airbyte detected schema changes for '%s'.",
        Notification.createLink(notification.connectionInfo.name, notification.connectionInfo.url),
      )
    val message =
      String.format(
        "The upstream schema of '%s' has changed. Please review and approve the changes to reflect them in your connection.",
        notification.connectionInfo.name,
      )

    val slackNotification =
      buildSchemaDiffToApplyNotification(
        notification.workspace.name!!,
        notification.sourceInfo.name!!,
        summary,
        header,
        message,
        notification.workspace.url,
        notification.sourceInfo.url,
      )

    val webhookUrl = config.webhook
    if (!StringUtils.isEmpty(webhookUrl)) {
      try {
        log.info { "Sending JSON..." }
        return notifyJson(slackNotification.toJsonNode(), workspaceId)
      } catch (e: IOException) {
        log.error(e) { "Error sending schema diff to apply Slack notification for workspaceId $workspaceId" }
        return false
      }
    }
    return false
  }

  override fun notifySchemaDiffToApplyWhenPropagationDisabled(
    notification: SchemaUpdateNotification,
    recipient: String?,
    workspaceId: UUID?,
  ): Boolean {
    log.info { "Sending slack notification to apply schema changes for workspaceId $workspaceId..." }
    val summary = buildSummary(notification.catalogDiff)
    // The following header and message are consistent with the email notification template.
    val header =
      String.format(
        "Airbyte detected schema changes for '%s'.",
        Notification.createLink(notification.connectionInfo.name, notification.connectionInfo.url),
      )
    val message =
      String.format(
        "The upstream schema of '%s' has changed. Airbyte has automatically disabled your connections according to your propagation setting. " +
          "Please review and approve the changes, then re-enable your connection to continue syncing.",
        notification.connectionInfo.name,
      )

    val slackNotification =
      buildSchemaDiffToApplyNotification(
        notification.workspace.name!!,
        notification.sourceInfo.name!!,
        summary,
        header,
        message,
        notification.workspace.url,
        notification.sourceInfo.url,
      )

    val webhookUrl = config.webhook
    if (!StringUtils.isEmpty(webhookUrl)) {
      try {
        log.info { "Sending JSON..." }
        return notifyJson(slackNotification.toJsonNode(), workspaceId)
      } catch (e: IOException) {
        log.error(e) { "Error sending schema diff to apply propagation disabled Slack notification for workspaceId $workspaceId" }
        return false
      }
    }
    return false
  }

  @Throws(IOException::class, InterruptedException::class)
  private fun notify(message: String): Boolean {
    val mapper = ObjectMapper()
    val node = mapper.createObjectNode()
    node.put("text", message)
    return notifyJson(node, null)
  }

  @Throws(IOException::class)
  private fun notifyJson(
    node: JsonNode,
    workspaceId: UUID?,
  ): Boolean {
    if (StringUtils.isEmpty(config.webhook)) {
      return false
    }
    val mapper = ObjectMapper()
    val httpClient =
      HttpClient
        .newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .build()
    val request =
      HttpRequest
        .newBuilder()
        .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(node)))
        .uri(URI.create(config.webhook))
        .header("Content-Type", "application/json")
        .build()
    val response: HttpResponse<String>
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    } catch (e: InterruptedException) {
      log.error(e) { "Error sending schema diff to apply Slack notification for workspaceId $workspaceId" }
      return false
    }
    if (isSuccessfulHttpResponse(response.statusCode())) {
      log.info { "Successful notification (${response.statusCode()}): {${response.body()}}" }
      return true
    } else {
      val errorMessage =
        String.format(
          "Failed to deliver notification workspaceId %s (%s): %s [%s]",
          workspaceId,
          response.statusCode(),
          response.body(),
          node.toString(),
        )
      throw IOException(errorMessage)
    }
  }

  override fun getNotificationClientType(): String = SLACK_CLIENT

  /**
   * Used when user tries to test the notification webhook settings on UI.
   */
  @Throws(IOException::class, InterruptedException::class)
  fun notifyTest(message: String): Boolean {
    val webhookUrl = config.webhook
    if (!StringUtils.isEmpty(webhookUrl)) {
      return notify(message)
    }
    return false
  }

  @Throws(IOException::class)
  fun renderTemplate(
    templateFile: String,
    vararg data: String?,
  ): String {
    val template = Resources.read(templateFile)
    return String.format(template, *data)
  }

  companion object {
    private const val SLACK_CLIENT = "slack"
    private const val MRKDOWN_TYPE_LABEL = "mrkdwn"

    fun buildJobCompletedNotification(
      summary: SyncSummary,
      titleText: String?,
      legacyText: String?,
      topContent: Optional<String>,
      tag: String?,
    ): Notification {
      val notification = Notification()
      notification.setText(legacyText)
      val title = notification.addSection()
      val connectionLink = Notification.createLink(summary.connection.name, summary.connection.url)
      val tagContent = if (tag != null) String.format("[%s] ", tag) else ""
      title.setText(String.format("%s%s: %s", tagContent, titleText, connectionLink))

      if (topContent.isPresent) {
        val topSection = notification.addSection()
        topSection.setText(topContent.get())
      }

      val description = notification.addSection()
      val sourceLabel = description.addField()

      sourceLabel.setType(MRKDOWN_TYPE_LABEL)
      sourceLabel.setText("*Source:*")
      val sourceValue = description.addField()
      sourceValue.setType(MRKDOWN_TYPE_LABEL)
      sourceValue.setText(Notification.createLink(summary.source.name, summary.source.url))

      val destinationLabel = description.addField()
      destinationLabel.setType(MRKDOWN_TYPE_LABEL)
      destinationLabel.setText("*Destination:*")
      val destinationValue = description.addField()
      destinationValue.setType(MRKDOWN_TYPE_LABEL)
      destinationValue.setText(Notification.createLink(summary.destination.name, summary.destination.url))

      if (summary.startedAt != null && summary.finishedAt != null) {
        val durationLabel = description.addField()
        durationLabel.setType(MRKDOWN_TYPE_LABEL)
        durationLabel.setText("*Duration:*")
        val durationValue = description.addField()
        durationValue.setType(MRKDOWN_TYPE_LABEL)
        durationValue.setText(summary.getDurationFormatted())
      }

      if (!summary.isSuccess && summary.errorMessage != null) {
        val failureSection = notification.addSection()
        failureSection.setText(
          String.format(
            """
            *Failure reason:*

            ```
            [%s] %s
            ```
            
            """.trimIndent(),
            summary.errorType,
            summary.errorMessage,
          ),
        )
      }
      val summarySection = notification.addSection()
      summarySection.setText(
        String.format(
          """
          *Sync Summary:*
          %d record(s) extracted / %d record(s) loaded
          %s extracted / %s loaded
          
          """.trimIndent(),
          summary.recordsEmitted,
          summary.recordsCommitted,
          summary.getBytesEmittedFormatted(),
          summary.getBytesCommittedFormatted(),
        ),
      )

      return notification
    }

    @JvmStatic
    @VisibleForTesting
    fun buildSummary(diff: CatalogDiff): String {
      val summaryBuilder = StringBuilder()

      val newStreams =
        diff.transforms
          .filter { it.transformType == StreamTransform.TransformTypeEnum.ADD_STREAM }
          .sortedBy { buildFullyQualifiedName(it.streamDescriptor) }

      val deletedStreams =
        diff.transforms
          .filter { it.transformType == StreamTransform.TransformTypeEnum.REMOVE_STREAM }
          .sortedBy { buildFullyQualifiedName(it.streamDescriptor) }

      val streamsWithPkChanges =
        diff.transforms
          .filter { it.transformType == StreamTransform.TransformTypeEnum.UPDATE_STREAM }
          .filter { t ->
            t.updateStream.streamAttributeTransforms.any { a ->
              a.transformType == StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY
            }
          }.sortedBy { buildFullyQualifiedName(it.streamDescriptor) }

      if (!newStreams.isEmpty() || !deletedStreams.isEmpty() || !streamsWithPkChanges.isEmpty()) {
        summaryBuilder.append(String.format(" • Streams (+%d/-%d/~%d)\n", newStreams.size, deletedStreams.size, streamsWithPkChanges.size))
        for (stream in newStreams) {
          val descriptor = stream.streamDescriptor
          val fullyQualifiedStreamName = buildFullyQualifiedName(descriptor)
          summaryBuilder.append(String.format("   ＋ %s\n", fullyQualifiedStreamName))
        }
        for (stream in deletedStreams) {
          val descriptor = stream.streamDescriptor
          val fullyQualifiedStreamName = buildFullyQualifiedName(descriptor)
          summaryBuilder.append(String.format("   － %s\n", fullyQualifiedStreamName))
        }
        for (stream in streamsWithPkChanges) {
          val descriptor = stream.streamDescriptor
          val fullyQualifiedStreamName = buildFullyQualifiedName(descriptor)
          val primaryKeyChange =
            stream.updateStream.streamAttributeTransforms
              .stream()
              .filter { t: StreamAttributeTransform -> t.transformType == StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY }
              .findFirst()
          if (primaryKeyChange.isPresent) {
            val oldPrimaryKeyString = formatPrimaryKeyString(primaryKeyChange.get().updatePrimaryKey.oldPrimaryKey)
            val newPrimaryKeyString = formatPrimaryKeyString(primaryKeyChange.get().updatePrimaryKey.newPrimaryKey)

            summaryBuilder.append(String.format("   ~ %s\n", fullyQualifiedStreamName))

            if (!oldPrimaryKeyString.isEmpty() && newPrimaryKeyString.isEmpty()) {
              summaryBuilder.append(String.format("     • %s removed as primary key\n", oldPrimaryKeyString))
            } else if (oldPrimaryKeyString.isEmpty() && !newPrimaryKeyString.isEmpty()) {
              summaryBuilder.append(String.format("     • %s added as primary key\n", newPrimaryKeyString))
            } else if (!oldPrimaryKeyString.isEmpty()) {
              summaryBuilder.append(String.format("     • Primary key changed (%s -> %s)\n", oldPrimaryKeyString, newPrimaryKeyString))
            }
          }
        }
      }

      val streamsWithFieldChanges =
        diff.transforms
          .filter { t ->
            t.transformType == StreamTransform.TransformTypeEnum.UPDATE_STREAM &&
              t.updateStream.streamAttributeTransforms.none { a ->
                a.transformType == StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY
              }
          }.sortedBy { buildFullyQualifiedName(it.streamDescriptor) }

      if (streamsWithFieldChanges.isNotEmpty()) {
        val newFieldCount =
          streamsWithFieldChanges
            .flatMap { it.updateStream.fieldTransforms }
            .count { it.transformType == FieldTransform.TransformTypeEnum.ADD_FIELD }

        val deletedFieldsCount =
          streamsWithFieldChanges
            .flatMap { it.updateStream.fieldTransforms }
            .count { it.transformType == FieldTransform.TransformTypeEnum.REMOVE_FIELD }

        val alteredFieldsCount =
          streamsWithFieldChanges
            .flatMap { it.updateStream.fieldTransforms }
            .count { it.transformType == FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA }

        summaryBuilder.append(String.format(" • Fields (+%d/~%d/-%d)\n", newFieldCount, alteredFieldsCount, deletedFieldsCount))

        for (stream in streamsWithFieldChanges) {
          val descriptor = stream.streamDescriptor
          val fullyQualifiedStreamName = buildFullyQualifiedName(descriptor)
          summaryBuilder.append(String.format("   • %s\n", fullyQualifiedStreamName))

          for (fieldChange in stream.updateStream.fieldTransforms
            .stream()
            .sorted { o1: FieldTransform, o2: FieldTransform ->
              if (o1.transformType == o2.transformType) {
                return@sorted buildFieldName(o1.fieldName)
                  .compareTo(buildFieldName(o2.fieldName))
              }
              if (o1.transformType == FieldTransform.TransformTypeEnum.ADD_FIELD ||
                (
                  o1.transformType == FieldTransform.TransformTypeEnum.REMOVE_FIELD &&
                    o2.transformType != FieldTransform.TransformTypeEnum.ADD_FIELD
                )
              ) {
                return@sorted -1
              }
              1
            }.toList()) {
            val fieldName = buildFieldName(fieldChange.fieldName)
            val operation =
              when (fieldChange.transformType) {
                FieldTransform.TransformTypeEnum.ADD_FIELD -> "＋"
                FieldTransform.TransformTypeEnum.REMOVE_FIELD -> "－"
                FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA -> "～"
                else -> "?"
              }
            summaryBuilder.append(String.format("     %s %s\n", operation, fieldName))
          }
        }
      }
      return summaryBuilder.toString()
    }

    fun buildSchemaPropagationNotification(
      workspaceName: String,
      sourceName: String,
      summary: String?,
      header: String?,
      workspaceUrl: String?,
      sourceUrl: String?,
    ): Notification {
      val slackNotification = Notification()
      slackNotification.setText(header)
      val titleSection = slackNotification.addSection()
      titleSection.setText(header)
      val section = slackNotification.addSection()
      var field = section.addField()
      field.setType(MRKDOWN_TYPE_LABEL)
      field.setText("*Workspace*")
      field = section.addField()
      field.setType(MRKDOWN_TYPE_LABEL)
      field.setText(Notification.createLink(workspaceName, workspaceUrl))
      field = section.addField()
      field.setType(MRKDOWN_TYPE_LABEL)
      field.setText("*Source*")
      field = section.addField()
      field.setType(MRKDOWN_TYPE_LABEL)
      field.setText(Notification.createLink(sourceName, sourceUrl))
      slackNotification.addDivider()
      val changeSection = slackNotification.addSection()
      changeSection.setText(summary)
      return slackNotification
    }

    fun buildSchemaDiffToApplyNotification(
      workspaceName: String,
      sourceName: String,
      summary: String?,
      header: String?,
      message: String?,
      workspaceUrl: String?,
      sourceUrl: String?,
    ): Notification {
      val slackNotification = Notification()
      slackNotification.setText(header)
      val titleSection = slackNotification.addSection()
      titleSection.setText(header)
      val actionSection = slackNotification.addSection()
      actionSection.setText(message)
      val section = slackNotification.addSection()
      var field = section.addField()
      field.setType(MRKDOWN_TYPE_LABEL)
      field.setText("*Workspace*")
      field = section.addField()
      field.setType(MRKDOWN_TYPE_LABEL)
      field.setText(Notification.createLink(workspaceName, workspaceUrl))
      field = section.addField()
      field.setType(MRKDOWN_TYPE_LABEL)
      field.setText("*Source*")
      field = section.addField()
      field.setType(MRKDOWN_TYPE_LABEL)
      field.setText(Notification.createLink(sourceName, sourceUrl))
      slackNotification.addDivider()
      val changeSection = slackNotification.addSection()
      changeSection.setText(summary)
      return slackNotification
    }

    /**
     * Use an integer division to check successful HTTP status codes (i.e., those from 200-299), not
     * just 200. https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
     */
    private fun isSuccessfulHttpResponse(httpStatusCode: Int): Boolean = httpStatusCode / 100 == 2
  }
}
