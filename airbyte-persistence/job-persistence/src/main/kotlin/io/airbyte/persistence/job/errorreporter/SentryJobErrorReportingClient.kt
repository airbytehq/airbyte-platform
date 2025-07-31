/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardWorkspace
import io.sentry.Hub
import io.sentry.IHub
import io.sentry.NoOpHub
import io.sentry.SentryEvent
import io.sentry.SentryOptions
import io.sentry.protocol.Message
import io.sentry.protocol.SentryException
import io.sentry.protocol.User
import java.util.List

/**
 * Sentry implementation for job error reporting.
 */
class SentryJobErrorReportingClient internal constructor(
  private val sentryHub: IHub,
  private val exceptionHelper: SentryExceptionHelper,
) : JobErrorReportingClient {
  constructor(sentryDSN: String?, exceptionHelper: SentryExceptionHelper) : this(createSentryHubWithDSN(sentryDSN), exceptionHelper)

  /**
   * Reports a Connector Job FailureReason to Sentry.
   *
   * @param workspace - Workspace where this failure occurred
   * @param failureReason - FailureReason to report
   * @param dockerImage - Tagged docker image that represents the release where this failure occurred
   * @param metadata - Extra metadata to set as tags on the event
   */
  override fun reportJobFailureReason(
    workspace: StandardWorkspace?,
    failureReason: FailureReason,
    dockerImage: String?,
    metadata: Map<String?, String?>?,
    attemptConfig: AttemptConfigReportingContext?,
  ) {
    val event = SentryEvent()

    if (dockerImage != null) {
      // Remove invalid characters from the release name, use @ so sentry knows how to grab the tag
      // e.g. airbyte/source-xyz:1.2.0 -> airbyte-source-xyz@1.2.0
      // More info at https://docs.sentry.io/product/cli/releases/#creating-releases
      val release = dockerImage.replace("/", "-").replace(":", "@")
      event.release = release

      // enhance event fingerprint to ensure separate grouping per connector
      val releaseParts = release.split("@".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
      if (releaseParts.size > 0) {
        event.fingerprints = List.of("{{ default }}", releaseParts[0])
      }
    }

    // set workspace as the user in sentry to get impact and priority
    if (workspace != null) {
      val sentryUser = User()
      sentryUser.id = workspace.workspaceId.toString()
      if (workspace.name != null) {
        sentryUser.username = workspace.name
      }
      event.user = sentryUser
    }

    // set metadata as tags
    event.tags = metadata

    // set failure reason's internalMessage as event message
    // Sentry will use this to fuzzy-group if no stacktrace information is available
    val message = Message()
    message.formatted = failureReason.internalMessage
    event.message = message

    // events can come from any platform
    event.platform = "other"

    // attach failure reason stack trace
    val failureStackTrace = failureReason.stacktrace
    if (failureStackTrace != null && !failureStackTrace.isBlank()) {
      val optParsedException = exceptionHelper.buildSentryExceptions(failureStackTrace)
      if (optParsedException.isPresent) {
        val parsedException = optParsedException.get()
        val platform = parsedException.platform.value
        event.platform = platform
        event.setTag(STACKTRACE_PLATFORM_TAG_KEY, platform)
        event.exceptions = parsedException.exceptions
      } else {
        event.setTag(STACKTRACE_PARSE_ERROR_TAG_KEY, "1")

        // We couldn't parse the stacktrace, but we can still give it to Sentry for (less accurate) grouping
        val normalizedStacktrace =
          failureStackTrace
            .replace("\n", ", ")
            .replace(failureReason.internalMessage, "")

        val sentryException = SentryException()
        sentryException.value = normalizedStacktrace
        event.exceptions = List.of(sentryException)
      }
    }

    // Attach contexts to provide more debugging info
    val contexts = event.contexts

    val failureReasonContext: MutableMap<String, String> = HashMap()
    failureReasonContext["internalMessage"] = failureReason.internalMessage
    failureReasonContext["externalMessage"] = failureReason.externalMessage
    failureReasonContext["stacktrace"] = failureReason.stacktrace
    // This is not the same as the actual timestamp that Sentry will use for the timestamp.
    // This is just intended to track when the failure was created.
    // But some failures don't actually track their timestamp, so we need to handle nullness here.
    failureReason.timestamp?.let { failureReasonContext["timestamp"] = it.toString() }

    val failureReasonMeta = failureReason.metadata
    if (failureReasonMeta != null) {
      failureReasonContext["metadata"] = failureReasonMeta.toString()
    }

    contexts["Failure Reason"] = failureReasonContext

    if (attemptConfig != null) {
      val stateContext: MutableMap<String, String> = HashMap()
      stateContext["state"] = if (attemptConfig.state != null) attemptConfig.state.toString() else "null"
      contexts["State"] = stateContext
      contexts["Source Configuration"] = getContextFromNode(attemptConfig.sourceConfig)
      contexts["Destination Configuration"] = getContextFromNode(attemptConfig.destinationConfig)
    }

    // Send the event to sentry
    sentryHub.captureEvent(event)
  }

  companion object {
    const val STACKTRACE_PARSE_ERROR_TAG_KEY: String = "stacktrace_parse_error"
    const val STACKTRACE_PLATFORM_TAG_KEY: String = "stacktrace_platform"

    @JvmStatic
    fun createSentryHubWithDSN(sentryDSN: String?): IHub {
      if (sentryDSN == null || sentryDSN.isEmpty()) {
        return NoOpHub.getInstance()
      }

      val options = SentryOptions()
      options.dsn = sentryDSN
      options.isAttachStacktrace = false
      options.isEnableUncaughtExceptionHandler = false
      return Hub(options)
    }

    private fun getContextFromNode(node: JsonNode?): Map<String, String> {
      val flatMap: MutableMap<String, String> = HashMap()
      if (node != null) {
        flattenJsonNode("", node, flatMap)
      }
      return flatMap
    }

    /**
     * This flattens a JsonNode into its related dot paths.
     *
     * e.g. {"a": { "b": [{"c": 1}]}} -> {"a.b[0].c": 1}
     */
    @JvmStatic
    fun flattenJsonNode(
      currentPath: String,
      node: JsonNode,
      flatMap: MutableMap<String, String>,
    ) {
      if (node.isArray) {
        for (i in 0..<node.size()) {
          val item = node[i]
          val newPath = String.format("%s[%d]", currentPath, i)
          flattenJsonNode(newPath, item, flatMap)
        }
      } else if (node.isObject) {
        val fields = node.fields()
        while (fields.hasNext()) {
          val field = fields.next()
          val fieldName = field.key
          val fieldValue = field.value

          val newPath = if (currentPath.isEmpty()) fieldName else "$currentPath.$fieldName"
          flattenJsonNode(newPath, fieldValue, flatMap)
        }
      } else {
        flatMap[currentPath] = node.asText()
      }
    }
  }
}
