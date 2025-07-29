/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import com.fasterxml.jackson.annotation.JsonValue
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.temporal.exception.SizeLimitException
import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.FailureReason
import io.airbyte.config.Metadata
import io.airbyte.config.StreamDescriptor
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.workers.exception.ResourceConstraintException
import io.airbyte.workers.exception.WorkloadLauncherException
import io.airbyte.workers.exception.WorkloadMonitorException

// For limiting strings passed from connectors to the platform.
internal const val MAX_MSG_LENGTH: Int = 50000
internal const val MAX_STACK_TRACE_LENGTH: Int = 100000

// For limiting the number of failures to prevent Temporal serialization issues
const val MAX_FAILURES_TO_KEEP: Int = 10

private const val ATTRIBUTION_MESSAGE = "Remainder truncated by the Airbyte platform."
private const val JOB_ID_METADATA_KEY = "jobId"
private const val ATTEMPT_NUMBER_METADATA_KEY = "attemptNumber"
const val TRACE_MESSAGE_METADATA_KEY = "from_trace_message"
private const val CONNECTOR_COMMAND_METADATA_KEY = "connector_command"

/**
 * Failure helpers. Repository for all failures that happen in the worker.
 */
object FailureHelper {
  /**
   * Create generic failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  private fun genericFailure(
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
  ): FailureReason =
    FailureReason()
      .withInternalMessage(t.message)
      .withStacktrace(t.stackTraceToString())
      .withTimestamp(System.currentTimeMillis())
      .withMetadata(jobAndAttemptMetadata(jobId, attemptNumber))

  /**
   * Create generic failure.
   *
   * Generate a FailureReason from an AirbyteTraceMessage. The FailureReason.failureType enum value is
   * taken from the AirbyteErrorTraceMessage.failureType enum value, so the same enum value must exist
   * on both Enums in order to be applied correctly to the FailureReason
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   *
   * TODO: convert to internal once the tests have been migrated to kotlin
   */
  @JvmStatic
  fun genericFailure(
    m: AirbyteTraceMessage,
    jobId: Long?,
    attemptNumber: Int?,
  ): FailureReason? {
    var failureType: FailureReason.FailureType = FailureReason.FailureType.SYSTEM_ERROR
    if (m.error.failureType != null) {
      try {
        val traceMessageError: String? = m.error.failureType.toString()
        failureType = FailureReason.FailureType.fromValue(traceMessageError)
      } catch (_: IllegalArgumentException) {
        // the trace message error does not exist as a FailureReason failure type,
        // so set the failure type to null
        failureType = FailureReason.FailureType.SYSTEM_ERROR
      }
    }

    val streamDescriptor: StreamDescriptor? =
      m.error.streamDescriptor?.let {
        StreamDescriptor()
          .withNamespace(it.namespace)
          .withName(it.name)
      }

    // for strings coming from outside the platform (namely, from community connectors)
    // we want to ensure a max string length to keep payloads of reasonable size.
    val internalMsg = truncateWithPlatformMessage(m.error.internalMessage, MAX_MSG_LENGTH)
    val externalMsg = truncateWithPlatformMessage(m.error.message, MAX_MSG_LENGTH)
    val stackTrace = truncateWithPlatformMessage(m.error.stackTrace, MAX_STACK_TRACE_LENGTH)

    return FailureReason()
      .withInternalMessage(internalMsg)
      .withExternalMessage(externalMsg)
      .withStacktrace(stackTrace)
      .withTimestamp(m.getEmittedAt().toLong())
      .withStreamDescriptor(streamDescriptor)
      .withFailureType(failureType)
      .withMetadata(traceMessageMetadata(jobId, attemptNumber))
  }

  /**
   * Create connector command failure.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  @JvmStatic
  fun connectorCommandFailure(
    m: AirbyteTraceMessage,
    jobId: Long?,
    attemptNumber: Int?,
    connectorCommand: ConnectorCommand,
  ): FailureReason {
    val metadata =
      traceMessageMetadata(jobId, attemptNumber)
        .withAdditionalProperty(CONNECTOR_COMMAND_METADATA_KEY, connectorCommand.toString())

    return genericFailure(m, jobId, attemptNumber)!!
      .withMetadata(metadata)
  }

  /**
   * Create connector command failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  @JvmStatic
  fun connectorCommandFailure(
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
    connectorCommand: ConnectorCommand,
  ): FailureReason {
    val metadata =
      jobAndAttemptMetadata(jobId, attemptNumber)
        .withAdditionalProperty(CONNECTOR_COMMAND_METADATA_KEY, connectorCommand.toString())

    return genericFailure(t, jobId, attemptNumber)
      .withMetadata(metadata)
  }

  /**
   * Create source failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  @JvmStatic
  fun sourceFailure(
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
  ): FailureReason =
    connectorCommandFailure(t, jobId, attemptNumber, ConnectorCommand.READ)
      .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
      .withExternalMessage("Something went wrong within the source connector")

  /**
   * Create source failure.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  @JvmStatic
  fun sourceFailure(
    m: AirbyteTraceMessage,
    jobId: Long?,
    attemptNumber: Int?,
  ): FailureReason =
    connectorCommandFailure(m, jobId, attemptNumber, ConnectorCommand.READ)
      .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)

  /**
   * Create source heartbeat failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  fun sourceHeartbeatFailure(
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
    humanReadableThreshold: String?,
    timeBetweenLastRecord: String?,
  ): FailureReason {
    val errorMessage =
      "Airbyte detected that the Source didn't send any records in the last $timeBetweenLastRecord, " +
        "exceeding the configured $humanReadableThreshold threshold. Airbyte will try reading again on the next sync. " +
        "Please see https://docs.airbyte.com/understanding-airbyte/heartbeats for more info."

    return connectorCommandFailure(t, jobId, attemptNumber, ConnectorCommand.READ)
      .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
      .withFailureType(FailureReason.FailureType.HEARTBEAT_TIMEOUT)
      .withExternalMessage(errorMessage)
  }

  /**
   * Create destination timeout failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  fun destinationTimeoutFailure(
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
    humanReadableThreshold: String?,
    timeBetweenLastAction: String?,
  ): FailureReason {
    val errorMessage =
      "Airbyte detected that the Destination didn't make progress in the last $timeBetweenLastAction, " +
        "exceeding the configured $humanReadableThreshold threshold. Airbyte will try reading again on the next sync. " +
        "Please see https://docs.airbyte.com/understanding-airbyte/heartbeats for more info."

    return connectorCommandFailure(t, jobId, attemptNumber, ConnectorCommand.WRITE)
      .withFailureOrigin(FailureReason.FailureOrigin.DESTINATION)
      .withFailureType(FailureReason.FailureType.DESTINATION_TIMEOUT)
      .withExternalMessage(errorMessage)
  }

  /**
   * Create destination failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  @JvmStatic
  fun destinationFailure(
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
  ): FailureReason =
    connectorCommandFailure(t, jobId, attemptNumber, ConnectorCommand.WRITE)
      .withFailureOrigin(FailureReason.FailureOrigin.DESTINATION)
      .withExternalMessage("Something went wrong within the destination connector")

  /**
   * Create destination failure.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  @JvmStatic
  fun destinationFailure(
    m: AirbyteTraceMessage,
    jobId: Long?,
    attemptNumber: Int?,
  ): FailureReason =
    connectorCommandFailure(m, jobId, attemptNumber, ConnectorCommand.WRITE)
      .withFailureOrigin(FailureReason.FailureOrigin.DESTINATION)

  /**
   * Create check failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  @JvmStatic
  fun checkFailure(
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
    origin: FailureReason.FailureOrigin?,
  ): FailureReason? =
    connectorCommandFailure(t, jobId, attemptNumber, ConnectorCommand.CHECK)
      .withFailureOrigin(origin)
      .withFailureType(FailureReason.FailureType.CONFIG_ERROR)
      .withRetryable(false)
      .withExternalMessage(
        String.format(
          "Checking %s connection failed - please review this connection's configuration to prevent future syncs from failing",
          origin,
        ),
      )

  /**
   * Create replication failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  fun replicationFailure(
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
  ): FailureReason {
    val failure =
      genericFailure(t, jobId, attemptNumber)
        .withFailureOrigin(FailureReason.FailureOrigin.REPLICATION)
    if (isInstanceOf(t, ResourceConstraintException::class.java)) {
      return failure
        .withFailureType(FailureReason.FailureType.TRANSIENT_ERROR)
        .withExternalMessage(
          "Airbyte could not start the sync process." +
            " This may be due to insufficient system resources. Please check available resources and try again.",
        )
    }
    if (isInstanceOf(t, WorkloadLauncherException::class.java)) {
      return failure
        .withFailureType(FailureReason.FailureType.TRANSIENT_ERROR)
        .withExternalMessage("Airbyte could not start the sync process.")
    } else if (isInstanceOf(t, WorkloadMonitorException::class.java)) {
      return failure
        .withFailureType(FailureReason.FailureType.TRANSIENT_ERROR)
        .withExternalMessage("Airbyte could not start the sync process or track the progress of the sync.")
    } else {
      return failure.withExternalMessage("Something went wrong during replication")
    }
  }

  private fun isInstanceOf(
    exception: Throwable?,
    exceptionType: Class<out Throwable?>,
  ): Boolean {
    var current = exception
    while (current != null) {
      if (exceptionType.isInstance(exception)) {
        return true
      }
      current = current.cause
    }

    return exception?.message?.contains(exceptionType.name) == true
  }

  /**
   * Create unknown origin failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  @JvmStatic
  fun unknownOriginFailure(
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
  ): FailureReason =
    genericFailure(t, jobId, attemptNumber)
      .withFailureOrigin(FailureReason.FailureOrigin.UNKNOWN)
      .withExternalMessage("An unknown failure occurred")

  /**
   * Create failure summary from failures.
   *
   * @param failures failures
   * @param partialSuccess partial success
   * @return attempt failure summary
   */
  @JvmStatic
  fun failureSummary(
    failures: Set<FailureReason>,
    partialSuccess: Boolean?,
  ): AttemptFailureSummary? =
    AttemptFailureSummary()
      .withFailures(orderedFailures(failures))
      .withPartialSuccess(partialSuccess)

  /**
   * Create attempt failure summary for a cancellation.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @param failures failure reasons
   * @param partialSuccess partial success
   * @return attempt failure summary
   */
  @JvmStatic
  fun failureSummaryForCancellation(
    jobId: Long?,
    attemptNumber: Int?,
    failures: MutableSet<FailureReason>,
    partialSuccess: Boolean?,
  ): AttemptFailureSummary? {
    failures.add(
      FailureReason()
        .withFailureType(FailureReason.FailureType.MANUAL_CANCELLATION)
        .withInternalMessage("Setting attempt to FAILED because the job was cancelled")
        .withExternalMessage("This attempt was cancelled")
        .withTimestamp(System.currentTimeMillis())
        .withMetadata(jobAndAttemptMetadata(jobId, attemptNumber)),
    )

    return failureSummary(failures, partialSuccess)
  }

  /**
   * Create a failure reason based workflow type and activity type.
   *
   * @param workflowType workflow type
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  @JvmStatic
  fun failureReasonFromWorkflowAndActivity(
    workflowType: String,
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
  ): FailureReason {
    if (SyncWorkflow::class.java.getName().contains(workflowType)) {
      return replicationFailure(t, jobId, attemptNumber)
    } else {
      return unknownOriginFailure(t, jobId, attemptNumber)
    }
  }

  /**
   * Create generic platform failure.
   *
   * @param t throwable that cause the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  @JvmStatic
  fun platformFailure(
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
  ): FailureReason {
    val externalMessage =
      if (exceptionChainContains(t, SizeLimitException::class.java)) {
        "Size limit exceeded, please check your configuration, this is often related to a high number of fields."
      } else {
        "Something went wrong within the airbyte platform"
      }
    return genericFailure(t, jobId, attemptNumber)
      .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
      .withExternalMessage(externalMessage)
  }

  /**
   * Create generic platform failure.
   *
   * @param t throwable that cause the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  fun platformFailure(
    t: Throwable,
    jobId: Long?,
    attemptNumber: Int?,
    externalMessage: String?,
  ): FailureReason =
    genericFailure(t, jobId, attemptNumber)
      .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
      .withExternalMessage(externalMessage)

  /**
   * Orders failures by timestamp, so earlier failures come first.
   */
  @JvmStatic
  fun orderedFailures(failures: Collection<FailureReason>): List<FailureReason> = failures.sortedBy { it.timestamp }

  @VisibleForTesting
  @JvmStatic
  fun exceptionChainContains(
    t: Throwable?,
    type: Class<*>,
  ): Boolean {
    var tp = t
    while (tp != null) {
      if (type.isInstance(tp)) {
        return true
      }
      tp = tp.cause
    }
    return false
  }

  /**
   * Connector Commands.
   */
  enum class ConnectorCommand(
    private val value: String,
  ) {
    SPEC("spec"),
    CHECK("check"),
    DISCOVER("discover"),
    WRITE("write"),
    READ("read"),
    ;

    @JsonValue
    override fun toString(): String = value
  }
}

private fun jobAndAttemptMetadata(
  jobId: Long?,
  attemptNumber: Int?,
): Metadata =
  Metadata()
    .withAdditionalProperty(JOB_ID_METADATA_KEY, jobId)
    .withAdditionalProperty(ATTEMPT_NUMBER_METADATA_KEY, attemptNumber)

private fun traceMessageMetadata(
  jobId: Long?,
  attemptNumber: Int?,
): Metadata =
  Metadata()
    .withAdditionalProperty(JOB_ID_METADATA_KEY, jobId)
    .withAdditionalProperty(ATTEMPT_NUMBER_METADATA_KEY, attemptNumber)
    .withAdditionalProperty(TRACE_MESSAGE_METADATA_KEY, true)

private fun abbreviate(
  s: String?,
  length: Int,
): String? {
  if (s == null || s.length <= length) {
    return s
  }

  return s.substring(0, length - 3) + "..."
}

/**
 * Utility function for truncating strings with attribution to the platform for debugging purposes.
 * Factor out into separate class as necessary.
 *
 * TODO: should be private (once tests have been converted to kotlin
 */
fun truncateWithPlatformMessage(
  str: String?,
  maxWidth: Int,
): String? {
  if (str == null || str.length <= maxWidth) {
    return str
  }

  val maxWidthAdjusted = maxWidth - ATTRIBUTION_MESSAGE.length - 1
  // 4 is the min maxWidth of the Apache Lib we are using.
  val minMaxWidthApacheLib = 4
  if (maxWidthAdjusted < minMaxWidthApacheLib) {
    return str
  }

  return abbreviate(str, maxWidthAdjusted) + " " + ATTRIBUTION_MESSAGE
}
