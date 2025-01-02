/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.temporal.exception.SizeLimitException;
import io.airbyte.commons.temporal.scheduling.SyncWorkflow;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.Metadata;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.workers.exception.ResourceConstraintException;
import io.airbyte.workers.exception.WorkloadLauncherException;
import io.airbyte.workers.exception.WorkloadMonitorException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Failure helpers. Repository for all failures that happen in the worker.
 */
public class FailureHelper {

  private static final String JOB_ID_METADATA_KEY = "jobId";
  private static final String ATTEMPT_NUMBER_METADATA_KEY = "attemptNumber";
  private static final String TRACE_MESSAGE_METADATA_KEY = "from_trace_message";
  private static final String CONNECTOR_COMMAND_METADATA_KEY = "connector_command";
  // For limiting strings passed from connectors to the platform.
  private static final String ATTRIBUTION_MESSAGE = "Remainder truncated by the Airbyte platform.";
  static final int MAX_MSG_LENGTH = 50000;
  static final int MAX_STACK_TRACE_LENGTH = 100000;

  /**
   * Connector Commands.
   */
  public enum ConnectorCommand {

    SPEC("spec"),
    CHECK("check"),
    DISCOVER("discover"),
    WRITE("write"),
    READ("read");

    private final String value;

    ConnectorCommand(final String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return String.valueOf(value);
    }

  }

  /**
   * Create generic failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason genericFailure(final Throwable t, final Long jobId, final Integer attemptNumber) {
    return new FailureReason()
        .withInternalMessage(t.getMessage())
        .withStacktrace(ExceptionUtils.getStackTrace(t))
        .withTimestamp(System.currentTimeMillis())
        .withMetadata(jobAndAttemptMetadata(jobId, attemptNumber));
  }

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
   */
  public static FailureReason genericFailure(final AirbyteTraceMessage m, final Long jobId, final Integer attemptNumber) {
    FailureType failureType;
    if (m.getError().getFailureType() == null) {
      // default to system_error when no failure type is set
      failureType = FailureType.SYSTEM_ERROR;
    } else {
      try {
        final String traceMessageError = m.getError().getFailureType().toString();
        failureType = FailureReason.FailureType.fromValue(traceMessageError);
      } catch (final IllegalArgumentException e) {
        // the trace message error does not exist as a FailureReason failure type,
        // so set the failure type to null
        failureType = FailureType.SYSTEM_ERROR;
      }
    }
    StreamDescriptor streamDescriptor = null;
    if (m.getError().getStreamDescriptor() != null) {
      streamDescriptor = new StreamDescriptor()
          .withNamespace(m.getError().getStreamDescriptor().getNamespace())
          .withName(m.getError().getStreamDescriptor().getName());
    }

    // for strings coming from outside the platform (namely, from community connectors)
    // we want to ensure a max string length to keep payloads of reasonable size.
    final var internalMsg = truncateWithPlatformMessage(m.getError().getInternalMessage(), MAX_MSG_LENGTH);
    final var externalMsg = truncateWithPlatformMessage(m.getError().getMessage(), MAX_MSG_LENGTH);
    final var stackTrace = truncateWithPlatformMessage(m.getError().getStackTrace(), MAX_STACK_TRACE_LENGTH);

    return new FailureReason()
        .withInternalMessage(internalMsg)
        .withExternalMessage(externalMsg)
        .withStacktrace(stackTrace)
        .withTimestamp(m.getEmittedAt().longValue())
        .withStreamDescriptor(streamDescriptor)
        .withFailureType(failureType)
        .withMetadata(traceMessageMetadata(jobId, attemptNumber));
  }

  /**
   * Create connector command failure.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason connectorCommandFailure(final AirbyteTraceMessage m,
                                                      final Long jobId,
                                                      final Integer attemptNumber,
                                                      final ConnectorCommand connectorCommand) {
    final Metadata metadata = traceMessageMetadata(jobId, attemptNumber);
    metadata.withAdditionalProperty(CONNECTOR_COMMAND_METADATA_KEY, connectorCommand.toString());
    return genericFailure(m, jobId, attemptNumber)
        .withMetadata(metadata);
  }

  /**
   * Create connector command failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason connectorCommandFailure(final Throwable t,
                                                      final Long jobId,
                                                      final Integer attemptNumber,
                                                      final ConnectorCommand connectorCommand) {
    final Metadata metadata = jobAndAttemptMetadata(jobId, attemptNumber);
    metadata.withAdditionalProperty(CONNECTOR_COMMAND_METADATA_KEY, connectorCommand.toString());
    return genericFailure(t, jobId, attemptNumber)
        .withMetadata(metadata);
  }

  /**
   * Create source failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason sourceFailure(final Throwable t, final Long jobId, final Integer attemptNumber) {
    return connectorCommandFailure(t, jobId, attemptNumber, ConnectorCommand.READ)
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withExternalMessage("Something went wrong within the source connector");
  }

  /**
   * Create source failure.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason sourceFailure(final AirbyteTraceMessage m, final Long jobId, final Integer attemptNumber) {
    return connectorCommandFailure(m, jobId, attemptNumber, ConnectorCommand.READ)
        .withFailureOrigin(FailureOrigin.SOURCE);
  }

  /**
   * Create source heartbeat failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason sourceHeartbeatFailure(final Throwable t,
                                                     final Long jobId,
                                                     final Integer attemptNumber,
                                                     final String humanReadableThreshold,
                                                     final String timeBetweenLastRecord) {
    final var errorMessage = String.format(
        "Airbyte detected that the Source didn't send any records in the last %s, exceeding the configured %s threshold. Airbyte will try reading again on the next sync. Please see https://docs.airbyte.com/understanding-airbyte/heartbeats for more info.",
        timeBetweenLastRecord, humanReadableThreshold);
    return connectorCommandFailure(t, jobId, attemptNumber, ConnectorCommand.READ)
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.HEARTBEAT_TIMEOUT)
        .withExternalMessage(errorMessage);
  }

  /**
   * Create destination timeout failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason destinationTimeoutFailure(final Throwable t,
                                                        final Long jobId,
                                                        final Integer attemptNumber,
                                                        final String humanReadableThreshold,
                                                        final String timeBetweenLastAction) {
    final var errorMessage = String.format(
        "Airbyte detected that the Destination didn't make progress in the last %s, exceeding the configured %s threshold. Airbyte will try reading again on the next sync. Please see https://docs.airbyte.com/understanding-airbyte/heartbeats for more info.",
        timeBetweenLastAction, humanReadableThreshold);
    return connectorCommandFailure(t, jobId, attemptNumber, ConnectorCommand.WRITE)
        .withFailureOrigin(FailureOrigin.DESTINATION)
        .withFailureType(FailureType.DESTINATION_TIMEOUT)
        .withExternalMessage(errorMessage);
  }

  /**
   * Create destination failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason destinationFailure(final Throwable t, final Long jobId, final Integer attemptNumber) {
    return connectorCommandFailure(t, jobId, attemptNumber, ConnectorCommand.WRITE)
        .withFailureOrigin(FailureOrigin.DESTINATION)
        .withExternalMessage("Something went wrong within the destination connector");
  }

  /**
   * Create destination failure.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason destinationFailure(final AirbyteTraceMessage m, final Long jobId, final Integer attemptNumber) {
    return connectorCommandFailure(m, jobId, attemptNumber, ConnectorCommand.WRITE)
        .withFailureOrigin(FailureOrigin.DESTINATION);
  }

  /**
   * Create check failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason checkFailure(final Throwable t,
                                           final Long jobId,
                                           final Integer attemptNumber,
                                           final FailureReason.FailureOrigin origin) {
    return connectorCommandFailure(t, jobId, attemptNumber, ConnectorCommand.CHECK)
        .withFailureOrigin(origin)
        .withFailureType(FailureReason.FailureType.CONFIG_ERROR)
        .withRetryable(false)
        .withExternalMessage(String
            .format("Checking %s connection failed - please review this connection's configuration to prevent future syncs from failing", origin));
  }

  /**
   * Create replication failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason replicationFailure(final Throwable t, final Long jobId, final Integer attemptNumber) {
    final FailureReason failure = genericFailure(t, jobId, attemptNumber)
        .withFailureOrigin(FailureOrigin.REPLICATION);
    if (isInstanceOf(t, ResourceConstraintException.class)) {
      return failure.withFailureType(FailureType.TRANSIENT_ERROR)
          .withExternalMessage("Airbyte could not start the sync process."
              + " This may be due to insufficient system resources. Please check available resources and try again.");
    }
    if (isInstanceOf(t, WorkloadLauncherException.class)) {
      return failure.withFailureType(FailureType.TRANSIENT_ERROR)
          .withExternalMessage("Airbyte could not start the sync process.");
    } else if (isInstanceOf(t, WorkloadMonitorException.class)) {
      return failure.withFailureType(FailureType.TRANSIENT_ERROR)
          .withExternalMessage("Airbyte could not start the sync process or track the progress of the sync.");
    } else {
      return failure.withExternalMessage("Something went wrong during replication");
    }
  }

  private static boolean isInstanceOf(final Throwable exception, final Class<? extends Throwable> exceptionType) {
    Throwable current = exception;
    while (current != null) {
      if (exceptionType.isInstance(exception)) {
        return true;
      }
      current = current.getCause();
    }

    return Objects.nonNull(exception) && Objects.nonNull(exception.getMessage()) && exception.getMessage().contains(exceptionType.getName());
  }

  /**
   * Create unknown origin failure.
   *
   * @param t throwable that caused the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason unknownOriginFailure(final Throwable t, final Long jobId, final Integer attemptNumber) {
    return genericFailure(t, jobId, attemptNumber)
        .withFailureOrigin(FailureOrigin.UNKNOWN)
        .withExternalMessage("An unknown failure occurred");
  }

  /**
   * Create failure summary from failures.
   *
   * @param failures failures
   * @param partialSuccess partial success
   * @return attempt failure summary
   */
  public static AttemptFailureSummary failureSummary(final Set<FailureReason> failures, final Boolean partialSuccess) {
    return new AttemptFailureSummary()
        .withFailures(orderedFailures(failures))
        .withPartialSuccess(partialSuccess);
  }

  /**
   * Create attempt failure summary for a cancellation.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @param failures failure reasons
   * @param partialSuccess partial success
   * @return attempt failure summary
   */
  public static AttemptFailureSummary failureSummaryForCancellation(final Long jobId,
                                                                    final Integer attemptNumber,
                                                                    final Set<FailureReason> failures,
                                                                    final Boolean partialSuccess) {
    failures.add(new FailureReason()
        .withFailureType(FailureType.MANUAL_CANCELLATION)
        .withInternalMessage("Setting attempt to FAILED because the job was cancelled")
        .withExternalMessage("This attempt was cancelled")
        .withTimestamp(System.currentTimeMillis())
        .withMetadata(jobAndAttemptMetadata(jobId, attemptNumber)));

    return failureSummary(failures, partialSuccess);
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
  public static FailureReason failureReasonFromWorkflowAndActivity(final String workflowType,
                                                                   final Throwable t,
                                                                   final Long jobId,
                                                                   final Integer attemptNumber) {
    if (SyncWorkflow.class.getName().contains(workflowType)) {
      return replicationFailure(t, jobId, attemptNumber);
    } else {
      return unknownOriginFailure(t, jobId, attemptNumber);
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
  public static FailureReason platformFailure(final Throwable t, final Long jobId, final Integer attemptNumber) {
    final String externalMessage =
        exceptionChainContains(t, SizeLimitException.class)
            ? "Size limit exceeded, please check your configuration, this is often related to a high number of fields."
            : "Something went wrong within the airbyte platform";
    return genericFailure(t, jobId, attemptNumber)
        .withFailureOrigin(FailureOrigin.AIRBYTE_PLATFORM)
        .withExternalMessage(externalMessage);
  }

  /**
   * Create generic platform failure.
   *
   * @param t throwable that cause the failure
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return failure reason
   */
  public static FailureReason platformFailure(final Throwable t, final Long jobId, final Integer attemptNumber, final String externalMessage) {
    return genericFailure(t, jobId, attemptNumber)
        .withFailureOrigin(FailureOrigin.AIRBYTE_PLATFORM)
        .withExternalMessage(externalMessage);
  }

  private static Metadata jobAndAttemptMetadata(final Long jobId, final Integer attemptNumber) {
    return new Metadata()
        .withAdditionalProperty(JOB_ID_METADATA_KEY, jobId)
        .withAdditionalProperty(ATTEMPT_NUMBER_METADATA_KEY, attemptNumber);
  }

  private static Metadata traceMessageMetadata(final Long jobId, final Integer attemptNumber) {
    return new Metadata()
        .withAdditionalProperty(JOB_ID_METADATA_KEY, jobId)
        .withAdditionalProperty(ATTEMPT_NUMBER_METADATA_KEY, attemptNumber)
        .withAdditionalProperty(TRACE_MESSAGE_METADATA_KEY, true);
  }

  /**
   * Orders failures by putting errors from trace messages first, and then orders by timestamp, so
   * that earlier failures come first.
   */
  public static List<FailureReason> orderedFailures(final Set<FailureReason> failures) {
    final Comparator<FailureReason> compareByIsTrace = Comparator.comparing(failureReason -> {
      final Object metadata = failureReason.getMetadata();
      if (metadata != null) {
        return failureReason.getMetadata().getAdditionalProperties().containsKey(TRACE_MESSAGE_METADATA_KEY) ? 0 : 1;
      } else {
        return 1;
      }
    });
    final Comparator<FailureReason> compareByTimestamp = Comparator.comparing(FailureReason::getTimestamp);
    final Comparator<FailureReason> compareByTraceAndTimestamp = compareByIsTrace.thenComparing(compareByTimestamp);
    return failures.stream().sorted(compareByTraceAndTimestamp).toList();
  }

  @VisibleForTesting
  static boolean exceptionChainContains(final Throwable t, Class<?> type) {
    Throwable tp = t;
    while (tp != null) {
      if (type.isInstance(tp)) {
        return true;
      }
      tp = tp.getCause();
    }
    return false;
  }

  /**
   * Utility function for truncating strings with attribution to the platform for debugging purposes.
   * Factor out into separate class as necessary.
   */
  @VisibleForTesting
  static String truncateWithPlatformMessage(final String str, final int maxWidth) {
    if (str == null || str.length() <= maxWidth) {
      return str;
    }

    final var maxWidthAdjusted = maxWidth - ATTRIBUTION_MESSAGE.length() - 1;
    // 4 is the min maxWidth of the Apache Lib we are using.
    final var minMaxWidthApacheLib = 4;
    if (maxWidthAdjusted < minMaxWidthApacheLib) {
      return str;
    }

    return StringUtils.abbreviate(str, maxWidthAdjusted) + " " + ATTRIBUTION_MESSAGE;
  }

}
