/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.commons.temporal.exception.SizeLimitException;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.Metadata;
import io.airbyte.protocol.models.AirbyteErrorTraceMessage;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.helper.FailureHelper.ConnectorCommand;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import io.temporal.api.enums.v1.RetryState;
import io.temporal.failure.ActivityFailure;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FailureHelperTest {

  private static final String FROM_TRACE_MESSAGE_KEY = "from_trace_message";
  private static final String CONNECTOR_COMMAND_KEY = "connector_command";
  private static final String JOB_ID_KEY = "jobId";
  private static final String ATTEMPT_NUMBER_KEY = "attemptNumber";

  private static final FailureReason TRACE_FAILURE_REASON = new FailureReason()
      .withInternalMessage("internal message")
      .withStacktrace("stack trace")
      .withTimestamp(Long.valueOf(1111112))
      .withMetadata(new Metadata()
          .withAdditionalProperty(JOB_ID_KEY, 12345)
          .withAdditionalProperty(ATTEMPT_NUMBER_KEY, 1)
          .withAdditionalProperty(FROM_TRACE_MESSAGE_KEY, true));

  private static final FailureReason TRACE_FAILURE_REASON_2 = new FailureReason()
      .withInternalMessage("internal message")
      .withStacktrace("stack trace")
      .withTimestamp(Long.valueOf(1111113))
      .withMetadata(new Metadata()
          .withAdditionalProperty(JOB_ID_KEY, 12345)
          .withAdditionalProperty(ATTEMPT_NUMBER_KEY, 1)
          .withAdditionalProperty(FROM_TRACE_MESSAGE_KEY, true));

  private static final FailureReason EXCEPTION_FAILURE_REASON = new FailureReason()
      .withInternalMessage("internal message")
      .withStacktrace("stack trace")
      .withTimestamp(Long.valueOf(1111111))
      .withMetadata(new Metadata()
          .withAdditionalProperty(JOB_ID_KEY, 12345)
          .withAdditionalProperty(ATTEMPT_NUMBER_KEY, 1));

  private static final AirbyteTraceMessage TRACE_MESSAGE = AirbyteMessageUtils.createErrorTraceMessage(
      "trace message error",
      Double.valueOf(123),
      AirbyteErrorTraceMessage.FailureType.SYSTEM_ERROR);

  @Test
  void testGenericFailureFromTrace() throws Exception {
    final AirbyteTraceMessage traceMessage = AirbyteMessageUtils.createErrorTraceMessage("trace message error", Double.valueOf(123),
        AirbyteErrorTraceMessage.FailureType.CONFIG_ERROR);
    final FailureReason failureReason = FailureHelper.genericFailure(traceMessage, Long.valueOf(12345), 1);
    assertEquals(FailureType.CONFIG_ERROR, failureReason.getFailureType());
  }

  @Test
  void testFailureWithTransientFailureType() {
    final AirbyteTraceMessage traceMessage =
        AirbyteMessageUtils.createErrorTraceMessage("sample trace message", 10.0, AirbyteErrorTraceMessage.FailureType.TRANSIENT_ERROR);
    final FailureReason reason = FailureHelper.genericFailure(traceMessage, 1034L, 0);
    assertEquals(FailureType.TRANSIENT_ERROR, reason.getFailureType());
  }

  @Test
  void testGenericFailureFromTraceNoFailureType() throws Exception {
    final FailureReason failureReason = FailureHelper.genericFailure(TRACE_MESSAGE, Long.valueOf(12345), 1);
    assertEquals(failureReason.getFailureType(), FailureType.SYSTEM_ERROR);
  }

  @Test
  void testConnectorCommandFailure() {
    final Throwable t = new RuntimeException();
    final Long jobId = 12345L;
    final Integer attemptNumber = 1;
    final FailureReason failureReason = FailureHelper.connectorCommandFailure(t, jobId, attemptNumber, ConnectorCommand.CHECK);

    final Map<String, Object> metadata = failureReason.getMetadata().getAdditionalProperties();
    assertEquals("check", metadata.get(CONNECTOR_COMMAND_KEY));
    assertNull(metadata.get(FROM_TRACE_MESSAGE_KEY));
    assertEquals(jobId, metadata.get(JOB_ID_KEY));
    assertEquals(attemptNumber, metadata.get(ATTEMPT_NUMBER_KEY));
  }

  @Test
  void testConnectorCommandFailureFromTrace() {
    final Long jobId = 12345L;
    final Integer attemptNumber = 1;
    final FailureReason failureReason = FailureHelper.connectorCommandFailure(TRACE_MESSAGE, jobId, attemptNumber, ConnectorCommand.DISCOVER);

    final Map<String, Object> metadata = failureReason.getMetadata().getAdditionalProperties();
    assertEquals("discover", metadata.get(CONNECTOR_COMMAND_KEY));
    assertEquals(true, metadata.get(FROM_TRACE_MESSAGE_KEY));
    assertEquals(jobId, metadata.get(JOB_ID_KEY));
    assertEquals(attemptNumber, metadata.get(ATTEMPT_NUMBER_KEY));
  }

  @Test
  void testSourceFailure() {
    final Throwable t = new RuntimeException();
    final Long jobId = 12345L;
    final Integer attemptNumber = 1;
    final FailureReason failureReason = FailureHelper.sourceFailure(t, jobId, attemptNumber);
    assertEquals(FailureOrigin.SOURCE, failureReason.getFailureOrigin());

    final Map<String, Object> metadata = failureReason.getMetadata().getAdditionalProperties();
    assertEquals("read", metadata.get(CONNECTOR_COMMAND_KEY));
    assertNull(metadata.get(FROM_TRACE_MESSAGE_KEY));
    assertEquals(jobId, metadata.get(JOB_ID_KEY));
    assertEquals(attemptNumber, metadata.get(ATTEMPT_NUMBER_KEY));
  }

  @Test
  void testSourceHeartbeatFailure() {
    final Throwable t = new RuntimeException();
    final Long jobId = 12345L;
    final Integer attemptNumber = 1;
    final FailureReason failureReason = FailureHelper.sourceFailure(t, jobId, attemptNumber);
    assertEquals(FailureOrigin.SOURCE, failureReason.getFailureOrigin());
    // assertEquals(FailureType.);

    final Map<String, Object> metadata = failureReason.getMetadata().getAdditionalProperties();
    assertEquals("read", metadata.get(CONNECTOR_COMMAND_KEY));
    assertNull(metadata.get(FROM_TRACE_MESSAGE_KEY));
    assertEquals(jobId, metadata.get(JOB_ID_KEY));
    assertEquals(attemptNumber, metadata.get(ATTEMPT_NUMBER_KEY));
  }

  @Test
  void testSourceFailureFromTrace() {
    final Long jobId = 12345L;
    final Integer attemptNumber = 1;
    final FailureReason failureReason = FailureHelper.sourceFailure(TRACE_MESSAGE, jobId, attemptNumber);
    assertEquals(FailureOrigin.SOURCE, failureReason.getFailureOrigin());

    final Map<String, Object> metadata = failureReason.getMetadata().getAdditionalProperties();
    assertEquals("read", metadata.get(CONNECTOR_COMMAND_KEY));
    assertEquals(true, metadata.get(FROM_TRACE_MESSAGE_KEY));
    assertEquals(jobId, metadata.get(JOB_ID_KEY));
    assertEquals(attemptNumber, metadata.get(ATTEMPT_NUMBER_KEY));
  }

  @Test
  void testDestinationFailure() {
    final Throwable t = new RuntimeException();
    final Long jobId = 12345L;
    final Integer attemptNumber = 1;
    final FailureReason failureReason = FailureHelper.destinationFailure(t, jobId, attemptNumber);
    assertEquals(FailureOrigin.DESTINATION, failureReason.getFailureOrigin());

    final Map<String, Object> metadata = failureReason.getMetadata().getAdditionalProperties();
    assertEquals("write", metadata.get(CONNECTOR_COMMAND_KEY));
    assertNull(metadata.get(FROM_TRACE_MESSAGE_KEY));
    assertEquals(jobId, metadata.get(JOB_ID_KEY));
    assertEquals(attemptNumber, metadata.get(ATTEMPT_NUMBER_KEY));
  }

  @Test
  void testDestinationFailureFromTrace() {
    final Long jobId = 12345L;
    final Integer attemptNumber = 1;
    final FailureReason failureReason = FailureHelper.destinationFailure(TRACE_MESSAGE, jobId, attemptNumber);
    assertEquals(FailureOrigin.DESTINATION, failureReason.getFailureOrigin());

    final Map<String, Object> metadata = failureReason.getMetadata().getAdditionalProperties();
    assertEquals("write", metadata.get(CONNECTOR_COMMAND_KEY));
    assertEquals(true, metadata.get(FROM_TRACE_MESSAGE_KEY));
    assertEquals(jobId, metadata.get(JOB_ID_KEY));
    assertEquals(attemptNumber, metadata.get(ATTEMPT_NUMBER_KEY));
  }

  @Test
  void testCheckFailure() {
    final Throwable t = new RuntimeException();
    final Long jobId = 12345L;
    final Integer attemptNumber = 1;
    final FailureReason failureReason = FailureHelper.checkFailure(t, jobId, attemptNumber, FailureOrigin.DESTINATION);
    assertEquals(FailureOrigin.DESTINATION, failureReason.getFailureOrigin());

    final Map<String, Object> metadata = failureReason.getMetadata().getAdditionalProperties();
    assertEquals("check", metadata.get(CONNECTOR_COMMAND_KEY));
    assertNull(metadata.get(FROM_TRACE_MESSAGE_KEY));
    assertEquals(jobId, metadata.get(JOB_ID_KEY));
    assertEquals(attemptNumber, metadata.get(ATTEMPT_NUMBER_KEY));
  }

  @Test
  void testOrderedFailures() throws Exception {
    final List<FailureReason> failureReasonList =
        FailureHelper.orderedFailures(Set.of(TRACE_FAILURE_REASON_2, TRACE_FAILURE_REASON, EXCEPTION_FAILURE_REASON));
    assertEquals(failureReasonList.get(0), TRACE_FAILURE_REASON);
  }

  @Test
  void testUnknownOriginFailure() {
    final Throwable t = new RuntimeException();
    final Long jobId = 12345L;
    final Integer attemptNumber = 1;
    final FailureReason failureReason = FailureHelper.unknownOriginFailure(t, jobId, attemptNumber);
    assertEquals(FailureOrigin.UNKNOWN, failureReason.getFailureOrigin());
    assertEquals("An unknown failure occurred", failureReason.getExternalMessage());
  }

  @Test
  void testExceptionChainContains() {
    final Throwable t =
        new ActivityFailure("", 1L, 2L, "act", "actId", RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE, "id",
            new RuntimeException("oops",
                new SizeLimitException("oops too big")));
    assertTrue(FailureHelper.exceptionChainContains(t, ActivityFailure.class));
    assertTrue(FailureHelper.exceptionChainContains(t, SizeLimitException.class));
    assertFalse(FailureHelper.exceptionChainContains(t, WorkerException.class));
  }

}
