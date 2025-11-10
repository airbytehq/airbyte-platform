/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import io.airbyte.commons.temporal.exception.SizeLimitException
import io.airbyte.config.FailureReason
import io.airbyte.config.Metadata
import io.airbyte.protocol.models.v0.AirbyteErrorTraceMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.helper.FailureHelper.checkFailure
import io.airbyte.workers.helper.FailureHelper.connectorCommandFailure
import io.airbyte.workers.helper.FailureHelper.destinationFailure
import io.airbyte.workers.helper.FailureHelper.exceptionChainContains
import io.airbyte.workers.helper.FailureHelper.genericFailure
import io.airbyte.workers.helper.FailureHelper.orderedFailures
import io.airbyte.workers.helper.FailureHelper.sourceFailure
import io.airbyte.workers.helper.FailureHelper.unknownOriginFailure
import io.airbyte.workers.testutils.AirbyteMessageUtils.createErrorTraceMessage
import io.temporal.api.enums.v1.RetryState
import io.temporal.failure.ActivityFailure
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class FailureHelperTest {
  @Test
  fun testGenericFailureFromTrace() {
    val traceMessage =
      createErrorTraceMessage(
        "trace message error",
        123.0,
        AirbyteErrorTraceMessage.FailureType.CONFIG_ERROR,
      )
    val failureReason = genericFailure(traceMessage, 12345, 1)
    Assertions.assertEquals(FailureReason.FailureType.CONFIG_ERROR, failureReason!!.failureType)
  }

  @Test
  fun testFailureWithTransientFailureType() {
    val traceMessage =
      createErrorTraceMessage("sample trace message", 10.0, AirbyteErrorTraceMessage.FailureType.TRANSIENT_ERROR)
    val reason = genericFailure(traceMessage, 1034L, 0)
    Assertions.assertEquals(FailureReason.FailureType.TRANSIENT_ERROR, reason!!.failureType)
  }

  @Test
  fun testGenericFailureFromTraceNoFailureType() {
    val failureReason = genericFailure(TRACE_MESSAGE, 12345, 1)
    Assertions.assertEquals(failureReason!!.failureType, FailureReason.FailureType.SYSTEM_ERROR)
  }

  @Test
  fun genericFailureFromTraceTruncatesStrings() {
    val oneMbString = String.format("%1048576s", "")

    val traceMsg =
      AirbyteTraceMessage()
        .withType(AirbyteTraceMessage.Type.ERROR)
        .withError(
          AirbyteErrorTraceMessage()
            .withFailureType(AirbyteErrorTraceMessage.FailureType.CONFIG_ERROR)
            .withMessage(oneMbString)
            .withInternalMessage(oneMbString)
            .withStackTrace(oneMbString),
        ).withEmittedAt(123.0)

    val result = genericFailure(traceMsg, 667, 0)

    Assertions.assertTrue(oneMbString.length > MAX_MSG_LENGTH)
    Assertions.assertEquals(MAX_MSG_LENGTH, result!!.externalMessage.length)
    Assertions.assertEquals(MAX_MSG_LENGTH, result.internalMessage.length)
    Assertions.assertEquals(MAX_STACK_TRACE_LENGTH, result.stacktrace.length)
  }

  @Test
  fun testExtractStreamDescriptor() {
    val name = "users"
    val namespace = "public"
    val traceMessage =
      createErrorTraceMessage("a error with a stream", 80.0, AirbyteErrorTraceMessage.FailureType.SYSTEM_ERROR)
    traceMessage.error.streamDescriptor = StreamDescriptor().withName(name).withNamespace(namespace)
    val failureReason = genericFailure(traceMessage, 1934L, 0)
    Assertions.assertNotNull(failureReason!!.streamDescriptor)
    Assertions.assertEquals(failureReason.streamDescriptor.name, name)
    Assertions.assertEquals(failureReason.streamDescriptor.namespace, namespace)
  }

  @Test
  fun testExtractStreamDescriptorNoNamespace() {
    val name = "users"
    val traceMessage =
      createErrorTraceMessage("a error with a stream", 80.0, AirbyteErrorTraceMessage.FailureType.SYSTEM_ERROR)
    traceMessage.error.streamDescriptor = StreamDescriptor().withName(name)
    val failureReason = genericFailure(traceMessage, 1934L, 0)
    Assertions.assertNotNull(failureReason!!.streamDescriptor)
    Assertions.assertEquals(failureReason.streamDescriptor.name, name)
    Assertions.assertNull(failureReason.streamDescriptor.namespace)
  }

  @Test
  fun testConnectorCommandFailure() {
    val t: Throwable = RuntimeException()
    val jobId = 12345L
    val attemptNumber = 1
    val failureReason = connectorCommandFailure(t, jobId, attemptNumber, FailureHelper.ConnectorCommand.CHECK)

    val metadata = failureReason.metadata.additionalProperties
    Assertions.assertEquals("check", metadata[CONNECTOR_COMMAND_KEY])
    Assertions.assertNull(metadata[FROM_TRACE_MESSAGE_KEY])
    Assertions.assertEquals(jobId, metadata[JOB_ID_KEY])
    Assertions.assertEquals(attemptNumber, metadata[ATTEMPT_NUMBER_KEY])
  }

  @Test
  fun testConnectorCommandFailureFromTrace() {
    val jobId = 12345L
    val attemptNumber = 1
    val failureReason: FailureReason = connectorCommandFailure(TRACE_MESSAGE, jobId, attemptNumber, FailureHelper.ConnectorCommand.DISCOVER)

    val metadata = failureReason.metadata.additionalProperties
    Assertions.assertEquals("discover", metadata[CONNECTOR_COMMAND_KEY])
    Assertions.assertEquals(true, metadata[FROM_TRACE_MESSAGE_KEY])
    Assertions.assertEquals(jobId, metadata[JOB_ID_KEY])
    Assertions.assertEquals(attemptNumber, metadata[ATTEMPT_NUMBER_KEY])
  }

  @Test
  fun testSourceFailure() {
    val t: Throwable = RuntimeException()
    val jobId = 12345L
    val attemptNumber = 1
    val failureReason = sourceFailure(t, jobId, attemptNumber)
    Assertions.assertEquals(FailureReason.FailureOrigin.SOURCE, failureReason.failureOrigin)

    val metadata = failureReason.metadata.additionalProperties
    Assertions.assertEquals("read", metadata[CONNECTOR_COMMAND_KEY])
    Assertions.assertNull(metadata[FROM_TRACE_MESSAGE_KEY])
    Assertions.assertEquals(jobId, metadata[JOB_ID_KEY])
    Assertions.assertEquals(attemptNumber, metadata[ATTEMPT_NUMBER_KEY])
  }

  @Test
  fun testSourceHeartbeatFailure() {
    val t: Throwable = RuntimeException()
    val jobId = 12345L
    val attemptNumber = 1
    val failureReason = sourceFailure(t, jobId, attemptNumber)
    Assertions.assertEquals(FailureReason.FailureOrigin.SOURCE, failureReason.failureOrigin)

    // assertEquals(FailureType.);
    val metadata = failureReason.metadata.additionalProperties
    Assertions.assertEquals("read", metadata[CONNECTOR_COMMAND_KEY])
    Assertions.assertNull(metadata[FROM_TRACE_MESSAGE_KEY])
    Assertions.assertEquals(jobId, metadata[JOB_ID_KEY])
    Assertions.assertEquals(attemptNumber, metadata[ATTEMPT_NUMBER_KEY])
  }

  @Test
  fun testSourceFailureFromTrace() {
    val jobId = 12345L
    val attemptNumber = 1
    val failureReason: FailureReason = sourceFailure(TRACE_MESSAGE, jobId, attemptNumber)
    Assertions.assertEquals(FailureReason.FailureOrigin.SOURCE, failureReason.failureOrigin)

    val metadata = failureReason.metadata.additionalProperties
    Assertions.assertEquals("read", metadata[CONNECTOR_COMMAND_KEY])
    Assertions.assertEquals(true, metadata[FROM_TRACE_MESSAGE_KEY])
    Assertions.assertEquals(jobId, metadata[JOB_ID_KEY])
    Assertions.assertEquals(attemptNumber, metadata[ATTEMPT_NUMBER_KEY])
  }

  @Test
  fun testDestinationFailure() {
    val t: Throwable = RuntimeException()
    val jobId = 12345L
    val attemptNumber = 1
    val failureReason = destinationFailure(t, jobId, attemptNumber)
    Assertions.assertEquals(FailureReason.FailureOrigin.DESTINATION, failureReason.failureOrigin)

    val metadata = failureReason.metadata.additionalProperties
    Assertions.assertEquals("write", metadata[CONNECTOR_COMMAND_KEY])
    Assertions.assertNull(metadata[FROM_TRACE_MESSAGE_KEY])
    Assertions.assertEquals(jobId, metadata[JOB_ID_KEY])
    Assertions.assertEquals(attemptNumber, metadata[ATTEMPT_NUMBER_KEY])
  }

  @Test
  fun testDestinationFailureFromTrace() {
    val jobId = 12345L
    val attemptNumber = 1
    val failureReason: FailureReason = destinationFailure(TRACE_MESSAGE, jobId, attemptNumber)
    Assertions.assertEquals(FailureReason.FailureOrigin.DESTINATION, failureReason.failureOrigin)

    val metadata = failureReason.metadata.additionalProperties
    Assertions.assertEquals("write", metadata[CONNECTOR_COMMAND_KEY])
    Assertions.assertEquals(true, metadata[FROM_TRACE_MESSAGE_KEY])
    Assertions.assertEquals(jobId, metadata[JOB_ID_KEY])
    Assertions.assertEquals(attemptNumber, metadata[ATTEMPT_NUMBER_KEY])
  }

  @Test
  fun testCheckFailure() {
    val t: Throwable = RuntimeException()
    val jobId = 12345L
    val attemptNumber = 1
    val failureReason = checkFailure(t, jobId, attemptNumber, FailureReason.FailureOrigin.DESTINATION)
    Assertions.assertEquals(FailureReason.FailureOrigin.DESTINATION, failureReason!!.failureOrigin)

    val metadata = failureReason.metadata.additionalProperties
    Assertions.assertEquals("check", metadata[CONNECTOR_COMMAND_KEY])
    Assertions.assertNull(metadata[FROM_TRACE_MESSAGE_KEY])
    Assertions.assertEquals(jobId, metadata[JOB_ID_KEY])
    Assertions.assertEquals(attemptNumber, metadata[ATTEMPT_NUMBER_KEY])
  }

  @Test
  fun testOrderedFailures() {
    val failureReasonList: List<FailureReason> =
      orderedFailures(setOf(TRACE_FAILURE_REASON_2, TRACE_FAILURE_REASON, EXCEPTION_FAILURE_REASON))
    Assertions.assertEquals(failureReasonList[0], EXCEPTION_FAILURE_REASON)
  }

  @Test
  fun testUnknownOriginFailure() {
    val t: Throwable = RuntimeException()
    val jobId = 12345L
    val attemptNumber = 1
    val failureReason = unknownOriginFailure(t, jobId, attemptNumber)
    Assertions.assertEquals(FailureReason.FailureOrigin.UNKNOWN, failureReason.failureOrigin)
    Assertions.assertEquals("An unknown failure occurred", failureReason.externalMessage)
  }

  @Test
  fun testExceptionChainContains() {
    val t: Throwable =
      ActivityFailure(
        "",
        1L,
        2L,
        "act",
        "actId",
        RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE,
        "id",
        RuntimeException(
          "oops",
          SizeLimitException("oops too big"),
        ),
      )
    Assertions.assertTrue(exceptionChainContains(t, ActivityFailure::class.java))
    Assertions.assertTrue(exceptionChainContains(t, SizeLimitException::class.java))
    Assertions.assertFalse(exceptionChainContains(t, WorkerException::class.java))
  }

  @Test
  fun truncateWithPlatformMessageTruncatesStrings() {
    val maxWidth = 1000

    val longStr1 = String.format("%14218s", "")
    val longStr2 = String.format("%9288s", "")
    val longStr3 = String.format("%1100s", "")

    Assertions.assertTrue(longStr1.length > maxWidth)
    Assertions.assertTrue(longStr2.length > maxWidth)
    Assertions.assertTrue(longStr3.length > maxWidth)

    Assertions.assertEquals(maxWidth, truncateWithPlatformMessage(longStr1, maxWidth)!!.length)
    Assertions.assertEquals(maxWidth, truncateWithPlatformMessage(longStr2, maxWidth)!!.length)
    Assertions.assertEquals(maxWidth, truncateWithPlatformMessage(longStr3, maxWidth)!!.length)
  }

  @Test
  fun truncateWithPlatformMessageHandlesSmallStrings() {
    val shortStr1 = ""
    val shortStr2 = "a"
    val shortStr3 = "abcde"
    val shortStr4 = String.format("%10s", "")

    Assertions.assertDoesNotThrow<String?> { truncateWithPlatformMessage(shortStr1, 10) }
    Assertions.assertDoesNotThrow<String?> { truncateWithPlatformMessage(shortStr1, 0) }
    Assertions.assertDoesNotThrow<String?> { truncateWithPlatformMessage(shortStr1, -1) }
    Assertions.assertDoesNotThrow<String?> { truncateWithPlatformMessage(shortStr1, -24) }
    Assertions.assertDoesNotThrow<String?> { truncateWithPlatformMessage(shortStr2, 10) }
    Assertions.assertDoesNotThrow<String?> { truncateWithPlatformMessage(shortStr3, 3) }
    Assertions.assertDoesNotThrow<String?> { truncateWithPlatformMessage(shortStr3, 5) }
    Assertions.assertDoesNotThrow<String?> { truncateWithPlatformMessage(shortStr4, 10) }
  }

  companion object {
    private const val FROM_TRACE_MESSAGE_KEY = "from_trace_message"
    private const val CONNECTOR_COMMAND_KEY = "connector_command"
    private const val JOB_ID_KEY = "jobId"
    private const val ATTEMPT_NUMBER_KEY = "attemptNumber"

    private val TRACE_FAILURE_REASON: FailureReason =
      FailureReason()
        .withInternalMessage("internal message")
        .withStacktrace("stack trace")
        .withTimestamp(1111112)
        .withMetadata(
          Metadata()
            .withAdditionalProperty(JOB_ID_KEY, 12345)
            .withAdditionalProperty(ATTEMPT_NUMBER_KEY, 1)
            .withAdditionalProperty(FROM_TRACE_MESSAGE_KEY, true),
        )

    private val TRACE_FAILURE_REASON_2: FailureReason =
      FailureReason()
        .withInternalMessage("internal message")
        .withStacktrace("stack trace")
        .withTimestamp(1111113)
        .withMetadata(
          Metadata()
            .withAdditionalProperty(JOB_ID_KEY, 12345)
            .withAdditionalProperty(ATTEMPT_NUMBER_KEY, 1)
            .withAdditionalProperty(FROM_TRACE_MESSAGE_KEY, true),
        )

    private val EXCEPTION_FAILURE_REASON: FailureReason =
      FailureReason()
        .withInternalMessage("internal message")
        .withStacktrace("stack trace")
        .withTimestamp(1111111)
        .withMetadata(
          Metadata()
            .withAdditionalProperty(JOB_ID_KEY, 12345)
            .withAdditionalProperty(ATTEMPT_NUMBER_KEY, 1),
        )

    private val TRACE_MESSAGE =
      createErrorTraceMessage(
        "trace message error",
        123.0,
        AirbyteErrorTraceMessage.FailureType.SYSTEM_ERROR,
      )
  }
}
