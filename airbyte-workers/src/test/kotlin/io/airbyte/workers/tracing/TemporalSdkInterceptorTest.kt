/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.tracing

import datadog.trace.api.interceptor.MutableSpan
import io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

/**
 * Test suite for the [TemporalSdkInterceptor] class.
 */
internal class TemporalSdkInterceptorTest {
  @Test
  fun testOnTraceCompleteErrorMsg() {
    val simple = DummySpan()

    val noError = DummySpan()
    noError.setError(false)
    noError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    noError.setTag(TAG, VALUE)

    val otherError = DummySpan()
    otherError.setError(true)
    otherError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    otherError.setTag(TemporalSdkInterceptor.Companion.ERROR_MSG_TAG, SOME_OTHER_ERROR)

    val temporalExitMsgOperationNameError = DummySpan()
    temporalExitMsgOperationNameError.setError(true)
    temporalExitMsgOperationNameError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    temporalExitMsgOperationNameError.setTag(TemporalSdkInterceptor.Companion.ERROR_MSG_TAG, TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE)

    val connectionManagerTemporalExitMsgResourceNameError = DummySpan()
    connectionManagerTemporalExitMsgResourceNameError.setError(true)
    connectionManagerTemporalExitMsgResourceNameError.setResourceName(
      TemporalSdkInterceptor.Companion.CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME,
    )
    connectionManagerTemporalExitMsgResourceNameError.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MSG_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )

    val syncWorkflowTemporalExitMsgResourceNameError = DummySpan()
    syncWorkflowTemporalExitMsgResourceNameError.setError(true)
    syncWorkflowTemporalExitMsgResourceNameError.setResourceName(TemporalSdkInterceptor.Companion.SYNC_WORKFLOW_IMPL_RESOURCE_NAME)
    syncWorkflowTemporalExitMsgResourceNameError.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MSG_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )

    val temporalExitMsgOtherOperationError = DummySpan()
    temporalExitMsgOtherOperationError.setError(true)
    temporalExitMsgOtherOperationError.setOperationName(OTHER_OPERATION)
    temporalExitMsgOtherOperationError.setTag(TemporalSdkInterceptor.Companion.ERROR_MSG_TAG, TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE)

    val temporalExitMsgOtherResourceError = DummySpan()
    temporalExitMsgOtherResourceError.setError(true)
    temporalExitMsgOtherResourceError.setResourceName(OTHER_RESOURCE)
    temporalExitMsgOtherResourceError.setTag(TemporalSdkInterceptor.Companion.ERROR_MSG_TAG, TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE)

    val spans =
      listOf<MutableSpan>(
        simple,
        noError,
        otherError,
        temporalExitMsgOperationNameError,
        connectionManagerTemporalExitMsgResourceNameError,
        syncWorkflowTemporalExitMsgResourceNameError,
        temporalExitMsgOtherOperationError,
        temporalExitMsgOtherResourceError,
      )

    val interceptor = TemporalSdkInterceptor()
    val actual: Collection<MutableSpan> = interceptor.onTraceComplete(spans)

    Assertions.assertEquals(spans, actual)
    Assertions.assertFalse(simple.isError())
    Assertions.assertFalse(noError.isError())
    Assertions.assertTrue(otherError.isError())
    Assertions.assertFalse(temporalExitMsgOperationNameError.isError())
    Assertions.assertFalse(connectionManagerTemporalExitMsgResourceNameError.isError())
    Assertions.assertFalse(syncWorkflowTemporalExitMsgResourceNameError.isError())
    Assertions.assertTrue(temporalExitMsgOtherOperationError.isError())
    Assertions.assertTrue(temporalExitMsgOtherResourceError.isError())
  }

  @Test
  fun testOnTraceCompleteErrorMessage() {
    val simple = DummySpan()

    val noError = DummySpan()
    noError.setError(false)
    noError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    noError.setTag(TAG, VALUE)

    val otherError = DummySpan()
    otherError.setError(true)
    otherError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    otherError.setTag(TemporalSdkInterceptor.Companion.ERROR_MESSAGE_TAG, SOME_OTHER_ERROR)

    val temporalExitMsgOperationNameError = DummySpan()
    temporalExitMsgOperationNameError.setError(true)
    temporalExitMsgOperationNameError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    temporalExitMsgOperationNameError.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MESSAGE_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )

    val connectionManagerTemporalExitMsgResourceNameError = DummySpan()
    connectionManagerTemporalExitMsgResourceNameError.setError(true)
    connectionManagerTemporalExitMsgResourceNameError.setResourceName(
      TemporalSdkInterceptor.Companion.CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME,
    )
    connectionManagerTemporalExitMsgResourceNameError.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MESSAGE_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )

    val syncWorkflowTemporalExitMsgResourceNameError = DummySpan()
    syncWorkflowTemporalExitMsgResourceNameError.setError(true)
    syncWorkflowTemporalExitMsgResourceNameError.setResourceName(TemporalSdkInterceptor.Companion.SYNC_WORKFLOW_IMPL_RESOURCE_NAME)
    syncWorkflowTemporalExitMsgResourceNameError.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MESSAGE_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )

    val temporalExitMsgOtherOperationError = DummySpan()
    temporalExitMsgOtherOperationError.setError(true)
    temporalExitMsgOtherOperationError.setOperationName(OTHER_OPERATION)
    temporalExitMsgOtherOperationError.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MESSAGE_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )

    val temporalExitMsgOtherResourceError = DummySpan()
    temporalExitMsgOtherResourceError.setError(true)
    temporalExitMsgOtherResourceError.setResourceName(OTHER_RESOURCE)
    temporalExitMsgOtherResourceError.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MESSAGE_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )

    val spans =
      listOf<MutableSpan>(
        simple,
        noError,
        otherError,
        temporalExitMsgOperationNameError,
        connectionManagerTemporalExitMsgResourceNameError,
        syncWorkflowTemporalExitMsgResourceNameError,
        temporalExitMsgOtherOperationError,
        temporalExitMsgOtherResourceError,
      )

    val interceptor = TemporalSdkInterceptor()
    val actual: Collection<MutableSpan> = interceptor.onTraceComplete(spans)

    Assertions.assertEquals(spans, actual)
    Assertions.assertFalse(simple.isError())
    Assertions.assertFalse(noError.isError())
    Assertions.assertTrue(otherError.isError())
    Assertions.assertFalse(temporalExitMsgOperationNameError.isError())
    Assertions.assertFalse(connectionManagerTemporalExitMsgResourceNameError.isError())
    Assertions.assertFalse(syncWorkflowTemporalExitMsgResourceNameError.isError())
    Assertions.assertTrue(temporalExitMsgOtherOperationError.isError())
    Assertions.assertTrue(temporalExitMsgOtherResourceError.isError())
  }

  @Test
  fun testIsExitTraceErrorMsg() {
    val interceptor = TemporalSdkInterceptor()

    Assertions.assertFalse(interceptor.isExitTrace(null))
    Assertions.assertFalse(interceptor.isExitTrace(DummySpan()))

    val temporalTraceWithOperationName = DummySpan()
    temporalTraceWithOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithOperationName))

    val temporalTraceWithResourceName = DummySpan()
    temporalTraceWithResourceName.setResourceName(TemporalSdkInterceptor.Companion.CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME)
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithResourceName))

    val temporalTraceWithErrorAndOperationName = DummySpan()
    temporalTraceWithErrorAndOperationName.setError(true)
    temporalTraceWithErrorAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndOperationName))

    val temporalTraceWithErrorAndConnectionManagerResourceName = DummySpan()
    temporalTraceWithErrorAndConnectionManagerResourceName.setError(true)
    temporalTraceWithErrorAndConnectionManagerResourceName.setResourceName(
      TemporalSdkInterceptor.Companion.CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME,
    )
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndConnectionManagerResourceName))

    val temporalTraceWithErrorAndSyncWorkflowResourceName = DummySpan()
    temporalTraceWithErrorAndSyncWorkflowResourceName.setError(true)
    temporalTraceWithErrorAndSyncWorkflowResourceName.setResourceName(TemporalSdkInterceptor.Companion.SYNC_WORKFLOW_IMPL_RESOURCE_NAME)
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndSyncWorkflowResourceName))

    val temporalTraceWithExitErrorAndOperationName = DummySpan()
    temporalTraceWithExitErrorAndOperationName.setError(true)
    temporalTraceWithExitErrorAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    temporalTraceWithExitErrorAndOperationName.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MSG_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )
    Assertions.assertTrue(interceptor.isExitTrace(temporalTraceWithExitErrorAndOperationName))

    val temporalTraceWithExitErrorAndResourceName = DummySpan()
    temporalTraceWithExitErrorAndResourceName.setError(true)
    temporalTraceWithExitErrorAndResourceName.setResourceName(TemporalSdkInterceptor.Companion.CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME)
    temporalTraceWithExitErrorAndResourceName.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MSG_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )
    Assertions.assertTrue(interceptor.isExitTrace(temporalTraceWithExitErrorAndResourceName))

    val otherTemporalTraceWithExitErrorAndOtherOperationName = DummySpan()
    otherTemporalTraceWithExitErrorAndOtherOperationName.setError(true)
    otherTemporalTraceWithExitErrorAndOtherOperationName.setOperationName(OTHER_OPERATION)
    otherTemporalTraceWithExitErrorAndOtherOperationName.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MSG_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )
    Assertions.assertFalse(interceptor.isExitTrace(otherTemporalTraceWithExitErrorAndOtherOperationName))

    val otherTemporalTraceWithExitErrorAndOtherResourceName = DummySpan()
    otherTemporalTraceWithExitErrorAndOtherResourceName.setError(true)
    otherTemporalTraceWithExitErrorAndOtherResourceName.setResourceName(OTHER_RESOURCE)
    otherTemporalTraceWithExitErrorAndOtherResourceName.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MSG_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )
    Assertions.assertFalse(interceptor.isExitTrace(otherTemporalTraceWithExitErrorAndOtherResourceName))
  }

  @Test
  fun testIsExitTraceErrorMessage() {
    val interceptor = TemporalSdkInterceptor()

    Assertions.assertFalse(interceptor.isExitTrace(null))
    Assertions.assertFalse(interceptor.isExitTrace(DummySpan()))

    val temporalTraceWithOperationName = DummySpan()
    temporalTraceWithOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithOperationName))

    val temporalTraceWithResourceName = DummySpan()
    temporalTraceWithResourceName.setResourceName(TemporalSdkInterceptor.Companion.CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME)
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithResourceName))

    val temporalTraceWithErrorAndOperationName = DummySpan()
    temporalTraceWithErrorAndOperationName.setError(true)
    temporalTraceWithErrorAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndOperationName))

    val temporalTraceWithErrorAndConnectionManagerResourceName = DummySpan()
    temporalTraceWithErrorAndConnectionManagerResourceName.setError(true)
    temporalTraceWithErrorAndConnectionManagerResourceName.setResourceName(
      TemporalSdkInterceptor.Companion.CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME,
    )
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndConnectionManagerResourceName))

    val temporalTraceWithErrorAndSyncWorkflowResourceName = DummySpan()
    temporalTraceWithErrorAndSyncWorkflowResourceName.setError(true)
    temporalTraceWithErrorAndSyncWorkflowResourceName.setResourceName(TemporalSdkInterceptor.Companion.SYNC_WORKFLOW_IMPL_RESOURCE_NAME)
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndSyncWorkflowResourceName))

    val temporalTraceWithExitErrorAndOperationName = DummySpan()
    temporalTraceWithExitErrorAndOperationName.setError(true)
    temporalTraceWithExitErrorAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    temporalTraceWithExitErrorAndOperationName.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MESSAGE_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )
    Assertions.assertTrue(interceptor.isExitTrace(temporalTraceWithExitErrorAndOperationName))

    val temporalTraceWithExitErrorAndResourceName = DummySpan()
    temporalTraceWithExitErrorAndResourceName.setError(true)
    temporalTraceWithExitErrorAndResourceName.setResourceName(TemporalSdkInterceptor.Companion.CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME)
    temporalTraceWithExitErrorAndResourceName.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MESSAGE_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )
    Assertions.assertTrue(interceptor.isExitTrace(temporalTraceWithExitErrorAndResourceName))

    val otherTemporalTraceWithExitErrorAndOtherOperationName = DummySpan()
    otherTemporalTraceWithExitErrorAndOtherOperationName.setError(true)
    otherTemporalTraceWithExitErrorAndOtherOperationName.setOperationName(OTHER_OPERATION)
    otherTemporalTraceWithExitErrorAndOtherOperationName.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MESSAGE_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )
    Assertions.assertFalse(interceptor.isExitTrace(otherTemporalTraceWithExitErrorAndOtherOperationName))

    val otherTemporalTraceWithExitErrorAndOtherResourceName = DummySpan()
    otherTemporalTraceWithExitErrorAndOtherResourceName.setError(true)
    otherTemporalTraceWithExitErrorAndOtherResourceName.setResourceName(OTHER_RESOURCE)
    otherTemporalTraceWithExitErrorAndOtherResourceName.setTag(
      TemporalSdkInterceptor.Companion.ERROR_MESSAGE_TAG,
      TemporalSdkInterceptor.Companion.EXIT_ERROR_MESSAGE,
    )
    Assertions.assertFalse(interceptor.isExitTrace(otherTemporalTraceWithExitErrorAndOtherResourceName))
  }

  @Test
  fun testIsDestroyThreadTrace() {
    val interceptor = TemporalSdkInterceptor()

    Assertions.assertFalse(interceptor.isDestroyThreadTrace(null))
    Assertions.assertFalse(interceptor.isDestroyThreadTrace(DummySpan()))

    val temporalTraceWithOperationName = DummySpan()
    temporalTraceWithOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithOperationName))

    val temporalTraceWithResourceName = DummySpan()
    temporalTraceWithResourceName.setResourceName(TemporalSdkInterceptor.Companion.CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME)
    Assertions.assertFalse(interceptor.isExitTrace(temporalTraceWithResourceName))

    val temporalTraceWithErrorTypeAndOperationName = DummySpan()
    temporalTraceWithErrorTypeAndOperationName.setError(true)
    temporalTraceWithErrorTypeAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    temporalTraceWithErrorTypeAndOperationName.setTag(
      TemporalSdkInterceptor.Companion.ERROR_TYPE_TAG,
      "io.temporal.internal.sync.DestroyWorkflowThreadError",
    )
    Assertions.assertTrue(interceptor.isDestroyThreadTrace(temporalTraceWithErrorTypeAndOperationName))

    val temporalTraceWithOtherErrorTypeAndOperationName = DummySpan()
    temporalTraceWithOtherErrorTypeAndOperationName.setError(true)
    temporalTraceWithOtherErrorTypeAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME)
    temporalTraceWithOtherErrorTypeAndOperationName.setTag(TemporalSdkInterceptor.Companion.ERROR_TYPE_TAG, "DestroyThreadError")
    Assertions.assertFalse(interceptor.isDestroyThreadTrace(temporalTraceWithOtherErrorTypeAndOperationName))

    val temporalTraceWithErrorTypeAndOtherOperationName = DummySpan()
    temporalTraceWithErrorTypeAndOtherOperationName.setError(true)
    temporalTraceWithErrorTypeAndOtherOperationName.setOperationName(OTHER_OPERATION)
    temporalTraceWithErrorTypeAndOtherOperationName.setTag(
      TemporalSdkInterceptor.Companion.ERROR_TYPE_TAG,
      "io.temporal.internal.sync.DestroyWorkflowThreadError",
    )
    Assertions.assertFalse(interceptor.isDestroyThreadTrace(temporalTraceWithErrorTypeAndOtherOperationName))
  }

  @Test
  fun testPriority() {
    val interceptor = TemporalSdkInterceptor()
    Assertions.assertEquals(0, interceptor.priority())
  }

  companion object {
    private const val OTHER_OPERATION = "OtherOperation"
    private const val OTHER_RESOURCE = "OtherResource"
    private const val SOME_OTHER_ERROR = "some other error"
    private const val TAG = "tag"
    private const val VALUE = "value"
  }
}
