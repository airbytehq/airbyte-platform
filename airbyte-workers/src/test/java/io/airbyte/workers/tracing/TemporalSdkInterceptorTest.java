/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.tracing;

import static io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME;
import static io.airbyte.workers.tracing.TemporalSdkInterceptor.CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME;
import static io.airbyte.workers.tracing.TemporalSdkInterceptor.ERROR_MESSAGE_TAG;
import static io.airbyte.workers.tracing.TemporalSdkInterceptor.ERROR_MSG_TAG;
import static io.airbyte.workers.tracing.TemporalSdkInterceptor.ERROR_TYPE_TAG;
import static io.airbyte.workers.tracing.TemporalSdkInterceptor.EXIT_ERROR_MESSAGE;
import static io.airbyte.workers.tracing.TemporalSdkInterceptor.SYNC_WORKFLOW_IMPL_RESOURCE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link TemporalSdkInterceptor} class.
 */
class TemporalSdkInterceptorTest {

  private static final String OTHER_OPERATION = "OtherOperation";
  private static final String OTHER_RESOURCE = "OtherResource";
  private static final String SOME_OTHER_ERROR = "some other error";
  private static final String TAG = "tag";
  private static final String VALUE = "value";

  @Test
  void testOnTraceCompleteErrorMsg() {
    final var simple = new DummySpan();

    final var noError = new DummySpan();
    noError.setError(false);
    noError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    noError.setTag(TAG, VALUE);

    final var otherError = new DummySpan();
    otherError.setError(true);
    otherError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    otherError.setTag(ERROR_MSG_TAG, SOME_OTHER_ERROR);

    final var temporalExitMsgOperationNameError = new DummySpan();
    temporalExitMsgOperationNameError.setError(true);
    temporalExitMsgOperationNameError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    temporalExitMsgOperationNameError.setTag(ERROR_MSG_TAG, EXIT_ERROR_MESSAGE);

    final var connectionManagerTemporalExitMsgResourceNameError = new DummySpan();
    connectionManagerTemporalExitMsgResourceNameError.setError(true);
    connectionManagerTemporalExitMsgResourceNameError.setResourceName(CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME);
    connectionManagerTemporalExitMsgResourceNameError.setTag(ERROR_MSG_TAG, EXIT_ERROR_MESSAGE);

    final var syncWorkflowTemporalExitMsgResourceNameError = new DummySpan();
    syncWorkflowTemporalExitMsgResourceNameError.setError(true);
    syncWorkflowTemporalExitMsgResourceNameError.setResourceName(SYNC_WORKFLOW_IMPL_RESOURCE_NAME);
    syncWorkflowTemporalExitMsgResourceNameError.setTag(ERROR_MSG_TAG, EXIT_ERROR_MESSAGE);

    final var temporalExitMsgOtherOperationError = new DummySpan();
    temporalExitMsgOtherOperationError.setError(true);
    temporalExitMsgOtherOperationError.setOperationName(OTHER_OPERATION);
    temporalExitMsgOtherOperationError.setTag(ERROR_MSG_TAG, EXIT_ERROR_MESSAGE);

    final var temporalExitMsgOtherResourceError = new DummySpan();
    temporalExitMsgOtherResourceError.setError(true);
    temporalExitMsgOtherResourceError.setResourceName(OTHER_RESOURCE);
    temporalExitMsgOtherResourceError.setTag(ERROR_MSG_TAG, EXIT_ERROR_MESSAGE);

    final var spans = List.of(
        simple, noError, otherError, temporalExitMsgOperationNameError, connectionManagerTemporalExitMsgResourceNameError,
        syncWorkflowTemporalExitMsgResourceNameError, temporalExitMsgOtherOperationError,
        temporalExitMsgOtherResourceError);

    final var interceptor = new TemporalSdkInterceptor();
    final var actual = interceptor.onTraceComplete(spans);

    assertEquals(spans, actual);
    assertFalse(simple.isError());
    assertFalse(noError.isError());
    assertTrue(otherError.isError());
    assertFalse(temporalExitMsgOperationNameError.isError());
    assertFalse(connectionManagerTemporalExitMsgResourceNameError.isError());
    assertFalse(syncWorkflowTemporalExitMsgResourceNameError.isError());
    assertTrue(temporalExitMsgOtherOperationError.isError());
    assertTrue(temporalExitMsgOtherResourceError.isError());
  }

  @Test
  void testOnTraceCompleteErrorMessage() {
    final var simple = new DummySpan();

    final var noError = new DummySpan();
    noError.setError(false);
    noError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    noError.setTag(TAG, VALUE);

    final var otherError = new DummySpan();
    otherError.setError(true);
    otherError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    otherError.setTag(ERROR_MESSAGE_TAG, SOME_OTHER_ERROR);

    final var temporalExitMsgOperationNameError = new DummySpan();
    temporalExitMsgOperationNameError.setError(true);
    temporalExitMsgOperationNameError.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    temporalExitMsgOperationNameError.setTag(ERROR_MESSAGE_TAG, EXIT_ERROR_MESSAGE);

    final var connectionManagerTemporalExitMsgResourceNameError = new DummySpan();
    connectionManagerTemporalExitMsgResourceNameError.setError(true);
    connectionManagerTemporalExitMsgResourceNameError.setResourceName(CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME);
    connectionManagerTemporalExitMsgResourceNameError.setTag(ERROR_MESSAGE_TAG, EXIT_ERROR_MESSAGE);

    final var syncWorkflowTemporalExitMsgResourceNameError = new DummySpan();
    syncWorkflowTemporalExitMsgResourceNameError.setError(true);
    syncWorkflowTemporalExitMsgResourceNameError.setResourceName(SYNC_WORKFLOW_IMPL_RESOURCE_NAME);
    syncWorkflowTemporalExitMsgResourceNameError.setTag(ERROR_MESSAGE_TAG, EXIT_ERROR_MESSAGE);

    final var temporalExitMsgOtherOperationError = new DummySpan();
    temporalExitMsgOtherOperationError.setError(true);
    temporalExitMsgOtherOperationError.setOperationName(OTHER_OPERATION);
    temporalExitMsgOtherOperationError.setTag(ERROR_MESSAGE_TAG, EXIT_ERROR_MESSAGE);

    final var temporalExitMsgOtherResourceError = new DummySpan();
    temporalExitMsgOtherResourceError.setError(true);
    temporalExitMsgOtherResourceError.setResourceName(OTHER_RESOURCE);
    temporalExitMsgOtherResourceError.setTag(ERROR_MESSAGE_TAG, EXIT_ERROR_MESSAGE);

    final var spans = List.of(
        simple, noError, otherError, temporalExitMsgOperationNameError, connectionManagerTemporalExitMsgResourceNameError,
        syncWorkflowTemporalExitMsgResourceNameError, temporalExitMsgOtherOperationError,
        temporalExitMsgOtherResourceError);

    final var interceptor = new TemporalSdkInterceptor();
    final var actual = interceptor.onTraceComplete(spans);

    assertEquals(spans, actual);
    assertFalse(simple.isError());
    assertFalse(noError.isError());
    assertTrue(otherError.isError());
    assertFalse(temporalExitMsgOperationNameError.isError());
    assertFalse(connectionManagerTemporalExitMsgResourceNameError.isError());
    assertFalse(syncWorkflowTemporalExitMsgResourceNameError.isError());
    assertTrue(temporalExitMsgOtherOperationError.isError());
    assertTrue(temporalExitMsgOtherResourceError.isError());
  }

  @Test
  void testIsExitTraceErrorMsg() {
    final var interceptor = new TemporalSdkInterceptor();

    assertFalse(interceptor.isExitTrace(null));
    assertFalse(interceptor.isExitTrace(new DummySpan()));

    final var temporalTraceWithOperationName = new DummySpan();
    temporalTraceWithOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithOperationName));

    final var temporalTraceWithResourceName = new DummySpan();
    temporalTraceWithResourceName.setResourceName(CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithResourceName));

    final var temporalTraceWithErrorAndOperationName = new DummySpan();
    temporalTraceWithErrorAndOperationName.setError(true);
    temporalTraceWithErrorAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndOperationName));

    final var temporalTraceWithErrorAndConnectionManagerResourceName = new DummySpan();
    temporalTraceWithErrorAndConnectionManagerResourceName.setError(true);
    temporalTraceWithErrorAndConnectionManagerResourceName.setResourceName(CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndConnectionManagerResourceName));

    final var temporalTraceWithErrorAndSyncWorkflowResourceName = new DummySpan();
    temporalTraceWithErrorAndSyncWorkflowResourceName.setError(true);
    temporalTraceWithErrorAndSyncWorkflowResourceName.setResourceName(SYNC_WORKFLOW_IMPL_RESOURCE_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndSyncWorkflowResourceName));

    final var temporalTraceWithExitErrorAndOperationName = new DummySpan();
    temporalTraceWithExitErrorAndOperationName.setError(true);
    temporalTraceWithExitErrorAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    temporalTraceWithExitErrorAndOperationName.setTag(ERROR_MSG_TAG, EXIT_ERROR_MESSAGE);
    assertTrue(interceptor.isExitTrace(temporalTraceWithExitErrorAndOperationName));

    final var temporalTraceWithExitErrorAndResourceName = new DummySpan();
    temporalTraceWithExitErrorAndResourceName.setError(true);
    temporalTraceWithExitErrorAndResourceName.setResourceName(CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME);
    temporalTraceWithExitErrorAndResourceName.setTag(ERROR_MSG_TAG, EXIT_ERROR_MESSAGE);
    assertTrue(interceptor.isExitTrace(temporalTraceWithExitErrorAndResourceName));

    final var otherTemporalTraceWithExitErrorAndOtherOperationName = new DummySpan();
    otherTemporalTraceWithExitErrorAndOtherOperationName.setError(true);
    otherTemporalTraceWithExitErrorAndOtherOperationName.setOperationName(OTHER_OPERATION);
    otherTemporalTraceWithExitErrorAndOtherOperationName.setTag(ERROR_MSG_TAG, EXIT_ERROR_MESSAGE);
    assertFalse(interceptor.isExitTrace(otherTemporalTraceWithExitErrorAndOtherOperationName));

    final var otherTemporalTraceWithExitErrorAndOtherResourceName = new DummySpan();
    otherTemporalTraceWithExitErrorAndOtherResourceName.setError(true);
    otherTemporalTraceWithExitErrorAndOtherResourceName.setResourceName(OTHER_RESOURCE);
    otherTemporalTraceWithExitErrorAndOtherResourceName.setTag(ERROR_MSG_TAG, EXIT_ERROR_MESSAGE);
    assertFalse(interceptor.isExitTrace(otherTemporalTraceWithExitErrorAndOtherResourceName));
  }

  @Test
  void testIsExitTraceErrorMessage() {
    final var interceptor = new TemporalSdkInterceptor();

    assertFalse(interceptor.isExitTrace(null));
    assertFalse(interceptor.isExitTrace(new DummySpan()));

    final var temporalTraceWithOperationName = new DummySpan();
    temporalTraceWithOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithOperationName));

    final var temporalTraceWithResourceName = new DummySpan();
    temporalTraceWithResourceName.setResourceName(CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithResourceName));

    final var temporalTraceWithErrorAndOperationName = new DummySpan();
    temporalTraceWithErrorAndOperationName.setError(true);
    temporalTraceWithErrorAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndOperationName));

    final var temporalTraceWithErrorAndConnectionManagerResourceName = new DummySpan();
    temporalTraceWithErrorAndConnectionManagerResourceName.setError(true);
    temporalTraceWithErrorAndConnectionManagerResourceName.setResourceName(CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndConnectionManagerResourceName));

    final var temporalTraceWithErrorAndSyncWorkflowResourceName = new DummySpan();
    temporalTraceWithErrorAndSyncWorkflowResourceName.setError(true);
    temporalTraceWithErrorAndSyncWorkflowResourceName.setResourceName(SYNC_WORKFLOW_IMPL_RESOURCE_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithErrorAndSyncWorkflowResourceName));

    final var temporalTraceWithExitErrorAndOperationName = new DummySpan();
    temporalTraceWithExitErrorAndOperationName.setError(true);
    temporalTraceWithExitErrorAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    temporalTraceWithExitErrorAndOperationName.setTag(ERROR_MESSAGE_TAG, EXIT_ERROR_MESSAGE);
    assertTrue(interceptor.isExitTrace(temporalTraceWithExitErrorAndOperationName));

    final var temporalTraceWithExitErrorAndResourceName = new DummySpan();
    temporalTraceWithExitErrorAndResourceName.setError(true);
    temporalTraceWithExitErrorAndResourceName.setResourceName(CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME);
    temporalTraceWithExitErrorAndResourceName.setTag(ERROR_MESSAGE_TAG, EXIT_ERROR_MESSAGE);
    assertTrue(interceptor.isExitTrace(temporalTraceWithExitErrorAndResourceName));

    final var otherTemporalTraceWithExitErrorAndOtherOperationName = new DummySpan();
    otherTemporalTraceWithExitErrorAndOtherOperationName.setError(true);
    otherTemporalTraceWithExitErrorAndOtherOperationName.setOperationName(OTHER_OPERATION);
    otherTemporalTraceWithExitErrorAndOtherOperationName.setTag(ERROR_MESSAGE_TAG, EXIT_ERROR_MESSAGE);
    assertFalse(interceptor.isExitTrace(otherTemporalTraceWithExitErrorAndOtherOperationName));

    final var otherTemporalTraceWithExitErrorAndOtherResourceName = new DummySpan();
    otherTemporalTraceWithExitErrorAndOtherResourceName.setError(true);
    otherTemporalTraceWithExitErrorAndOtherResourceName.setResourceName(OTHER_RESOURCE);
    otherTemporalTraceWithExitErrorAndOtherResourceName.setTag(ERROR_MESSAGE_TAG, EXIT_ERROR_MESSAGE);
    assertFalse(interceptor.isExitTrace(otherTemporalTraceWithExitErrorAndOtherResourceName));
  }

  @Test
  void testIsDestroyThreadTrace() {
    final var interceptor = new TemporalSdkInterceptor();

    assertFalse(interceptor.isDestroyThreadTrace(null));
    assertFalse(interceptor.isDestroyThreadTrace(new DummySpan()));

    final var temporalTraceWithOperationName = new DummySpan();
    temporalTraceWithOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithOperationName));

    final var temporalTraceWithResourceName = new DummySpan();
    temporalTraceWithResourceName.setResourceName(CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME);
    assertFalse(interceptor.isExitTrace(temporalTraceWithResourceName));

    final var temporalTraceWithErrorTypeAndOperationName = new DummySpan();
    temporalTraceWithErrorTypeAndOperationName.setError(true);
    temporalTraceWithErrorTypeAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    temporalTraceWithErrorTypeAndOperationName.setTag(ERROR_TYPE_TAG, "io.temporal.internal.sync.DestroyWorkflowThreadError");
    assertTrue(interceptor.isDestroyThreadTrace(temporalTraceWithErrorTypeAndOperationName));

    final var temporalTraceWithOtherErrorTypeAndOperationName = new DummySpan();
    temporalTraceWithOtherErrorTypeAndOperationName.setError(true);
    temporalTraceWithOtherErrorTypeAndOperationName.setOperationName(WORKFLOW_TRACE_OPERATION_NAME);
    temporalTraceWithOtherErrorTypeAndOperationName.setTag(ERROR_TYPE_TAG, "DestroyThreadError");
    assertFalse(interceptor.isDestroyThreadTrace(temporalTraceWithOtherErrorTypeAndOperationName));

    final var temporalTraceWithErrorTypeAndOtherOperationName = new DummySpan();
    temporalTraceWithErrorTypeAndOtherOperationName.setError(true);
    temporalTraceWithErrorTypeAndOtherOperationName.setOperationName(OTHER_OPERATION);
    temporalTraceWithErrorTypeAndOtherOperationName.setTag(ERROR_TYPE_TAG, "io.temporal.internal.sync.DestroyWorkflowThreadError");
    assertFalse(interceptor.isDestroyThreadTrace(temporalTraceWithErrorTypeAndOtherOperationName));
  }

  @Test
  void testPriority() {
    final var interceptor = new TemporalSdkInterceptor();
    assertEquals(0, interceptor.priority());
  }

}
