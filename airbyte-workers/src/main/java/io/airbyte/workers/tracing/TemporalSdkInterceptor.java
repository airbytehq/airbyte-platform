/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.tracing;

import static io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME;

import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.DDTags;
import datadog.trace.api.interceptor.MutableSpan;
import datadog.trace.api.interceptor.TraceInterceptor;
import io.micronaut.core.util.CollectionUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

/**
 * Custom {@link TraceInterceptor} to handle Temporal SDK traces that include a non-error used to
 * exit Workflows.
 */
public class TemporalSdkInterceptor implements TraceInterceptor {

  /**
   * Connection Manager trace resource name used to scope the filtering performed by this interceptor.
   */
  static final String CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME = "ConnectionManagerWorkflowImpl.run";

  /**
   * Sync Workflow trace resource name used to scope the filtering performed by this interceptor.
   */
  static final String SYNC_WORKFLOW_IMPL_RESOURCE_NAME = "SyncWorkflowImpl.run";

  /**
   * The {@code error.msg} tag name.
   */
  static final String ERROR_MSG_TAG = "error.msg";

  /**
   * The {@code error.message} tag name.
   */
  static final String ERROR_MESSAGE_TAG = DDTags.ERROR_MSG;

  /**
   * The {@code error.type} tag name.
   */
  static final String ERROR_TYPE_TAG = "error.type";

  /**
   * Error message tag key name that contains the Temporal exit error message.
   */
  static final Set<String> ERROR_MESSAGE_TAG_KEYS = Set.of(ERROR_MSG_TAG, ERROR_MESSAGE_TAG);

  /**
   * Temporal exit error message text.
   */
  static final String EXIT_ERROR_MESSAGE = "exit";

  /**
   * Temporal destroy workflow thread error type.
   */
  static final String DESTROY_WORKFLOW_ERROR_TYPE = "DestroyWorkflowThreadError";

  @Override
  public Collection<? extends MutableSpan> onTraceComplete(final Collection<? extends MutableSpan> trace) {
    final var filtered = new ArrayList<MutableSpan>();

    trace.forEach(t -> {
      final var tags = t.getTags();

      // if no tags, then keep the span and move on to the next one
      if (CollectionUtils.isEmpty(tags)) {
        filtered.add(t);
        return;
      }

      if (isExitTrace(t) || isDestroyThreadTrace(t)) {
        t.setError(false);
      }

      filtered.add(t);
    });

    return filtered;
  }

  @Override
  public int priority() {
    return 0;
  }

  /**
   * Test whether the provided {@link MutableSpan} contains a Temporal workflow exit error.
   *
   * @param trace The {@link MutableSpan} to be tested.
   * @return {@code true} if the {@link MutableSpan} contains a Temporal workflow exit error or
   *         {@code false} otherwise.
   */
  @VisibleForTesting
  boolean isExitTrace(final MutableSpan trace) {
    if (trace == null) {
      return false;
    }

    return trace.isError()
        && ERROR_MESSAGE_TAG_KEYS.stream().map(key -> trace.getTags().getOrDefault(key, "").toString())
            .anyMatch(EXIT_ERROR_MESSAGE::equalsIgnoreCase)
        && (safeEquals(trace.getOperationName(), WORKFLOW_TRACE_OPERATION_NAME)
            || safeEquals(trace.getResourceName(), CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME)
            || safeEquals(trace.getResourceName(), SYNC_WORKFLOW_IMPL_RESOURCE_NAME));
  }

  /**
   * Test whether the provided {@link MutableSpan} contains a Temporal destroy thread error.
   *
   * @param trace The {@link MutableSpan} to be tested.
   * @return {@code true} if the {@link MutableSpan} contains a Temporal destroy thread error or
   *         {@code false} otherwise.
   */
  @VisibleForTesting
  boolean isDestroyThreadTrace(final MutableSpan trace) {
    if (trace == null) {
      return false;
    }

    return trace.isError()
        && trace.getTags().getOrDefault(ERROR_TYPE_TAG, "").toString().endsWith(DESTROY_WORKFLOW_ERROR_TYPE)
        && safeEquals(trace.getOperationName(), WORKFLOW_TRACE_OPERATION_NAME);
  }

  /**
   * Safely test if the provided {@link CharSequence} equals the provided expected string value.
   *
   * @param actual The {@link CharSequence} to test.
   * @param expected The expected string value to be contained in the {@link CharSequence}.
   * @return {@code true} if the strings are equal (ignoring case) or {@code false} otherwise.
   */
  private boolean safeEquals(final CharSequence actual, final String expected) {
    if (actual != null) {
      return expected.equalsIgnoreCase(actual.toString());
    } else {
      return false;
    }
  }

}
