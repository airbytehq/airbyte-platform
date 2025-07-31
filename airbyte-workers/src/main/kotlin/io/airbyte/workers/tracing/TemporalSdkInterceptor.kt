/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.tracing

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.DDTags
import datadog.trace.api.interceptor.MutableSpan
import datadog.trace.api.interceptor.TraceInterceptor
import io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME

/**
 * Custom [TraceInterceptor] to handle Temporal SDK traces that include a non-error used to
 * exit Workflows.
 */
class TemporalSdkInterceptor : TraceInterceptor {
  override fun onTraceComplete(trace: Collection<MutableSpan>): Collection<MutableSpan> {
    val filtered = mutableListOf<MutableSpan>()

    trace.forEach { t: MutableSpan ->
      val tags = t.tags
      // if no tags, then keep the span and move on to the next one
      if (tags.isEmpty()) {
        filtered.add(t)
        return@forEach
      }

      if (isExitTrace(t) || isDestroyThreadTrace(t)) {
        t.setError(false)
      }
      filtered.add(t)
    }

    return filtered
  }

  override fun priority(): Int = 0

  /**
   * Test whether the provided [MutableSpan] contains a Temporal workflow exit error.
   *
   * @param trace The [MutableSpan] to be tested.
   * @return `true` if the [MutableSpan] contains a Temporal workflow exit error or
   * `false` otherwise.
   */
  @VisibleForTesting
  fun isExitTrace(trace: MutableSpan?): Boolean {
    if (trace == null) {
      return false
    }

    return trace.isError &&
      ERROR_MESSAGE_TAG_KEYS
        .map { key: String -> trace.tags.getOrDefault(key, "").toString() }
        .any { anotherString: String -> EXIT_ERROR_MESSAGE.equals(anotherString, ignoreCase = true) } &&
      (
        safeEquals(trace.operationName, WORKFLOW_TRACE_OPERATION_NAME) ||
          safeEquals(trace.resourceName, CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME) ||
          safeEquals(trace.resourceName, SYNC_WORKFLOW_IMPL_RESOURCE_NAME)
      )
  }

  /**
   * Test whether the provided [MutableSpan] contains a Temporal destroy thread error.
   *
   * @param trace The [MutableSpan] to be tested.
   * @return `true` if the [MutableSpan] contains a Temporal destroy thread error or
   * `false` otherwise.
   */
  @VisibleForTesting
  fun isDestroyThreadTrace(trace: MutableSpan?): Boolean {
    if (trace == null) {
      return false
    }

    return trace.isError &&
      trace.tags
        .getOrDefault(ERROR_TYPE_TAG, "")
        .toString()
        .endsWith(DESTROY_WORKFLOW_ERROR_TYPE) &&
      safeEquals(trace.operationName, WORKFLOW_TRACE_OPERATION_NAME)
  }

  /**
   * Safely test if the provided [CharSequence] equals the provided expected string value.
   *
   * @param actual The [CharSequence] to test.
   * @param expected The expected string value to be contained in the [CharSequence].
   * @return `true` if the strings are equal (ignoring case) or `false` otherwise.
   */
  private fun safeEquals(
    actual: CharSequence?,
    expected: String,
  ): Boolean =
    if (actual != null) {
      expected.equals(actual.toString(), ignoreCase = true)
    } else {
      false
    }

  companion object {
    /**
     * Connection Manager trace resource name used to scope the filtering performed by this interceptor.
     */
    const val CONNECTION_MANAGER_WORKFLOW_IMPL_RESOURCE_NAME: String = "ConnectionManagerWorkflowImpl.run"

    /**
     * Sync Workflow trace resource name used to scope the filtering performed by this interceptor.
     */
    const val SYNC_WORKFLOW_IMPL_RESOURCE_NAME: String = "SyncWorkflowImpl.run"

    /**
     * The `error.msg` tag name.
     */
    const val ERROR_MSG_TAG: String = "error.msg"

    /**
     * The `error.message` tag name.
     */
    const val ERROR_MESSAGE_TAG: String = DDTags.ERROR_MSG

    /**
     * The `error.type` tag name.
     */
    const val ERROR_TYPE_TAG: String = "error.type"

    /**
     * Error message tag key name that contains the Temporal exit error message.
     */
    val ERROR_MESSAGE_TAG_KEYS: Set<String> = setOf(ERROR_MSG_TAG, ERROR_MESSAGE_TAG)

    /**
     * Temporal exit error message text.
     */
    const val EXIT_ERROR_MESSAGE: String = "exit"

    /**
     * Temporal destroy workflow thread error type.
     */
    const val DESTROY_WORKFLOW_ERROR_TYPE: String = "DestroyWorkflowThreadError"
  }
}
