/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib

import io.airbyte.metrics.MetricAttribute
import io.github.oshai.kotlinlogging.KotlinLogging
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.context.ContextKey
import java.nio.file.Path
import java.util.UUID
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

/**
 * Collection of utility methods to help with performance tracing.
 */
object ApmTraceUtils {
  /**
   * String format for the name of tags added to spans.
   */
  const val TAG_FORMAT: String = "airbyte.%s.%s"

  /**
   * Standard prefix for tags added to spans.
   */
  const val TAG_PREFIX: String = "metadata"

  /**
   * Converts the provided metric attributes to tags and adds them to the currently active span, if
   * one exists. <br></br>
   * All tags added via this method will use the default [.TAG_PREFIX] namespace.
   *
   * @param attrs A list of attributes to be converted to tags and added to the currently active span.
   */
  fun addTagsToTrace(attrs: List<MetricAttribute>) {
    val tags =
      attrs
        .stream()
        .collect(Collectors.toMap(MetricAttribute::key, MetricAttribute::value))

    addTagsToTrace(tags, TAG_PREFIX)
  }

  /**
   * Adds all provided tags to the currently active span, if one exists, under the provided tag name
   * namespace.
   *
   * @param tags A map of tags to be added to the currently active span.
   * @param tagPrefix The prefix to be added to each custom tag name.
   */

  fun addTagsToTrace(tags: Map<String?, Any?>) {
    addTagsToTrace(tags, TAG_PREFIX)
  }

  /**
   * Adds all the provided tags to the currently active span, if one exists. <br></br>
   * All tags added via this method will use the default [.TAG_PREFIX] namespace.
   *
   * @param tags A map of tags to be added to the currently active span.
   */
  fun addTagsToTrace(
    tags: Map<String?, Any?>,
    tagPrefix: String = TAG_PREFIX,
  ) {
    addTagsToTrace(Span.current(), tags, tagPrefix)
  }

  fun rootSpan(): Span? = Context.current().get(ContextKey.named("opentelemetry-traces-local-root-span"))

  /**
   * Adds all the provided tags to the provided span, if one exists.
   *
   * @param span The [Span] that will be associated with the tags.
   * @param tags A map of tags to be added to the currently active span.
   * @param tagPrefix The prefix to be added to each custom tag name.
   */
  fun addTagsToTrace(
    span: Span?,
    tags: Map<String?, Any?>,
    tagPrefix: String,
  ) {
    if (span != null) {
      tags.entries
        .stream()
        .filter { e: Map.Entry<String?, Any?> -> e.key != null && e.value != null }
        .forEach { entry: Map.Entry<String?, Any?> ->
          span.setAttribute(
            formatTag(entry.key, tagPrefix),
            entry.value.toString(),
          )
        }
    }
  }

  /**
   * Adds all the provided values to the currently active span, if one exists. <br></br>
   * All tags added via this method will use the default [.TAG_PREFIX] namespace. Any null
   * values will be ignored.
   */
  fun addTagsToTrace(
    connectionId: UUID?,
    attemptNumber: Long?,
    jobId: String?,
    jobRoot: Path?,
  ) {
    val tags: MutableMap<String?, Any?> = HashMap()

    if (connectionId != null) {
      tags[ApmTraceConstants.Tags.CONNECTION_ID_KEY] = connectionId
    }
    if (attemptNumber != null) {
      tags[ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY] = attemptNumber
    }
    if (jobId != null) {
      tags[ApmTraceConstants.Tags.JOB_ID_KEY] = jobId
    }
    if (jobRoot != null) {
      tags[ApmTraceConstants.Tags.JOB_ROOT_KEY] = jobRoot
    }
    addTagsToTrace(tags)
  }

  /**
   * Adds an exception to the currently active span, if one exists.
   *
   * @param t The [Throwable] to be added to the currently active span.
   */
  fun addExceptionToTrace(t: Throwable) {
    Span.current().recordException(t)
  }

  /**
   * Adds all the provided tags to the root span.
   *
   * @param tags A map of tags to be added to the root span.
   */
  fun addTagsToRootSpan(tags: Map<String?, Any?>) {
    rootSpan()?.also {
      tags.forEach { (key: String?, value: Any?) -> it.setAttribute(formatTag(key, TAG_PREFIX), value.toString()) }
    }
  }

  /**
   * Adds an exception to the root span, if an active one exists.
   *
   * @param t The [Throwable] to be added to the provided span.
   */
  fun recordErrorOnRootSpan(t: Throwable) {
    rootSpan()?.also {
      it.recordException(t)
    }
  }

  /**
   * Formats the tag key using [.TAG_FORMAT] provided by this utility with the provided tag
   * prefix.
   *
   * @param tagKey The tag key to format.
   * @param tagPrefix The prefix to be added to each custom tag name.
   * @return The formatted tag key.
   */
  fun formatTag(
    tagKey: String?,
    tagPrefix: String? = TAG_PREFIX,
  ): String = String.format(TAG_FORMAT, tagPrefix, tagKey)
}
