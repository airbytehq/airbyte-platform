/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib

import datadog.trace.api.DDTags
import datadog.trace.api.interceptor.MutableSpan
import io.airbyte.metrics.MetricAttribute
import io.opentracing.Span
import io.opentracing.log.Fields
import io.opentracing.tag.Tags
import io.opentracing.util.GlobalTracer
import java.io.PrintWriter
import java.io.StringWriter
import java.nio.file.Path
import java.util.UUID
import java.util.stream.Collectors

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
  @JvmStatic
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

  @JvmStatic
  fun addTagsToTrace(tags: Map<String?, Any?>) {
    addTagsToTrace(tags, TAG_PREFIX)
  }

  /**
   * Adds all the provided tags to the currently active span, if one exists. <br></br>
   * All tags added via this method will use the default [.TAG_PREFIX] namespace.
   *
   * @param tags A map of tags to be added to the currently active span.
   */
  @JvmStatic
  fun addTagsToTrace(
    tags: Map<String?, Any?>,
    tagPrefix: String = TAG_PREFIX,
  ) {
    addTagsToTrace(GlobalTracer.get().activeSpan(), tags, tagPrefix)
  }

  /**
   * Adds all the provided tags to the provided span, if one exists.
   *
   * @param span The [Span] that will be associated with the tags.
   * @param tags A map of tags to be added to the currently active span.
   * @param tagPrefix The prefix to be added to each custom tag name.
   */
  @JvmStatic
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
          span.setTag(
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
  @JvmStatic
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
  @JvmStatic
  fun addExceptionToTrace(t: Throwable) {
    addExceptionToTrace(GlobalTracer.get().activeSpan(), t)
  }

  /**
   * Adds an exception to the provided span, if one exists.
   *
   * @param span The [Span] that will be associated with the exception.
   * @param t The [Throwable] to be added to the provided span.
   */
  @JvmStatic
  fun addExceptionToTrace(
    span: Span?,
    t: Throwable,
  ) {
    if (span != null) {
      span.setTag(Tags.ERROR, true)
      span.log(java.util.Map.of(Fields.ERROR_OBJECT, t))
    }
  }

  /**
   * Adds all the provided tags to the root span.
   *
   * @param tags A map of tags to be added to the root span.
   */
  @JvmStatic
  fun addTagsToRootSpan(tags: Map<String?, Any?>) {
    val activeSpan = GlobalTracer.get().activeSpan()
    if (activeSpan is MutableSpan) {
      val localRootSpan = (activeSpan as MutableSpan).localRootSpan
      tags.forEach { (key: String?, value: Any?) -> localRootSpan.setTag(formatTag(key, TAG_PREFIX), value.toString()) }
    }
  }

  /**
   * Adds an exception to the root span, if an active one exists.
   *
   * @param t The [Throwable] to be added to the provided span.
   */
  @JvmStatic
  fun recordErrorOnRootSpan(t: Throwable) {
    val activeSpan = GlobalTracer.get().activeSpan()
    if (activeSpan != null) {
      activeSpan.setTag(Tags.ERROR, true)
      activeSpan.log(java.util.Map.of(Fields.ERROR_OBJECT, t))
    }
    if (activeSpan is MutableSpan) {
      val localRootSpan = (activeSpan as MutableSpan).localRootSpan
      localRootSpan.setError(true)
      localRootSpan.setTag(DDTags.ERROR_MSG, t.message)
      localRootSpan.setTag(DDTags.ERROR_TYPE, t.javaClass.name)
      val errorString = StringWriter()
      t.printStackTrace(PrintWriter(errorString))
      localRootSpan.setTag(DDTags.ERROR_STACK, errorString.toString())
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
  @JvmStatic
  @JvmOverloads
  fun formatTag(
    tagKey: String?,
    tagPrefix: String? = TAG_PREFIX,
  ): String = String.format(TAG_FORMAT, tagPrefix, tagKey)

  @JvmStatic
  fun addActualRootCauseToTrace(e: Exception?) {
    var inner: Throwable? = e
    while (inner!!.cause != null) {
      inner = inner.cause
    }
    addTagsToTrace(mapOf(ApmTraceConstants.Tags.ERROR_ACTUAL_TYPE_KEY to inner.javaClass.name))
  }
}
