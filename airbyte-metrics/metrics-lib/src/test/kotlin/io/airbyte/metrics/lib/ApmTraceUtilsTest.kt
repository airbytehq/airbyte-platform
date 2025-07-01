/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib

import datadog.trace.api.DDTags
import datadog.trace.api.interceptor.MutableSpan
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ROOT_KEY
import io.airbyte.metrics.lib.ApmTraceUtils.TAG_FORMAT
import io.airbyte.metrics.lib.ApmTraceUtils.TAG_PREFIX
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToRootSpan
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.metrics.lib.ApmTraceUtils.formatTag
import io.airbyte.metrics.lib.ApmTraceUtils.recordErrorOnRootSpan
import io.opentracing.Span
import io.opentracing.Tracer
import io.opentracing.log.Fields
import io.opentracing.tag.Tags
import io.opentracing.util.GlobalTracerTestUtil
import org.junit.After
import org.junit.Before
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.PrintWriter
import java.io.StringWriter
import java.nio.file.Path
import java.util.List
import java.util.UUID

/**
 * Test suite for the [ApmTraceUtils] class.
 */
internal class ApmTraceUtilsTest {
  @Before
  @After
  fun clearGlobalTracer() {
    GlobalTracerTestUtil.resetGlobalTracer()
  }

  @Test
  fun testAddingTags() {
    val span = Mockito.mock(Span::class.java)
    val tracer = Mockito.mock(Tracer::class.java)
    Mockito.`when`(tracer.activeSpan()).thenReturn(span)
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer)
    addTagsToTrace(TAGS)
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1)
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2)
  }

  @Test
  fun convertsAndAddsAttributes() {
    val span = Mockito.mock(Span::class.java)
    val tracer = Mockito.mock(Tracer::class.java)
    Mockito.`when`(tracer.activeSpan()).thenReturn(span)

    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer)

    val attrs = List.of(MetricAttribute(TAG_1, VALUE_1), MetricAttribute(TAG_2, VALUE_2))
    addTagsToTrace(attrs)

    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1)
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2)
  }

  @Test
  fun testAddingTagsWithPrefix() {
    val span = Mockito.mock(Span::class.java)
    val tracer = Mockito.mock(Tracer::class.java)
    Mockito.`when`(tracer.activeSpan()).thenReturn(span)
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer)
    val tagPrefix = PREFIX
    addTagsToTrace(TAGS, tagPrefix)
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, tagPrefix, TAG_1), VALUE_1)
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, tagPrefix, TAG_2), VALUE_2)
  }

  @Test
  fun testAddingTagsToSpanWithPrefix() {
    val tagPrefix = PREFIX
    val span = Mockito.mock(Span::class.java)
    addTagsToTrace(span, TAGS, tagPrefix)
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, tagPrefix, TAG_1), VALUE_1)
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, tagPrefix, TAG_2), VALUE_2)
  }

  @Test
  fun testAddingTagsToNullSpanWithPrefix() {
    val tagPrefix = "prefix"
    Assertions.assertDoesNotThrow { addTagsToTrace(null, TAGS, tagPrefix) }
  }

  @Test
  fun testFormattingTagKeys() {
    val tagKey1 = "tagKey1"
    val tagPrefix1 = PREFIX

    val result1 = formatTag(tagKey1)
    Assertions.assertEquals("airbyte.metadata.$tagKey1", result1)

    val result2 = formatTag(tagKey1, tagPrefix1)
    Assertions.assertEquals("airbyte.$tagPrefix1.$tagKey1", result2)
  }

  @Test
  fun testAddingTagsToRootSpan() {
    val activeSpan =
      Mockito.mock(
        Span::class.java,
        Mockito.withSettings().extraInterfaces(
          MutableSpan::class.java,
        ),
      )
    val tracer = Mockito.mock(Tracer::class.java)
    val localRootSpan = Mockito.mock(MutableSpan::class.java)
    Mockito.`when`(tracer.activeSpan()).thenReturn(activeSpan)
    Mockito.`when`((activeSpan as MutableSpan).localRootSpan).thenReturn(localRootSpan)
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer)
    addTagsToRootSpan(TAGS)
    Mockito.verify<MutableSpan>(localRootSpan, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1)
    Mockito.verify<MutableSpan>(localRootSpan, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2)
  }

  @Test
  fun testAddingTagsToRootSpanWhenActiveSpanIsNull() {
    val tracer = Mockito.mock(Tracer::class.java)
    Mockito.`when`(tracer.activeSpan()).thenReturn(null)
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer)
    Assertions.assertDoesNotThrow { addTagsToRootSpan(TAGS) }
  }

  @Test
  fun testAddingTagsWithNullChecks() {
    val span = Mockito.mock(Span::class.java)
    val tracer = Mockito.mock(Tracer::class.java)
    Mockito.`when`(tracer.activeSpan()).thenReturn(span)
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer)

    val connectionID = UUID.randomUUID()
    val jobId = UUID.randomUUID().toString()
    val jobRoot = Path.of("dev", "null")
    val attemptNumber = 2L

    addTagsToTrace(connectionID, attemptNumber, jobId, jobRoot)
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString())
    Mockito
      .verify<Span>(span, Mockito.times(1))
      .setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString())
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId)
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString())

    Mockito.clearInvocations(span)
    addTagsToTrace(null, null, jobId, jobRoot)
    Mockito.verify<Span>(span, Mockito.never()).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString())
    Mockito.verify<Span>(span, Mockito.never()).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString())
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId)
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString())

    Mockito.clearInvocations(span)
    addTagsToTrace(connectionID, null, jobId, null)
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString())
    Mockito.verify<Span>(span, Mockito.never()).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString())
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId)
    Mockito.verify<Span>(span, Mockito.never()).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString())

    Mockito.clearInvocations(span)
    addTagsToTrace(null, attemptNumber, jobId, null)
    Mockito
      .verify<Span>(span, Mockito.times(1))
      .setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString())
    Mockito.verify<Span>(span, Mockito.times(1)).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId)
    Mockito.verify<Span>(span, Mockito.never()).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), jobRoot.toString())
    Mockito.verify<Span>(span, Mockito.never()).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString())

    Mockito.clearInvocations(span)
    addTagsToTrace(null as UUID?, null, null, null)
    Mockito.verify<Span>(span, Mockito.never()).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString())
    Mockito.verify<Span>(span, Mockito.never()).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString())
    Mockito.verify<Span>(span, Mockito.never()).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId)
    Mockito.verify<Span>(span, Mockito.never()).setTag(kotlin.String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString())
  }

  @Test
  fun testRecordErrorOnRootSpan() {
    val activeSpan =
      Mockito.mock(
        Span::class.java,
        Mockito.withSettings().extraInterfaces(
          MutableSpan::class.java,
        ),
      )
    val tracer = Mockito.mock(Tracer::class.java)
    val localRootSpan = Mockito.mock(MutableSpan::class.java)
    val exception = Mockito.mock(Throwable::class.java)
    Mockito.`when`(tracer.activeSpan()).thenReturn(activeSpan)
    Mockito.`when`((activeSpan as MutableSpan).localRootSpan).thenReturn(localRootSpan)
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer)

    recordErrorOnRootSpan(exception)
    Mockito.verify<Span>(activeSpan, Mockito.times(1)).setTag(Tags.ERROR, true)
    Mockito.verify<Span>(activeSpan, Mockito.times(1)).log(java.util.Map.of(Fields.ERROR_OBJECT, exception))

    Mockito.verify(localRootSpan, Mockito.times(1)).setError(true)
    Mockito.verify(localRootSpan, Mockito.times(1)).setTag(DDTags.ERROR_MSG, exception.message)
    Mockito.verify(localRootSpan, Mockito.times(1)).setTag(DDTags.ERROR_TYPE, exception.javaClass.name)
    val expectedErrorString = StringWriter()
    exception.printStackTrace(PrintWriter(expectedErrorString))
    Mockito.verify(localRootSpan, Mockito.times(1)).setTag(DDTags.ERROR_STACK, expectedErrorString.toString())
  }

  @Test
  fun testRecordErrorOnRootSpanWhenActiveSpanIsNull() {
    val exception = Mockito.mock(Throwable::class.java)
    val tracer = Mockito.mock(Tracer::class.java)
    Mockito.`when`(tracer.activeSpan()).thenReturn(null)
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer)
    Assertions.assertDoesNotThrow { recordErrorOnRootSpan(exception) }
  }

  companion object {
    private const val TAG_1 = "tag1"
    private const val TAG_2 = "tag2"
    private const val VALUE_1 = "foo"
    private const val VALUE_2 = "bar"
    private const val PREFIX = "prefix"
    private val TAGS: Map<String?, Any?> = java.util.Map.of<String?, Any?>(TAG_1, VALUE_1, TAG_2, VALUE_2)
  }
}
