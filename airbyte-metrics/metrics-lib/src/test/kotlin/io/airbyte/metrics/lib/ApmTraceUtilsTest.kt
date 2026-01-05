/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib

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
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.context.ContextKey
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import java.nio.file.Path
import java.util.UUID

/**
 * Test suite for the [ApmTraceUtils] class.
 */
internal class ApmTraceUtilsTest {
  @Test
  fun testAddingTags() {
    val span = Mockito.mock(Span::class.java)

    Mockito.mockStatic(Span::class.java).use { spanMock ->
      spanMock.`when`<Span> { Span.current() }.thenReturn(span)

      addTagsToTrace(TAGS)

      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1)
      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2)
    }
  }

  @Test
  fun convertsAndAddsAttributes() {
    val span = Mockito.mock(Span::class.java)

    Mockito.mockStatic(Span::class.java).use { spanMock ->
      spanMock.`when`<Span> { Span.current() }.thenReturn(span)

      val attrs = listOf(MetricAttribute(TAG_1, VALUE_1), MetricAttribute(TAG_2, VALUE_2))
      addTagsToTrace(attrs)

      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1)
      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2)
    }
  }

  @Test
  fun testAddingTagsWithPrefix() {
    val span = Mockito.mock(Span::class.java)
    val tagPrefix = PREFIX

    Mockito.mockStatic(Span::class.java).use { spanMock ->
      spanMock.`when`<Span> { Span.current() }.thenReturn(span)

      addTagsToTrace(TAGS, tagPrefix)

      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, tagPrefix, TAG_1), VALUE_1)
      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, tagPrefix, TAG_2), VALUE_2)
    }
  }

  @Test
  fun testAddingTagsToSpanWithPrefix() {
    val tagPrefix = PREFIX
    val span = Mockito.mock(Span::class.java)
    addTagsToTrace(span, TAGS, tagPrefix)
    Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, tagPrefix, TAG_1), VALUE_1)
    Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, tagPrefix, TAG_2), VALUE_2)
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
    val rootSpan = Mockito.mock(Span::class.java)
    val context = Mockito.mock(Context::class.java)

    Mockito.`when`(context.get(any<ContextKey<Span>>())).thenReturn(rootSpan)

    Mockito.mockStatic(Context::class.java).use { contextMock ->
      contextMock.`when`<Context> { Context.current() }.thenReturn(context)

      addTagsToRootSpan(TAGS)

      Mockito.verify(rootSpan, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1)
      Mockito.verify(rootSpan, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2)
    }
  }

  @Test
  fun testAddingTagsToRootSpanWhenActiveSpanIsNull() {
    // When there's no active span, this should not throw
    Assertions.assertDoesNotThrow { addTagsToRootSpan(TAGS) }
  }

  @Test
  fun testAddingTagsWithNullChecks() {
    val span = Mockito.mock(Span::class.java)

    val connectionID = UUID.randomUUID()
    val jobId = UUID.randomUUID().toString()
    val jobRoot = Path.of("dev", "null")
    val attemptNumber = 2L

    Mockito.mockStatic(Span::class.java).use { spanMock ->
      spanMock.`when`<Span> { Span.current() }.thenReturn(span)

      addTagsToTrace(connectionID, attemptNumber, jobId, jobRoot)

      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString())
      Mockito
        .verify<Span>(span, Mockito.times(1))
        .setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString())
      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId)
      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString())

      Mockito.clearInvocations(span)
      addTagsToTrace(null, null, jobId, jobRoot)
      Mockito.verify(span, Mockito.never()).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString())
      Mockito.verify(span, Mockito.never()).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString())
      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId)
      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString())

      Mockito.clearInvocations(span)
      addTagsToTrace(connectionID, null, jobId, null)
      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString())
      Mockito.verify(span, Mockito.never()).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString())
      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId)
      Mockito.verify(span, Mockito.never()).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString())

      Mockito.clearInvocations(span)
      addTagsToTrace(null, attemptNumber, jobId, null)
      Mockito
        .verify(span, Mockito.times(1))
        .setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString())
      Mockito.verify(span, Mockito.times(1)).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId)
      Mockito.verify(span, Mockito.never()).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), jobRoot.toString())
      Mockito.verify(span, Mockito.never()).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString())

      Mockito.clearInvocations(span)
      addTagsToTrace(null as UUID?, null, null, null)
      Mockito.verify(span, Mockito.never()).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString())
      Mockito.verify(span, Mockito.never()).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString())
      Mockito.verify(span, Mockito.never()).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId)
      Mockito.verify(span, Mockito.never()).setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString())
    }
  }

  @Test
  fun testRecordErrorOnRootSpan() {
    val rootSpan = Mockito.mock(Span::class.java)
    val context = Mockito.mock(Context::class.java)
    val exception = RuntimeException("Test exception")

    Mockito.`when`(context.get(any<ContextKey<Span>>())).thenReturn(rootSpan)

    Mockito.mockStatic(Context::class.java).use { contextMock ->
      contextMock.`when`<Context> { Context.current() }.thenReturn(context)

      recordErrorOnRootSpan(exception)

      Mockito.verify(rootSpan, Mockito.times(1)).recordException(exception)
    }
  }

  @Test
  fun testRecordErrorOnRootSpanWhenActiveSpanIsNull() {
    val exception = RuntimeException("Test exception")
    // When there's no active span, this should not throw
    Assertions.assertDoesNotThrow { recordErrorOnRootSpan(exception) }
  }

  companion object {
    private const val TAG_1 = "tag1"
    private const val TAG_2 = "tag2"
    private const val VALUE_1 = "foo"
    private const val VALUE_2 = "bar"
    private const val PREFIX = "prefix"
    private val TAGS: Map<String?, Any?> = mapOf<String?, Any?>(TAG_1 to VALUE_1, TAG_2 to VALUE_2)
  }
}
