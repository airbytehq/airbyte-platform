/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.unmockkAll
import io.mockk.verify
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.context.ContextKey
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.UUID

/**
 * Test suite for the [ApmTraceUtils] class.
 */
internal class ApmTraceUtilsTest {
  @AfterEach
  fun tearDown() {
    unmockkAll()
  }

  @Test
  fun testAddingTags() {
    val span = mockk<Span>(relaxed = true)

    mockkStatic(Span::class)
    every { Span.current() } returns span

    addTagsToTrace(TAGS)

    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1) }
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2) }
  }

  @Test
  fun convertsAndAddsAttributes() {
    val span = mockk<Span>(relaxed = true)

    mockkStatic(Span::class)
    every { Span.current() } returns span

    val attrs = listOf(MetricAttribute(TAG_1, VALUE_1), MetricAttribute(TAG_2, VALUE_2))
    addTagsToTrace(attrs)

    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1) }
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2) }
  }

  @Test
  fun testAddingTagsWithPrefix() {
    val span = mockk<Span>(relaxed = true)
    val tagPrefix = PREFIX

    mockkStatic(Span::class)
    every { Span.current() } returns span

    addTagsToTrace(TAGS, tagPrefix)

    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, tagPrefix, TAG_1), VALUE_1) }
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, tagPrefix, TAG_2), VALUE_2) }
  }

  @Test
  fun testAddingTagsToSpanWithPrefix() {
    val tagPrefix = PREFIX
    val span = mockk<Span>(relaxed = true)
    addTagsToTrace(span, TAGS, tagPrefix)
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, tagPrefix, TAG_1), VALUE_1) }
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, tagPrefix, TAG_2), VALUE_2) }
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
    val rootSpan = mockk<Span>(relaxed = true)
    val context = mockk<Context>()

    every { context.get(any<ContextKey<Span>>()) } returns rootSpan

    mockkStatic(Context::class)
    every { Context.current() } returns context

    addTagsToRootSpan(TAGS)

    verify(exactly = 1) { rootSpan.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1) }
    verify(exactly = 1) { rootSpan.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2) }
  }

  @Test
  fun testAddingTagsToRootSpanWhenActiveSpanIsNull() {
    // When there's no active span, this should not throw
    Assertions.assertDoesNotThrow { addTagsToRootSpan(TAGS) }
  }

  @Test
  fun testAddingTagsWithNullChecks() {
    val span = mockk<Span>(relaxed = true)

    val connectionID = UUID.randomUUID()
    val jobId = UUID.randomUUID().toString()
    val jobRoot = Path.of("dev", "null")
    val attemptNumber = 2L

    mockkStatic(Span::class)
    every { Span.current() } returns span

    addTagsToTrace(connectionID, attemptNumber, jobId, jobRoot)

    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString()) }
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString()) }
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId) }
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString()) }

    clearMocks(span, answers = false)
    addTagsToTrace(null, null, jobId, jobRoot)
    verify(exactly = 0) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString()) }
    verify(exactly = 0) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString()) }
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId) }
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString()) }

    clearMocks(span, answers = false)
    addTagsToTrace(connectionID, null, jobId, null)
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString()) }
    verify(exactly = 0) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString()) }
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId) }
    verify(exactly = 0) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString()) }

    clearMocks(span, answers = false)
    addTagsToTrace(null, attemptNumber, jobId, null)
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString()) }
    verify(exactly = 1) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId) }
    verify(exactly = 0) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), jobRoot.toString()) }
    verify(exactly = 0) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString()) }

    clearMocks(span, answers = false)
    addTagsToTrace(null as UUID?, null, null, null)
    verify(exactly = 0) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString()) }
    verify(exactly = 0) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString()) }
    verify(exactly = 0) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId) }
    verify(exactly = 0) { span.setAttribute(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString()) }
  }

  @Test
  fun testRecordErrorOnRootSpan() {
    val rootSpan = mockk<Span>(relaxed = true)
    val context = mockk<Context>()
    val exception = RuntimeException("Test exception")

    every { context.get(any<ContextKey<Span>>()) } returns rootSpan

    mockkStatic(Context::class)
    every { Context.current() } returns context

    recordErrorOnRootSpan(exception)

    verify(exactly = 1) { rootSpan.recordException(exception) }
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
