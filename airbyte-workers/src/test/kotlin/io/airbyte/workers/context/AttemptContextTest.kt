/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.context

import io.airbyte.metrics.lib.ApmTraceConstants
import io.airbyte.metrics.lib.ApmTraceUtils
import io.mockk.clearMocks
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.unmockkStatic
import io.mockk.verify
import io.opentelemetry.api.trace.Span
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class AttemptContextTest {
  @AfterEach
  fun afterEach() {
    unmockkStatic(Span::class)
  }

  @Test
  fun addTagsToTrace() {
    val span = mockk<Span>(relaxed = true)
    mockkStatic(Span::class)
    io.mockk.every { Span.current() } returns span

    val connectionId = UUID.randomUUID()
    val jobId = 5L
    val attemptNumber = 3
    AttemptContext(connectionId, jobId, attemptNumber).addTagsToTrace()

    verifySpanSetTag(span, ApmTraceConstants.Tags.CONNECTION_ID_KEY, connectionId)
    verifySpanSetTag(span, ApmTraceConstants.Tags.JOB_ID_KEY, jobId)
    verifySpanSetTag(span, ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY, attemptNumber)
  }

  @Test
  fun addTagsToTraceShouldIgnoreNullValues() {
    val span = mockk<Span>(relaxed = true)
    mockkStatic(Span::class)
    io.mockk.every { Span.current() } returns span

    val connectionId = UUID.randomUUID()
    val jobId = 3L
    val attemptNumber = 7

    AttemptContext(connectionId, null, null).addTagsToTrace()
    verifySpanSetTag(span, ApmTraceConstants.Tags.CONNECTION_ID_KEY, connectionId)
    clearMocks(span)

    AttemptContext(null, jobId, null).addTagsToTrace()
    verifySpanSetTag(span, ApmTraceConstants.Tags.JOB_ID_KEY, jobId)
    clearMocks(span)

    AttemptContext(null, null, attemptNumber).addTagsToTrace()
    verifySpanSetTag(span, ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY, attemptNumber)
    clearMocks(span)
  }

  private fun verifySpanSetTag(
    span: Span,
    tag: String?,
    value: Any,
  ) {
    verify(exactly = 1) { span.setAttribute(ApmTraceUtils.formatTag(tag), value.toString()) }
  }
}
