/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.context

import io.airbyte.metrics.lib.ApmTraceConstants
import io.airbyte.metrics.lib.ApmTraceUtils
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.opentracing.Span
import io.opentracing.Tracer
import io.opentracing.util.GlobalTracerTestUtil
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class AttemptContextTest {
  private lateinit var span: Span
  private lateinit var tracer: Tracer

  @BeforeEach
  fun beforeEach() {
    span = mockk<Span>(relaxed = true)
    tracer =
      mockk<Tracer> {
        every { activeSpan() } returns span
      }
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer)
  }

  @AfterEach
  fun afterEach() {
    GlobalTracerTestUtil.resetGlobalTracer()
  }

  @Test
  fun addTagsToTrace() {
    val connectionId = UUID.randomUUID()
    val jobId = 5L
    val attemptNumber = 3
    AttemptContext(connectionId, jobId, attemptNumber).addTagsToTrace()

    verifySpanSetTag(ApmTraceConstants.Tags.CONNECTION_ID_KEY, connectionId)
    verifySpanSetTag(ApmTraceConstants.Tags.JOB_ID_KEY, jobId)
    verifySpanSetTag(ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY, attemptNumber)
  }

  @Test
  fun addTagsToTraceShouldIgnoreNullValues() {
    val connectionId = UUID.randomUUID()
    val jobId = 3L
    val attemptNumber = 7

    AttemptContext(connectionId, null, null).addTagsToTrace()
    verifySpanSetTag(ApmTraceConstants.Tags.CONNECTION_ID_KEY, connectionId)
    clearMocks(span)

    AttemptContext(null, jobId, null).addTagsToTrace()
    verifySpanSetTag(ApmTraceConstants.Tags.JOB_ID_KEY, jobId)
    clearMocks(span)

    AttemptContext(null, null, attemptNumber).addTagsToTrace()
    verifySpanSetTag(ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY, attemptNumber)
    clearMocks(span)
  }

  private fun verifySpanSetTag(
    tag: String?,
    value: Any,
  ) {
    verify(exactly = 1) { span.setTag(ApmTraceUtils.formatTag(tag), value.toString()) }
  }
}
