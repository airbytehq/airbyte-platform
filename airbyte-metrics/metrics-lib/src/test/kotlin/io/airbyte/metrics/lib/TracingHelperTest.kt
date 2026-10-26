/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib

import io.airbyte.metrics.lib.ApmTraceUtils.formatTag
import io.airbyte.metrics.lib.TracingHelper.addConnection
import io.airbyte.metrics.lib.TracingHelper.addSourceDestination
import io.airbyte.metrics.lib.TracingHelper.addWorkspace
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.unmockkAll
import io.mockk.verify
import io.opentelemetry.api.trace.Span
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class TracingHelperTest {
  @AfterEach
  fun tearDown() {
    unmockkAll()
  }

  @Test
  fun testAddConnection() {
    val span = mockk<Span>(relaxed = true)

    mockkStatic(Span::class)
    every { Span.current() } returns span

    addConnection(null)
    verifySpanNotSet(span, ApmTraceConstants.Tags.CONNECTION_ID_KEY)

    val connectionId = UUID.randomUUID()
    addConnection(connectionId)
    verifySpanSetTag(span, ApmTraceConstants.Tags.CONNECTION_ID_KEY, connectionId)
  }

  @Test
  fun testAddSourceDestination() {
    val span = mockk<Span>(relaxed = true)
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()

    mockkStatic(Span::class)
    every { Span.current() } returns span

    addSourceDestination(null, null)
    verifySpanNotSet(span, ApmTraceConstants.Tags.SOURCE_ID_KEY)
    verifySpanNotSet(span, ApmTraceConstants.Tags.DESTINATION_ID_KEY)

    clearMocks(span)
    addSourceDestination(sourceId, null)
    verifySpanSetTag(span, ApmTraceConstants.Tags.SOURCE_ID_KEY, sourceId)
    verifySpanNotSet(span, ApmTraceConstants.Tags.DESTINATION_ID_KEY)

    clearMocks(span)
    addSourceDestination(null, destinationId)
    verifySpanNotSet(span, ApmTraceConstants.Tags.SOURCE_ID_KEY)
    verifySpanSetTag(span, ApmTraceConstants.Tags.DESTINATION_ID_KEY, destinationId)

    clearMocks(span)
    addSourceDestination(sourceId, destinationId)
    verifySpanSetTag(span, ApmTraceConstants.Tags.SOURCE_ID_KEY, sourceId)
    verifySpanSetTag(span, ApmTraceConstants.Tags.DESTINATION_ID_KEY, destinationId)
  }

  @Test
  fun testAddWorkspace() {
    val span = mockk<Span>(relaxed = true)

    mockkStatic(Span::class)
    every { Span.current() } returns span

    addWorkspace(null)
    verifySpanNotSet(span, ApmTraceConstants.Tags.WORKSPACE_ID_KEY)

    val workspaceId = UUID.randomUUID()
    addWorkspace(workspaceId)
    verifySpanSetTag(span, ApmTraceConstants.Tags.WORKSPACE_ID_KEY, workspaceId)
  }

  private fun verifySpanNotSet(
    span: Span,
    tag: String,
  ) {
    verify(exactly = 0) { span.setAttribute(formatTag(tag), any<String>()) }
  }

  private fun verifySpanSetTag(
    span: Span,
    tag: String,
    value: Any,
  ) {
    verify(exactly = 1) { span.setAttribute(formatTag(tag), value.toString()) }
  }
}
