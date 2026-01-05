/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib

import io.airbyte.metrics.lib.ApmTraceUtils.formatTag
import io.airbyte.metrics.lib.TracingHelper.addConnection
import io.airbyte.metrics.lib.TracingHelper.addSourceDestination
import io.airbyte.metrics.lib.TracingHelper.addWorkspace
import io.opentelemetry.api.trace.Span
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.util.UUID

internal class TracingHelperTest {
  @Test
  fun testAddConnection() {
    val span = Mockito.mock(Span::class.java)

    Mockito.mockStatic(Span::class.java).use { spanMock ->
      spanMock.`when`<Span> { Span.current() }.thenReturn(span)

      addConnection(null)
      verifySpanNotSet(span, ApmTraceConstants.Tags.CONNECTION_ID_KEY)

      val connectionId = UUID.randomUUID()
      addConnection(connectionId)
      verifySpanSetTag(span, ApmTraceConstants.Tags.CONNECTION_ID_KEY, connectionId)
    }
  }

  @Test
  fun testAddSourceDestination() {
    val span = Mockito.mock(Span::class.java)
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()

    Mockito.mockStatic(Span::class.java).use { spanMock ->
      spanMock.`when`<Span> { Span.current() }.thenReturn(span)

      addSourceDestination(null, null)
      verifySpanNotSet(span, ApmTraceConstants.Tags.SOURCE_ID_KEY)
      verifySpanNotSet(span, ApmTraceConstants.Tags.DESTINATION_ID_KEY)

      Mockito.reset(span)
      addSourceDestination(sourceId, null)
      verifySpanSetTag(span, ApmTraceConstants.Tags.SOURCE_ID_KEY, sourceId)
      verifySpanNotSet(span, ApmTraceConstants.Tags.DESTINATION_ID_KEY)

      Mockito.reset(span)
      addSourceDestination(null, destinationId)
      verifySpanNotSet(span, ApmTraceConstants.Tags.SOURCE_ID_KEY)
      verifySpanSetTag(span, ApmTraceConstants.Tags.DESTINATION_ID_KEY, destinationId)

      Mockito.reset(span)
      addSourceDestination(sourceId, destinationId)
      verifySpanSetTag(span, ApmTraceConstants.Tags.SOURCE_ID_KEY, sourceId)
      verifySpanSetTag(span, ApmTraceConstants.Tags.DESTINATION_ID_KEY, destinationId)
    }
  }

  @Test
  fun testAddWorkspace() {
    val span = Mockito.mock(Span::class.java)

    Mockito.mockStatic(Span::class.java).use { spanMock ->
      spanMock.`when`<Span> { Span.current() }.thenReturn(span)

      addWorkspace(null)
      verifySpanNotSet(span, ApmTraceConstants.Tags.WORKSPACE_ID_KEY)

      val workspaceId = UUID.randomUUID()
      addWorkspace(workspaceId)
      verifySpanSetTag(span, ApmTraceConstants.Tags.WORKSPACE_ID_KEY, workspaceId)
    }
  }

  private fun verifySpanNotSet(
    span: Span,
    tag: String,
  ) {
    Mockito.verify(span, Mockito.never()).setAttribute(ArgumentMatchers.eq(formatTag(tag)), ArgumentMatchers.anyString())
  }

  private fun verifySpanSetTag(
    span: Span,
    tag: String,
    value: Any,
  ) {
    Mockito.verify(span).setAttribute(formatTag(tag), value.toString())
  }
}
