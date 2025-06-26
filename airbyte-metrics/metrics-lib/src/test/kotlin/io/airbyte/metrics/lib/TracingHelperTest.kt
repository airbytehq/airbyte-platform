/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib

import io.airbyte.metrics.lib.ApmTraceUtils.formatTag
import io.airbyte.metrics.lib.TracingHelper.addConnection
import io.airbyte.metrics.lib.TracingHelper.addSourceDestination
import io.airbyte.metrics.lib.TracingHelper.addWorkspace
import io.opentracing.Span
import io.opentracing.Tracer
import io.opentracing.util.GlobalTracerTestUtil
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.util.UUID

internal class TracingHelperTest {
  lateinit var span: Span
  lateinit var tracer: Tracer

  @BeforeEach
  fun beforeEach() {
    span = Mockito.mock(Span::class.java)
    tracer = Mockito.mock(Tracer::class.java)
    Mockito.`when`(tracer.activeSpan()).thenReturn(span)
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer)
  }

  @Test
  fun testAddConnection() {
    addConnection(null)
    verifySpanNotSet(ApmTraceConstants.Tags.CONNECTION_ID_KEY)

    val connectionId = UUID.randomUUID()
    addConnection(connectionId)
    verifySpanSetTag(ApmTraceConstants.Tags.CONNECTION_ID_KEY, connectionId)
  }

  @Test
  fun testAddSourceDestination() {
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()

    addSourceDestination(null, null)
    verifySpanNotSet(ApmTraceConstants.Tags.SOURCE_ID_KEY)
    verifySpanNotSet(ApmTraceConstants.Tags.DESTINATION_ID_KEY)

    addSourceDestination(sourceId, null)
    verifySpanSetTag(ApmTraceConstants.Tags.SOURCE_ID_KEY, sourceId)
    verifySpanNotSet(ApmTraceConstants.Tags.DESTINATION_ID_KEY)
    Mockito.reset(span)

    addSourceDestination(null, destinationId)
    verifySpanNotSet(ApmTraceConstants.Tags.SOURCE_ID_KEY)
    verifySpanSetTag(ApmTraceConstants.Tags.DESTINATION_ID_KEY, destinationId)
    Mockito.reset(span)

    addSourceDestination(sourceId, destinationId)
    verifySpanSetTag(ApmTraceConstants.Tags.SOURCE_ID_KEY, sourceId)
    verifySpanSetTag(ApmTraceConstants.Tags.DESTINATION_ID_KEY, destinationId)
  }

  @Test
  fun testAddWorkspace() {
    addWorkspace(null)
    verifySpanNotSet(ApmTraceConstants.Tags.WORKSPACE_ID_KEY)

    val workspaceId = UUID.randomUUID()
    addWorkspace(workspaceId)
    verifySpanSetTag(ApmTraceConstants.Tags.WORKSPACE_ID_KEY, workspaceId)
  }

  private fun verifySpanNotSet(tag: String) {
    Mockito.verify(span, Mockito.never()).setTag(formatTag(tag), ArgumentMatchers.eq(ArgumentMatchers.anyString()))
  }

  private fun verifySpanSetTag(
    tag: String,
    value: Any,
  ) {
    Mockito.verify(span).setTag(formatTag(tag), value.toString())
  }
}
