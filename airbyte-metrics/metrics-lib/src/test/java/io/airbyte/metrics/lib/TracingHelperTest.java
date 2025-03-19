/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

import static io.airbyte.metrics.lib.ApmTraceUtils.formatTag;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.metrics.lib.ApmTraceConstants.Tags;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracerTestUtil;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TracingHelperTest {

  Span span;
  Tracer tracer;

  @BeforeEach
  void beforeEach() {
    span = mock(Span.class);
    tracer = mock(Tracer.class);
    when(tracer.activeSpan()).thenReturn(span);
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer);
  }

  @Test
  void testAddConnection() {
    TracingHelper.addConnection(null);
    verifySpanNotSet(Tags.CONNECTION_ID_KEY);

    final UUID connectionId = UUID.randomUUID();
    TracingHelper.addConnection(connectionId);
    verifySpanSetTag(Tags.CONNECTION_ID_KEY, connectionId);
  }

  @Test
  void testAddSourceDestination() {
    final UUID sourceId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();

    TracingHelper.addSourceDestination(null, null);
    verifySpanNotSet(Tags.SOURCE_ID_KEY);
    verifySpanNotSet(Tags.DESTINATION_ID_KEY);

    TracingHelper.addSourceDestination(sourceId, null);
    verifySpanSetTag(Tags.SOURCE_ID_KEY, sourceId);
    verifySpanNotSet(Tags.DESTINATION_ID_KEY);
    reset(span);

    TracingHelper.addSourceDestination(null, destinationId);
    verifySpanNotSet(Tags.SOURCE_ID_KEY);
    verifySpanSetTag(Tags.DESTINATION_ID_KEY, destinationId);
    reset(span);

    TracingHelper.addSourceDestination(sourceId, destinationId);
    verifySpanSetTag(Tags.SOURCE_ID_KEY, sourceId);
    verifySpanSetTag(Tags.DESTINATION_ID_KEY, destinationId);
  }

  @Test
  void testAddWorkspace() {
    TracingHelper.addWorkspace(null);
    verifySpanNotSet(Tags.WORKSPACE_ID_KEY);

    final UUID workspaceId = UUID.randomUUID();
    TracingHelper.addWorkspace(workspaceId);
    verifySpanSetTag(Tags.WORKSPACE_ID_KEY, workspaceId);
  }

  private void verifySpanNotSet(final String tag) {
    verify(span, never()).setTag(formatTag(tag), eq(anyString()));
  }

  private void verifySpanSetTag(final String tag, final Object value) {
    verify(span).setTag(formatTag(tag), value.toString());
  }

}
