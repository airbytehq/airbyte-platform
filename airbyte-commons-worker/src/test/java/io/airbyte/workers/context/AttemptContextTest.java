/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.context;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceUtils.formatTag;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracerTestUtil;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AttemptContextTest {

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
  void addTagsToTrace() {
    final UUID connectionId = UUID.randomUUID();
    final Long jobId = 5L;
    final Integer attemptNumber = 3;
    new AttemptContext(connectionId, jobId, attemptNumber).addTagsToTrace();

    verifySpanSetTag(CONNECTION_ID_KEY, connectionId);
    verifySpanSetTag(JOB_ID_KEY, jobId);
    verifySpanSetTag(ATTEMPT_NUMBER_KEY, attemptNumber);
  }

  @Test
  void addTagsToTraceShouldIgnoreNullValues() {
    final UUID connectionId = UUID.randomUUID();
    final Long jobId = 3L;
    final Integer attemptNumber = 7;

    new AttemptContext(connectionId, null, null).addTagsToTrace();
    verifySpanSetTag(CONNECTION_ID_KEY, connectionId);
    reset(span);

    new AttemptContext(null, jobId, null).addTagsToTrace();
    verifySpanSetTag(JOB_ID_KEY, jobId);
    reset(span);

    new AttemptContext(null, null, attemptNumber).addTagsToTrace();
    verifySpanSetTag(ATTEMPT_NUMBER_KEY, attemptNumber);
    reset(span);
  }

  private void verifySpanSetTag(final String tag, final Object value) {
    verify(span).setTag(formatTag(tag), value.toString());
  }

}
