/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ROOT_KEY;
import static io.airbyte.metrics.lib.ApmTraceUtils.TAG_FORMAT;
import static io.airbyte.metrics.lib.ApmTraceUtils.TAG_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import datadog.trace.api.DDTags;
import datadog.trace.api.interceptor.MutableSpan;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracerTestUtil;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link ApmTraceUtils} class.
 */
class ApmTraceUtilsTest {

  private static final String TAG_1 = "tag1";
  private static final String TAG_2 = "tag2";
  private static final String VALUE_1 = "foo";
  private static final String VALUE_2 = "bar";
  private static final String PREFIX = "prefix";
  private static final Map<String, Object> TAGS = Map.of(TAG_1, VALUE_1, TAG_2, VALUE_2);

  @Before
  @After
  public void clearGlobalTracer() {
    GlobalTracerTestUtil.resetGlobalTracer();
  }

  @Test
  void testAddingTags() {
    final Span span = mock(Span.class);
    final Tracer tracer = mock(Tracer.class);
    when(tracer.activeSpan()).thenReturn(span);
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer);
    ApmTraceUtils.addTagsToTrace(TAGS);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2);
  }

  @Test
  void convertsAndAddsAttributes() {
    final Span span = mock(Span.class);
    final Tracer tracer = mock(Tracer.class);
    when(tracer.activeSpan()).thenReturn(span);

    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer);

    final var attrs = List.of(new MetricAttribute(TAG_1, VALUE_1), new MetricAttribute(TAG_2, VALUE_2));
    ApmTraceUtils.addTagsToTrace(attrs);

    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2);
  }

  @Test
  void testAddingTagsWithPrefix() {
    final Span span = mock(Span.class);
    final Tracer tracer = mock(Tracer.class);
    when(tracer.activeSpan()).thenReturn(span);
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer);
    final String tagPrefix = PREFIX;
    ApmTraceUtils.addTagsToTrace(TAGS, tagPrefix);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, tagPrefix, TAG_1), VALUE_1);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, tagPrefix, TAG_2), VALUE_2);
  }

  @Test
  void testAddingTagsToSpanWithPrefix() {
    final String tagPrefix = PREFIX;
    final Span span = mock(Span.class);
    ApmTraceUtils.addTagsToTrace(span, TAGS, tagPrefix);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, tagPrefix, TAG_1), VALUE_1);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, tagPrefix, TAG_2), VALUE_2);
  }

  @Test
  void testAddingTagsToNullSpanWithPrefix() {
    final String tagPrefix = "prefix";
    Assertions.assertDoesNotThrow(() -> ApmTraceUtils.addTagsToTrace(null, TAGS, tagPrefix));
  }

  @Test
  void testFormattingTagKeys() {
    final String tagKey1 = "tagKey1";
    final String tagPrefix1 = PREFIX;

    final String result1 = ApmTraceUtils.formatTag(tagKey1);
    assertEquals("airbyte.metadata." + tagKey1, result1);

    final String result2 = ApmTraceUtils.formatTag(tagKey1, tagPrefix1);
    assertEquals("airbyte." + tagPrefix1 + "." + tagKey1, result2);
  }

  @Test
  void testAddingTagsToRootSpan() {
    final Span activeSpan = mock(Span.class, withSettings().extraInterfaces(MutableSpan.class));
    final Tracer tracer = mock(Tracer.class);
    final MutableSpan localRootSpan = mock(MutableSpan.class);
    when(tracer.activeSpan()).thenReturn(activeSpan);
    when(((MutableSpan) activeSpan).getLocalRootSpan()).thenReturn(localRootSpan);
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer);
    ApmTraceUtils.addTagsToRootSpan(TAGS);
    verify(localRootSpan, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, TAG_1), VALUE_1);
    verify(localRootSpan, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, TAG_2), VALUE_2);
  }

  @Test
  void testAddingTagsToRootSpanWhenActiveSpanIsNull() {
    final Tracer tracer = mock(Tracer.class);
    when(tracer.activeSpan()).thenReturn(null);
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer);
    Assertions.assertDoesNotThrow(() -> ApmTraceUtils.addTagsToRootSpan(TAGS));
  }

  @Test
  void testAddingTagsWithNullChecks() {
    final Span span = mock(Span.class);
    final Tracer tracer = mock(Tracer.class);
    when(tracer.activeSpan()).thenReturn(span);
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer);

    final UUID connectionID = UUID.randomUUID();
    final String jobId = UUID.randomUUID().toString();
    final Path jobRoot = Path.of("dev", "null");
    final Long attemptNumber = 2L;

    ApmTraceUtils.addTagsToTrace(connectionID, attemptNumber, jobId, jobRoot);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString());
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString());
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString());

    clearInvocations(span);
    ApmTraceUtils.addTagsToTrace(null, null, jobId, jobRoot);
    verify(span, never()).setTag(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString());
    verify(span, never()).setTag(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString());
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString());

    clearInvocations(span);
    ApmTraceUtils.addTagsToTrace(connectionID, null, jobId, null);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString());
    verify(span, never()).setTag(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString());
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId);
    verify(span, never()).setTag(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString());

    clearInvocations(span);
    ApmTraceUtils.addTagsToTrace(null, attemptNumber, jobId, null);
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString());
    verify(span, times(1)).setTag(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId);
    verify(span, never()).setTag(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), jobRoot.toString());
    verify(span, never()).setTag(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString());

    clearInvocations(span);
    ApmTraceUtils.addTagsToTrace((UUID) null, null, null, null);
    verify(span, never()).setTag(String.format(TAG_FORMAT, TAG_PREFIX, CONNECTION_ID_KEY), connectionID.toString());
    verify(span, never()).setTag(String.format(TAG_FORMAT, TAG_PREFIX, ATTEMPT_NUMBER_KEY), attemptNumber.toString());
    verify(span, never()).setTag(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ID_KEY), jobId);
    verify(span, never()).setTag(String.format(TAG_FORMAT, TAG_PREFIX, JOB_ROOT_KEY), jobRoot.toString());
  }

  @Test
  void testRecordErrorOnRootSpan() {
    final Span activeSpan = mock(Span.class, withSettings().extraInterfaces(MutableSpan.class));
    final Tracer tracer = mock(Tracer.class);
    final MutableSpan localRootSpan = mock(MutableSpan.class);
    final Throwable exception = mock(Throwable.class);
    when(tracer.activeSpan()).thenReturn(activeSpan);
    when(((MutableSpan) activeSpan).getLocalRootSpan()).thenReturn(localRootSpan);
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer);

    ApmTraceUtils.recordErrorOnRootSpan(exception);
    verify(activeSpan, times(1)).setTag(Tags.ERROR, true);
    verify(activeSpan, times(1)).log(Map.of(Fields.ERROR_OBJECT, exception));

    verify(localRootSpan, times(1)).setError(true);
    verify(localRootSpan, times(1)).setTag(DDTags.ERROR_MSG, exception.getMessage());
    verify(localRootSpan, times(1)).setTag(DDTags.ERROR_TYPE, exception.getClass().getName());
    final StringWriter expectedErrorString = new StringWriter();
    exception.printStackTrace(new PrintWriter(expectedErrorString));
    verify(localRootSpan, times(1)).setTag(DDTags.ERROR_STACK, expectedErrorString.toString());
  }

  @Test
  void testRecordErrorOnRootSpanWhenActiveSpanIsNull() {
    final Throwable exception = mock(Throwable.class);
    final Tracer tracer = mock(Tracer.class);
    when(tracer.activeSpan()).thenReturn(null);
    GlobalTracerTestUtil.setGlobalTracerUnconditionally(tracer);
    Assertions.assertDoesNotThrow(() -> ApmTraceUtils.recordErrorOnRootSpan(exception));
  }

}
