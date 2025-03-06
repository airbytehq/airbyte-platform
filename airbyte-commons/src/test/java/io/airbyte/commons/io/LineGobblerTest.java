/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.io;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.logging.MdcScope.Builder;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class LineGobblerTest {

  @Test
  @SuppressWarnings("unchecked")
  void readAllLines() {
    final Consumer<String> consumer = Mockito.mock(Consumer.class);
    final InputStream is = new ByteArrayInputStream("test\ntest2\n".getBytes(StandardCharsets.UTF_8));
    final ExecutorService executor = spy(MoreExecutors.newDirectExecutorService());

    executor.submit(new LineGobbler(is, consumer, executor, ImmutableMap.of()));

    Mockito.verify(consumer).accept("test");
    Mockito.verify(consumer).accept("test2");
    Mockito.verify(executor).shutdown();
  }

  @Test
  @SuppressWarnings("unchecked")
  void shutdownOnSuccess() {
    final Consumer<String> consumer = Mockito.mock(Consumer.class);
    final InputStream is = new ByteArrayInputStream("test\ntest2\n".getBytes(StandardCharsets.UTF_8));
    final ExecutorService executor = spy(MoreExecutors.newDirectExecutorService());

    executor.submit(new LineGobbler(is, consumer, executor, ImmutableMap.of()));

    Mockito.verify(consumer, times(2)).accept(anyString());
    Mockito.verify(executor).shutdown();
  }

  @Test
  @SuppressWarnings("unchecked")
  void shutdownOnError() {
    final Consumer<String> consumer = Mockito.mock(Consumer.class);
    Mockito.doThrow(RuntimeException.class).when(consumer).accept(anyString());
    final InputStream is = new ByteArrayInputStream("test\ntest2\n".getBytes(StandardCharsets.UTF_8));
    final ExecutorService executor = spy(MoreExecutors.newDirectExecutorService());

    executor.submit(new LineGobbler(is, consumer, executor, ImmutableMap.of()));

    verify(consumer).accept(anyString());
    Mockito.verify(executor).shutdown();
  }

  @Test
  void readFromNullInputStream() {
    final Consumer<String> consumer = Mockito.mock(Consumer.class);
    final MdcScope.Builder mdcBuilder = Mockito.mock(Builder.class);
    final InputStream is = null;

    assertDoesNotThrow(() -> {
      LineGobbler.gobble(is, consumer);
      LineGobbler.gobble(is, consumer, mdcBuilder);
      LineGobbler.gobble(is, consumer, "test", mdcBuilder);
    });

    verify(consumer, times(0)).accept(anyString());
  }

}
