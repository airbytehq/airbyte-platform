/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.io

import com.google.common.collect.ImmutableMap
import com.google.common.util.concurrent.MoreExecutors
import io.airbyte.commons.logging.MdcScope
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.function.Consumer

internal class LineGobblerTest {
  @Test
  fun readAllLines() {
    val consumer: Consumer<String> = Mockito.mock()
    val `is`: InputStream = ByteArrayInputStream("test\ntest2\n".toByteArray(StandardCharsets.UTF_8))
    val executor: ExecutorService = Mockito.spy(MoreExecutors.newDirectExecutorService())

    executor.submit<Void?>(LineGobbler(`is`, consumer, executor, ImmutableMap.of<String, String>()))

    Mockito.verify(consumer).accept("test")
    Mockito.verify(consumer).accept("test2")
    Mockito.verify(executor).shutdown()
  }

  @Test
  fun shutdownOnSuccess() {
    val consumer: Consumer<String> = Mockito.mock()
    val `is`: InputStream = ByteArrayInputStream("test\ntest2\n".toByteArray(StandardCharsets.UTF_8))
    val executor: ExecutorService = Mockito.spy(MoreExecutors.newDirectExecutorService())

    executor.submit<Void?>(LineGobbler(`is`, consumer, executor, ImmutableMap.of<String, String>()))

    Mockito.verify(consumer, Mockito.times(2)).accept(ArgumentMatchers.anyString())
    Mockito.verify(executor).shutdown()
  }

  @Test
  fun shutdownOnError() {
    val consumer: Consumer<String> = Mockito.mock()
    Mockito.doThrow(RuntimeException::class.java).`when`(consumer).accept(ArgumentMatchers.anyString())
    val `is`: InputStream = ByteArrayInputStream("test\ntest2\n".toByteArray(StandardCharsets.UTF_8))
    val executor: ExecutorService = Mockito.spy(MoreExecutors.newDirectExecutorService())

    executor.submit<Void?>(LineGobbler(`is`, consumer, executor, ImmutableMap.of<String, String>()))

    Mockito.verify(consumer).accept(ArgumentMatchers.anyString())
    Mockito.verify(executor).shutdown()
  }

  @Test
  fun readFromNullInputStream() {
    val consumer: Consumer<String> = Mockito.mock()
    val mdcBuilder = Mockito.mock(MdcScope.Builder::class.java)
    val `is`: InputStream? = null

    Assertions.assertDoesNotThrow(
      Executable {
        LineGobbler.gobble(`is`, consumer, "test", mdcBuilder, Executors.newSingleThreadExecutor())
      },
    )

    Mockito.verify(consumer, Mockito.times(0)).accept(ArgumentMatchers.anyString())
  }
}
