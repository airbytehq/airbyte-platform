/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.io

import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.commons.logging.MdcScope
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.function.Consumer

internal class LineGobblerTest {
  @Test
  fun readAllLines() {
    val consumer: Consumer<String> = mockk(relaxed = true)
    val `is`: InputStream = ByteArrayInputStream("test\ntest2\n".toByteArray(StandardCharsets.UTF_8))
    val executor: ExecutorService =
      mockk<ExecutorService> {
        every { submit(any<VoidCallable>()) } answers {
          (firstArg() as VoidCallable).call()
          mockk<Future<Void?>>()
        }
        every { shutdown() } returns Unit
      }

    executor.submit<Void?>(LineGobbler(`is`, consumer, executor, emptyMap()))

    verify(exactly = 1) { consumer.accept("test") }
    verify(exactly = 1) { consumer.accept("test2") }
    verify(exactly = 1) { executor.shutdown() }
  }

  @Test
  fun shutdownOnSuccess() {
    val consumer: Consumer<String> = mockk(relaxed = true)
    val `is`: InputStream = ByteArrayInputStream("test\ntest2\n".toByteArray(StandardCharsets.UTF_8))
    val executor: ExecutorService =
      mockk<ExecutorService> {
        every { submit(any<VoidCallable>()) } answers {
          (firstArg() as VoidCallable).call()
          mockk<Future<Void?>>()
        }
        every { shutdown() } returns Unit
      }

    executor.submit<Void?>(LineGobbler(`is`, consumer, executor, emptyMap()))

    verify(exactly = 2) { consumer.accept(any()) }
    verify(exactly = 1) { executor.shutdown() }
  }

  @Test
  fun shutdownOnError() {
    val consumer: Consumer<String> =
      mockk(relaxed = true) {
        every { accept(any()) } throws RuntimeException()
      }
    val `is`: InputStream = ByteArrayInputStream("test\ntest2\n".toByteArray(StandardCharsets.UTF_8))
    val executor: ExecutorService =
      mockk<ExecutorService> {
        every { submit(any<VoidCallable>()) } answers {
          (firstArg() as VoidCallable).call()
          mockk<Future<Void?>>()
        }
        every { shutdown() } returns Unit
      }
    executor.submit<Void?>(LineGobbler(`is`, consumer, executor, emptyMap()))

    verify(exactly = 1) { consumer.accept(any()) }
    verify(exactly = 1) { executor.shutdown() }
  }

  @Test
  fun readFromNullInputStream() {
    val consumer: Consumer<String> = mockk(relaxed = true)
    val mdcBuilder = mockk<MdcScope.Builder>()
    val `is`: InputStream? = null

    assertDoesNotThrow(
      {
        LineGobbler.gobble(`is`, consumer, "test", mdcBuilder, Executors.newSingleThreadExecutor())
      },
    )

    verify(exactly = 0) { consumer.accept(any()) }
  }
}
