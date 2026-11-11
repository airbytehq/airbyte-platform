/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import dev.failsafe.Failsafe
import dev.failsafe.FailsafeException
import dev.failsafe.RetryPolicy
import io.micronaut.data.connection.exceptions.ConnectionException
import org.jooq.exception.DataAccessException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf
import org.junit.jupiter.api.assertThrows
import java.sql.SQLException
import java.sql.SQLRecoverableException
import java.sql.SQLTransientConnectionException
import java.time.Duration

internal class WorkloadPurgeRetryPolicyTest {
  private val productionPolicy = WorkloadPurgeRetryPolicy().workloadPurgeRetryPolicy()
  private val retryPolicy =
    RetryPolicy
      .builder(productionPolicy.config)
      .withDelayFn { Duration.ZERO }
      .withMaxAttempts(3)
      .build()

  @Test
  fun `temporary connection SQL states and JDBC connection failures recover`() {
    val chained = SQLException("driver wrapper").apply { nextException = SQLException("disconnected", "08006") }
    val failures =
      listOf(
        SQLException("disconnected", "08006"),
        SQLException("shutdown", "57P01"),
        SQLException("crash", "57P02"),
        SQLException("starting", "57P03"),
        SQLTransientConnectionException("temporarily unavailable"),
        SQLRecoverableException("reconnect"),
        SQLException("Connection is closed"),
        chained,
      )
    failures.forEach { sqlException ->
      var attempts = 0
      val failure = DataAccessException("query failed", sqlException)

      val result =
        Failsafe.with(retryPolicy).get { _ ->
          attempts++
          if (attempts == 1) throw failure
          7
        }

      assertEquals(7, result, "$sqlException")
      assertEquals(2, attempts, "$sqlException")
    }
  }

  @Test
  fun `permanent or unrecognized failures propagate without retry`() {
    val failures =
      listOf(
        DataAccessException("constraint", SQLException("foreign key", "23503")),
        DataAccessException("data", SQLException("bad input", "22P02")),
        DataAccessException("auth", SQLException("credentials", "28P01")),
        DataAccessException("syntax", SQLException("syntax", "42601")),
        DataAccessException("cancelled", SQLException("cancelled", "57014")),
        DataAccessException("dropped", SQLException("database dropped", "57P04")),
        DataAccessException("unknown", SQLException("connection seems closed")),
        ConnectionException("Connection is closed"),
        ConnectionException("unknown", SQLException("other failure")),
        DataAccessException("permanent", SQLTransientConnectionException("Connection is closed", "23503")),
        DataAccessException("permanent", SQLRecoverableException("Connection is closed", "22P02")),
        DataAccessException("permanent", SQLException("Connection is closed", "42601")),
        IllegalStateException("Connection is closed"),
      )
    failures.forEach { failure ->
      var attempts = 0

      val thrown =
        assertThrows<RuntimeException> {
          Failsafe.with(retryPolicy).get<Int> { _ ->
            attempts++
            throw failure
          }
        }

      assertSame(failure, thrown)
      assertEquals(1, attempts, "$failure")
    }
  }

  @Test
  fun `cleanup errors cannot hide a permanent primary SQL failure`() {
    val primary = SQLException("constraint", "23503")
    primary.addSuppressed(SQLException("Connection is closed"))
    primary.nextException = SQLException("disconnected", "08006")
    val failure = DataAccessException("query failed", primary)
    var attempts = 0

    assertSame(
      failure,
      assertThrows<DataAccessException> {
        Failsafe.with(retryPolicy).get<Int> { _ ->
          attempts++
          throw failure
        }
      },
    )
    assertEquals(1, attempts)
  }

  @Test
  fun `retry exhaustion propagates the last connection failure`() {
    var attempts = 0
    val failure = ConnectionException("closed", SQLException("Connection is closed"))

    assertSame(
      failure,
      assertThrows<ConnectionException> {
        Failsafe.with(retryPolicy).get<Int> { _ ->
          attempts++
          throw failure
        }
      },
    )
    assertEquals(3, attempts)
  }

  @Test
  fun `interruption aborts retries and preserves the thread interrupt flag`() {
    var attempts = 0
    try {
      val failure =
        assertThrows<FailsafeException> {
          Failsafe.with(retryPolicy).get<Int> { _ ->
            attempts++
            Thread.currentThread().interrupt()
            throw ConnectionException("closed", SQLException("Connection is closed"))
          }
        }

      assertInstanceOf<InterruptedException>(failure.cause)
      assertTrue(Thread.currentThread().isInterrupted)
      assertEquals(1, attempts)
    } finally {
      Thread.interrupted()
    }
  }

  @Test
  fun `production retries use increasing bounded waits for up to ten minutes`() {
    val config = productionPolicy.config
    assertEquals(-1, config.maxRetries)
    assertEquals(Duration.ofMinutes(10), config.maxDuration)
    assertEquals(Duration.ofSeconds(1), config.delay)
    assertEquals(Duration.ofSeconds(30), config.maxDelay)
    assertEquals(2.0, config.delayFactor)
  }
}
