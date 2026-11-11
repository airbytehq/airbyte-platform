/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import dev.failsafe.RetryPolicy
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.SQLException
import java.sql.SQLRecoverableException
import java.sql.SQLTransientConnectionException
import java.time.Duration
import java.util.Collections
import java.util.IdentityHashMap

private val log = KotlinLogging.logger {}

@Factory
class WorkloadPurgeRetryPolicy {
  @Singleton
  @Named("workloadPurgeRetryPolicy")
  fun workloadPurgeRetryPolicy(): RetryPolicy<Int> =
    RetryPolicy
      .builder<Int>()
      .handleIf(::isTemporaryDisconnect)
      .withMaxRetries(-1)
      .withMaxDuration(Duration.ofMinutes(10))
      .withBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30))
      .onRetry { event ->
        log.warn(event.lastException) { "Retrying workload purge batch after temporary database disconnection; attempt ${event.attemptCount}" }
      }.build()

  private fun isTemporaryDisconnect(failure: Throwable): Boolean {
    val pending = ArrayDeque<Throwable>()
    pending.add(failure)
    val visited = Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())
    var disconnected = false
    while (pending.isNotEmpty()) {
      val current = pending.removeFirst()
      if (!visited.add(current)) {
        continue
      }

      if (current is SQLException) {
        val sqlState: String? = current.sqlState
        if (!sqlState.isNullOrEmpty()) {
          if (sqlState.startsWith("08") || sqlState in setOf("57P01", "57P02", "57P03")) {
            disconnected = true
          } else {
            return false
          }
        } else if (current is SQLTransientConnectionException || current is SQLRecoverableException) {
          disconnected = true
        } else if (current.javaClass == SQLException::class.java && current.message == "Connection is closed") {
          disconnected = true
        }
        current.nextException?.let(pending::add)
      }
      current.cause?.let(pending::add)
    }
    return disconnected
  }
}
