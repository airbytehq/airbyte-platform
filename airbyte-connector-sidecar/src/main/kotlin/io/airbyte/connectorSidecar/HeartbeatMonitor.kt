/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorSidecar

import io.airbyte.api.client.ApiException
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import io.micronaut.context.annotation.Parameter
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Singleton
class HeartbeatMonitor(
  private val logContextFactory: SidecarLogContextFactory,
  private val workloadApiClient: WorkloadApiClient,
  @param:Parameter private val clock: Clock = Clock.systemUTC(),
  @param:Parameter private val executorService: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(),
) {
  private val abort = AtomicBoolean(false)
  private val heartbeatStarted = AtomicBoolean(false)

  fun shouldAbort(): Boolean = abort.get()

  fun startHeartbeatThread(sidecarInput: SidecarInput) {
    if (heartbeatStarted.getAndSet(true)) {
      return
    }
    val heartbeatTimeoutDuration = Duration.ofMinutes(5)
    val heartbeatInterval = Duration.ofSeconds(30)

    val heartbeatTask =
      HeartbeatTask(
        sidecarInput,
        logContextFactory,
        workloadApiClient,
        clock,
        heartbeatTimeoutDuration,
        abort,
      )

    executorService.scheduleAtFixedRate(
      heartbeatTask,
      0,
      heartbeatInterval.toMillis(),
      TimeUnit.MILLISECONDS,
    )
  }

  fun stopHeartbeatThread() {
    executorService.shutdownNow()
    heartbeatStarted.set(false)
  }

  internal class HeartbeatTask(
    private val sidecarInput: SidecarInput,
    private val logContextFactory: SidecarLogContextFactory,
    private val workloadApiClient: WorkloadApiClient,
    private val clock: Clock,
    private val heartbeatTimeoutDuration: Duration,
    private val abort: AtomicBoolean,
  ) : Runnable {
    private val logger = KotlinLogging.logger {}

    @Volatile
    private var lastSuccessfulHeartbeat: Instant = clock.instant()

    override fun run() {
      if (abort.get()) {
        return
      }
      withLoggingContext(logContextFactory.create(sidecarInput.logPath)) {
        try {
          logger.debug { "Sending workload heartbeat" }
          workloadApiClient.workloadHeartbeat(WorkloadHeartbeatRequest(sidecarInput.workloadId))

          lastSuccessfulHeartbeat = clock.instant()
        } catch (e: Exception) {
          handleHeartbeatException(e)
        }
      }
    }

    private fun handleHeartbeatException(e: Exception) {
      when {
        e is ApiException && e.statusCode == HttpStatus.GONE.code -> {
          logger.warn(e) { "Cancelling job, workload is in a terminal state" }
          abort.set(true)
        }
        Duration.between(lastSuccessfulHeartbeat, clock.instant()) > heartbeatTimeoutDuration -> {
          logger.warn(e) {
            "Have not been able to update heartbeat for more than the timeout duration, shutting down heartbeat"
          }
          abort.set(true)
        }
        else -> {
          logger.warn(e) { "Error while trying to heartbeat, re-trying" }
        }
      }
    }
  }
}
