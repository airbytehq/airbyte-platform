/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import dev.failsafe.Failsafe
import dev.failsafe.function.CheckedRunnable
import io.airbyte.commons.io.IOs
import io.airbyte.commons.io.LineGobbler
import io.airbyte.commons.logging.MdcScope
import io.airbyte.config.WorkerSourceConfig
import io.airbyte.container.orchestrator.tracker.MessageMetricsTracker
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.internal.MessageOrigin
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.file.Path
import java.time.Instant
import java.util.Optional
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}
private const val CALLER = "airbyte-source"

// Log every 100000 heartbeats/messages when diagnostic logging is enabled
private const val HEARTBEAT_LOG_INTERVAL = 100000L
private const val MESSAGE_LOG_INTERVAL = 100000L

class LocalContainerAirbyteSource(
  private val heartbeatMonitor: HeartbeatMonitor,
  private val streamFactory: AirbyteStreamFactory,
  private val messageMetricsTracker: MessageMetricsTracker,
  private val containerIOHandle: ContainerIOHandle,
  private val containerLogMdcBuilder: MdcScope.Builder,
  private val diagnosticLogsEnabled: Boolean = false,
  private val exitCodeWaitSeconds: Long = EXIT_CODE_WAIT_SECONDS,
) : AirbyteSource {
  companion object {
    // Fallback exit code when the source container is killed (e.g., OOM) and no exit code file is written
    const val FALLBACK_EXIT_CODE = 1

    // Time to wait for exit code file after pipe closes (to handle race condition between pipe close and exit code write)
    const val EXIT_CODE_WAIT_SECONDS = 10L
  }

  private lateinit var messageIterator: Iterator<AirbyteMessage>

  // Tracks when the output stream iterator is exhausted (pipes closed).
  // This is important for detecting OOM kills where the container dies without writing an exit code file.
  @Volatile
  private var outputStreamExhausted = false

  // Diagnostic counters for heartbeat debugging
  private val messageCount = AtomicLong(0)
  private val heartbeatCount = AtomicLong(0)

  @Volatile
  private var lastHeartbeatTime: Instant? = null

  @Volatile
  private var startTime: Instant? = null

  override fun close() {
    messageMetricsTracker.flushSourceReadCountMetric()
    val terminationResult = containerIOHandle.terminate()
    if (terminationResult) {
      if (!LocalContainerConstants.IGNORED_EXIT_CODES.contains(exitValue)) {
        throw WorkerException("Source process exit with code $exitValue. This warning is normal if the job was cancelled.")
      }
    } else {
      throw WorkerException("Source has not terminated.  This warning is normal if the job was cancelled.")
    }
  }

  override fun start(
    sourceConfig: WorkerSourceConfig,
    jobRoot: Path?,
    connectionId: UUID?,
  ) {
    // TODO check if stdout file exists? or check if some other startup file exists?
    startTime = Instant.now()
    if (diagnosticLogsEnabled) {
      logger.info { "LocalContainerAirbyteSource starting for connectionId=$connectionId" }
    }

    messageMetricsTracker.trackConnectionId(connectionId)

    // stdout logs are logged elsewhere since stdout also contains data
    LineGobbler.gobble(containerIOHandle.getErrInputStream(), { msg: String -> logger.error { msg } }, CALLER, containerLogMdcBuilder)

    Failsafe.with(LocalContainerConstants.LOCAL_CONTAINER_RETRY_POLICY).run(
      CheckedRunnable {
        messageIterator =
          streamFactory
            .create(IOs.newBufferedReader(containerIOHandle.getInputStream()), MessageOrigin.SOURCE)
            .peek { message: AirbyteMessage ->
              val count = messageCount.incrementAndGet()
              if (shouldBeat(message.type)) {
                heartbeatMonitor.beat()
                val hbCount = heartbeatCount.incrementAndGet()
                lastHeartbeatTime = Instant.now()
                // Log periodically to track progress when diagnostic logs are enabled
                if (diagnosticLogsEnabled && hbCount % HEARTBEAT_LOG_INTERVAL == 0L) {
                  logger.info { "Source heartbeat progress: heartbeatCount=$hbCount, totalMessages=$count, messageType=${message.type}" }
                }
              }
              // Log periodically when diagnostic logs are enabled
              if (diagnosticLogsEnabled && count % MESSAGE_LOG_INTERVAL == 0L) {
                logger.info {
                  "Source message progress: totalMessages=$count, heartbeatCount=${heartbeatCount.get()}, lastHeartbeatTime=$lastHeartbeatTime"
                }
              }
            }.filter { message: AirbyteMessage -> LocalContainerConstants.ACCEPTED_MESSAGE_TYPES.contains(message.type) }
            .iterator()
      },
    )
    if (diagnosticLogsEnabled) {
      logger.info { "LocalContainerAirbyteSource message iterator initialized" }
    }
  }

  override val isFinished: Boolean
    /**
     * As this check is done on every message read, it is important for this operation to be efficient.
     * Short circuit early to avoid checking the underlying process. Note: hasNext is blocking.
     *
     * We consider the source finished in one case:
     * 1. The iterator has no more messages
     */
    get() {
      if (outputStreamExhausted) {
        return true
      }
      val hasNext = messageIterator.hasNext()
      if (!hasNext) {
        outputStreamExhausted = true
        return true
      }
      return false
    }

  override val exitValue: Int
    /**
     * Returns the exit code of the source container.
     * If no exit code file exists but the output stream was exhausted (pipes closed),
     * this waits briefly for the exit code file to appear (to handle the race condition
     * between pipe close and exit code file write), then returns a fallback exit code
     * if the file still doesn't exist.
     */
    get() {
      if (containerIOHandle.exitCodeExists()) {
        return containerIOHandle.getExitCode()
      }
      if (outputStreamExhausted) {
        // Wait for exit code file - there's a race condition where the pipe closes
        // before the shell script writes the exit code file
        try {
          Thread.sleep(exitCodeWaitSeconds * 1000)
        } catch (e: InterruptedException) {
          Thread.currentThread().interrupt()
        }
        if (containerIOHandle.exitCodeExists()) {
          return containerIOHandle.getExitCode()
        }
        logger.error {
          "No exit code file found after waiting ${exitCodeWaitSeconds}s. Container may have been killed (OOM or other signal). Returning fallback exit code $FALLBACK_EXIT_CODE."
        }
        return FALLBACK_EXIT_CODE
      }
      return containerIOHandle.getExitCode() // This will throw IllegalStateException as expected
    }

  override fun attemptRead(): Optional<AirbyteMessage> {
    val m = if (messageIterator.hasNext()) messageIterator.next() else null
    m?.let {
      messageMetricsTracker.trackSourceRead(it.type)
    }
    return Optional.ofNullable(m)
  }

  override fun cancel() {
    close()
  }

  private fun shouldBeat(airbyteMessageType: AirbyteMessage.Type): Boolean =
    airbyteMessageType == AirbyteMessage.Type.STATE || airbyteMessageType == AirbyteMessage.Type.RECORD
}
