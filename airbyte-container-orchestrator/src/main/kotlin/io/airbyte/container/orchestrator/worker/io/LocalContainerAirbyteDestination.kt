/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import dev.failsafe.Failsafe
import dev.failsafe.function.CheckedRunnable
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.io.IOs
import io.airbyte.commons.io.LineGobbler
import io.airbyte.commons.logging.MdcScope
import io.airbyte.config.WorkerDestinationConfig
import io.airbyte.container.orchestrator.tracker.MessageMetricsTracker
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.internal.MessageOrigin
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.BufferedWriter
import java.io.IOException
import java.io.OutputStreamWriter
import java.nio.file.Path
import java.util.Optional
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}
private const val CALLER = "airbyte-destination"

class LocalContainerAirbyteDestination(
  private val streamFactory: AirbyteStreamFactory,
  private val messageMetricsTracker: MessageMetricsTracker,
  private val messageWriterFactory: AirbyteMessageBufferedWriterFactory,
  private val destinationTimeoutMonitor: DestinationTimeoutMonitor,
  private val containerIOHandle: ContainerIOHandle,
  private val containerLogMdcBuilder: MdcScope.Builder,
  private val metricClient: MetricClient,
  private val workspaceId: UUID? = null,
  private val connectionId: UUID? = null,
  private val dockerImage: String? = null,
  private val flushImmediately: Boolean = false,
  private val exitCodeWaitSeconds: Long = EXIT_CODE_WAIT_SECONDS,
) : AirbyteDestination {
  companion object {
    // Fallback exit code when the destination container is killed (e.g., OOM) and no exit code file is written
    const val FALLBACK_EXIT_CODE = 1

    // Time to wait for exit code file after pipe closes (to handle race condition between pipe close and exit code write)
    const val EXIT_CODE_WAIT_SECONDS = 10L
  }

  private val inputHasEnded = AtomicBoolean(false)
  private lateinit var messageIterator: Iterator<AirbyteMessage>
  private lateinit var writer: AirbyteMessageBufferedWriter<AirbyteMessage>

  // Tracks when the output stream iterator is exhausted (pipes closed).
  // This is important for detecting OOM kills where the container dies without writing an exit code file.
  @Volatile
  private var outputStreamExhausted = false

  override fun close() {
    emitDestinationMessageCountMetrics()

    if (!inputHasEnded.get()) {
      notifyEndOfInput()
    }

    val terminationResult = containerIOHandle.terminate()
    if (terminationResult) {
      if (!LocalContainerConstants.IGNORED_EXIT_CODES.contains(exitValue)) {
        LocalContainerConstants.emitExitCodeMetric(metricClient, "destination", exitValue, workspaceId, connectionId, dockerImage)
        throw WorkerException("Destination process exit with code $exitValue. This warning is normal if the job was cancelled.")
      }
    } else {
      throw WorkerException("Destination has not terminated.  This warning is normal if the job was cancelled.")
    }
  }

  @Suppress("UNCHECKED_CAST")
  override fun start(
    destinationConfig: WorkerDestinationConfig,
    jobRoot: Path,
  ) {
    messageMetricsTracker.trackConnectionId(destinationConfig.connectionId)

    logger.info { "Running destination..." }

    // stdout logs are logged elsewhere since stdout also contains data
    LineGobbler.gobble(containerIOHandle.getErrInputStream(), { msg: String -> logger.error { msg } }, CALLER, containerLogMdcBuilder)

    // TODO are these the correct pipes?
    writer =
      messageWriterFactory.createWriter(
        BufferedWriter(OutputStreamWriter(containerIOHandle.getOutputStream(), Charsets.UTF_8)),
      ) as AirbyteMessageBufferedWriter<AirbyteMessage>

    Failsafe.with(LocalContainerConstants.LOCAL_CONTAINER_RETRY_POLICY).run(
      CheckedRunnable {
        messageIterator =
          streamFactory
            .create(IOs.newBufferedReader(containerIOHandle.getInputStream()), MessageOrigin.DESTINATION)
            .filter { message: AirbyteMessage -> LocalContainerConstants.ACCEPTED_MESSAGE_TYPES.contains(message.type) }
            .iterator()
      },
    )
  }

  override fun accept(message: AirbyteMessage) {
    messageMetricsTracker.trackDestSent(message.type)
    destinationTimeoutMonitor.startAcceptTimer()
    acceptWithNoTimeoutMonitor(message)
    destinationTimeoutMonitor.resetAcceptTimer()
  }

  override fun notifyEndOfInput() {
    destinationTimeoutMonitor.startNotifyEndOfInputTimer()
    try {
      notifyEndOfInputWithNoTimeoutMonitor()
    } catch (e: IOException) {
      logger.warn(e) { "Attempted to close a destination which is already closed." }
    }
    destinationTimeoutMonitor.resetNotifyEndOfInputTimer()
  }

  override val isFinished: Boolean
    /**
     * As this check is done on every message read, it is important for this operation to be efficient.
     * Short circuit early to avoid checking the underlying process. Note: hasNext is blocking.
     *
     * We consider the destination finished in one case:
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
     * Returns the exit code of the destination container.
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
      messageMetricsTracker.trackDestRead(it.type)
    }
    return Optional.ofNullable(m)
  }

  override fun cancel() {
    close()
  }

  fun acceptWithNoTimeoutMonitor(message: AirbyteMessage) {
    // TODO also check if stdout file exists? or check if some other startup file exists?
    check(!inputHasEnded.get())

    writer.write(message)
    if (flushImmediately) {
      writer.flush()
    }
  }

  private fun notifyEndOfInputWithNoTimeoutMonitor() {
    // TODO also check if stdout file exists? or check if some other startup file exists?
    check(!inputHasEnded.get())

    writer.flush()
    writer.close()
    setInputHasEnded(newValue = true)
  }

  @InternalForTesting
  internal fun setInputHasEnded(newValue: Boolean) {
    inputHasEnded.set(newValue)
  }

  private fun emitDestinationMessageCountMetrics() {
    messageMetricsTracker.flushDestReadCountMetric()
    messageMetricsTracker.flushDestSentCountMetric()
  }
}
