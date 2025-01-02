/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import com.google.common.annotations.VisibleForTesting
import dev.failsafe.Failsafe
import dev.failsafe.function.CheckedRunnable
import io.airbyte.commons.io.IOs
import io.airbyte.commons.io.LineGobbler
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.logging.MdcScope
import io.airbyte.config.WorkerDestinationConfig
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.LocalContainerConstants.ACCEPTED_MESSAGE_TYPES
import io.airbyte.workers.internal.LocalContainerConstants.IGNORED_EXIT_CODES
import io.airbyte.workers.internal.LocalContainerConstants.LOCAL_CONTAINER_RETRY_POLICY
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.BufferedWriter
import java.io.IOException
import java.io.OutputStreamWriter
import java.nio.file.Path
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

class LocalContainerAirbyteDestination(
  private val streamFactory: AirbyteStreamFactory,
  private val messageMetricsTracker: MessageMetricsTracker,
  private val messageWriterFactory: AirbyteMessageBufferedWriterFactory,
  private val destinationTimeoutMonitor: DestinationTimeoutMonitor,
  private val containerIOHandle: ContainerIOHandle,
  private val flushImmediately: Boolean = false,
) : AirbyteDestination {
  private val inputHasEnded = AtomicBoolean(false)
  private lateinit var messageIterator: Iterator<AirbyteMessage>
  private lateinit var writer: AirbyteMessageBufferedWriter

  companion object {
    const val CALLER = "airbyte-destination"

    val containerLogMdcBuilder: MdcScope.Builder =
      MdcScope.Builder()
        .setExtraMdcEntries(LogSource.DESTINATION.toMdc())
  }

  override fun close() {
    emitDestinationMessageCountMetrics()

    if (!inputHasEnded.get()) {
      notifyEndOfInput()
    }

    val terminationResult = containerIOHandle.terminate()
    if (terminationResult) {
      if (!IGNORED_EXIT_CODES.contains(exitValue)) {
        throw WorkerException("Destination process exit with code $exitValue. This warning is normal if the job was cancelled.")
      }
    } else {
      throw WorkerException("Destination has not terminated.  This warning is normal if the job was cancelled.")
    }
  }

  override fun start(
    destinationConfig: WorkerDestinationConfig,
    jobRoot: Path,
  ) {
    messageMetricsTracker.trackConnectionId(destinationConfig.connectionId)

    logger.info { "Running destination..." }

    // stdout logs are logged elsewhere since stdout also contains data
    LineGobbler.gobble(containerIOHandle.getErrInputStream(), { msg: String -> logger.error { msg } }, CALLER, containerLogMdcBuilder)

    // TODO are these the correct pipes?
    writer = messageWriterFactory.createWriter(BufferedWriter(OutputStreamWriter(containerIOHandle.getOutputStream(), Charsets.UTF_8)))

    Failsafe.with(LOCAL_CONTAINER_RETRY_POLICY).run(
      CheckedRunnable {
        messageIterator =
          streamFactory
            .create(IOs.newBufferedReader(containerIOHandle.getInputStream()))
            .filter { message: AirbyteMessage -> ACCEPTED_MESSAGE_TYPES.contains(message.type) }
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

  override fun isFinished(): Boolean {
    /*
     * As this check is done on every message read, it is important for this operation to be efficient.
     * Short circuit early to avoid checking the underlying process. Note: hasNext is blocking.
     */
    return !messageIterator.hasNext() && containerIOHandle.exitCodeExists()
  }

  override fun getExitValue(): Int = containerIOHandle.getExitCode()

  override fun attemptRead(): Optional<AirbyteMessage> {
    val m = if (messageIterator.hasNext()) messageIterator.next() else null
    m?.let {
      messageMetricsTracker.trackDestRead(m.type)
    }
    return Optional.ofNullable(m)
  }

  override fun cancel() {
    close()
  }

  @Throws(IOException::class)
  fun acceptWithNoTimeoutMonitor(message: AirbyteMessage) {
    // TODO also check if stdout file exists? or check if some other startup file exists?
    check(!inputHasEnded.get())

    writer.write(message)
    if (flushImmediately) {
      writer.flush()
    }
  }

  @Throws(IOException::class)
  private fun notifyEndOfInputWithNoTimeoutMonitor() {
    // TODO also check if stdout file exists? or check if some other startup file exists?
    check(!inputHasEnded.get())

    writer.flush()
    writer.close()
    setInputHasEnded(newValue = true)
  }

  @VisibleForTesting
  internal fun setInputHasEnded(newValue: Boolean) {
    inputHasEnded.set(newValue)
  }

  private fun emitDestinationMessageCountMetrics() {
    messageMetricsTracker.flushDestReadCountMetric()
    messageMetricsTracker.flushDestSentCountMetric()
  }
}
