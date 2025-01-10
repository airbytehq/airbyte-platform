/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import dev.failsafe.Failsafe
import dev.failsafe.function.CheckedRunnable
import io.airbyte.commons.io.IOs
import io.airbyte.commons.io.LineGobbler
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.logging.MdcScope
import io.airbyte.config.WorkerSourceConfig
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.LocalContainerConstants.ACCEPTED_MESSAGE_TYPES
import io.airbyte.workers.internal.LocalContainerConstants.IGNORED_EXIT_CODES
import io.airbyte.workers.internal.LocalContainerConstants.LOCAL_CONTAINER_RETRY_POLICY
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

private val logger = KotlinLogging.logger {}

class LocalContainerAirbyteSource(
  private val heartbeatMonitor: HeartbeatMonitor,
  private val streamFactory: AirbyteStreamFactory,
  private val messageMetricsTracker: MessageMetricsTracker,
  private val containerIOHandle: ContainerIOHandle,
) : AirbyteSource {
  private lateinit var messageIterator: Iterator<AirbyteMessage>

  companion object {
    const val CALLER = "airbyte-source"
    val containerLogMdcBuilder: MdcScope.Builder =
      MdcScope.Builder()
        .setExtraMdcEntries(LogSource.SOURCE.toMdc())
  }

  override fun close() {
    messageMetricsTracker.flushSourceReadCountMetric()
    val terminationResult = containerIOHandle.terminate()
    if (terminationResult) {
      if (!IGNORED_EXIT_CODES.contains(exitValue)) {
        throw WorkerException("Source process exit with code $exitValue. This warning is normal if the job was cancelled.")
      }
    } else {
      throw WorkerException("Source has not terminated.  This warning is normal if the job was cancelled.")
    }
  }

  override fun start(
    sourceConfig: WorkerSourceConfig,
    jobRoot: Path,
    connectionId: UUID,
  ) {
    // TODO check if stdout file exists? or check if some other startup file exists?

    messageMetricsTracker.trackConnectionId(connectionId)

    // stdout logs are logged elsewhere since stdout also contains data
    LineGobbler.gobble(containerIOHandle.getErrInputStream(), { msg: String -> logger.error { msg } }, CALLER, containerLogMdcBuilder)

    Failsafe.with(LOCAL_CONTAINER_RETRY_POLICY).run(
      CheckedRunnable {
        messageIterator =
          streamFactory.create(IOs.newBufferedReader(containerIOHandle.getInputStream()))
            .peek { message: AirbyteMessage ->
              if (shouldBeat(message.type)) {
                heartbeatMonitor.beat()
              }
            }
            .filter { message: AirbyteMessage -> ACCEPTED_MESSAGE_TYPES.contains(message.type) }
            .iterator()
      },
    )
  }

  override fun isFinished(): Boolean {
    /*
     * As this check is done on every message read, it is important for this operation to be efficient.
     * Short circuit early to avoid checking the underlying process. note: hasNext is blocking.
     */
    return !messageIterator.hasNext() && containerIOHandle.exitCodeExists()
  }

  override fun getExitValue(): Int {
    return containerIOHandle.getExitCode()
  }

  override fun attemptRead(): Optional<AirbyteMessage> {
    val m = if (messageIterator.hasNext()) messageIterator.next() else null
    m?.let {
      messageMetricsTracker.trackSourceRead(m.type)
    }
    return Optional.ofNullable(m)
  }

  override fun cancel() {
    close()
  }

  private fun shouldBeat(airbyteMessageType: AirbyteMessage.Type): Boolean {
    return airbyteMessageType == AirbyteMessage.Type.STATE || airbyteMessageType == AirbyteMessage.Type.RECORD
  }
}
