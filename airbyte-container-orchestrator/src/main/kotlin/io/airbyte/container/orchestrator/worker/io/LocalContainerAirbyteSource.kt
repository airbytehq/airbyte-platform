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
import java.util.Optional
import java.util.UUID

private val logger = KotlinLogging.logger {}
private const val CALLER = "airbyte-source"

class LocalContainerAirbyteSource(
  private val heartbeatMonitor: HeartbeatMonitor,
  private val streamFactory: AirbyteStreamFactory,
  private val messageMetricsTracker: MessageMetricsTracker,
  private val containerIOHandle: ContainerIOHandle,
  private val containerLogMdcBuilder: MdcScope.Builder,
) : AirbyteSource {
  private lateinit var messageIterator: Iterator<AirbyteMessage>

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

    messageMetricsTracker.trackConnectionId(connectionId)

    // stdout logs are logged elsewhere since stdout also contains data
    LineGobbler.gobble(containerIOHandle.getErrInputStream(), { msg: String -> logger.error { msg } }, CALLER, containerLogMdcBuilder)

    Failsafe.with(LocalContainerConstants.LOCAL_CONTAINER_RETRY_POLICY).run(
      CheckedRunnable {
        messageIterator =
          streamFactory
            .create(IOs.newBufferedReader(containerIOHandle.getInputStream()), MessageOrigin.SOURCE)
            .peek { message: AirbyteMessage ->
              if (shouldBeat(message.type)) {
                heartbeatMonitor.beat()
              }
            }.filter { message: AirbyteMessage -> LocalContainerConstants.ACCEPTED_MESSAGE_TYPES.contains(message.type) }
            .iterator()
      },
    )
  }

  override val isFinished: Boolean
    /*
     * As this check is done on every message read, it is important for this operation to be efficient.
     * Short circuit early to avoid checking the underlying process. note: hasNext is blocking.
     */
    get() = !messageIterator.hasNext() && containerIOHandle.exitCodeExists()

  override val exitValue: Int
    get() = containerIOHandle.getExitCode()

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
