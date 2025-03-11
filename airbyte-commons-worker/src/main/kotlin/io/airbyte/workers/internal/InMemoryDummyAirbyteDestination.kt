/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import io.airbyte.config.WorkerDestinationConfig
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.Jsons
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.file.Path
import java.util.Optional
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger { }

class InMemoryDummyAirbyteDestination : AirbyteDestination {
  private var size: Long = 0
  val counter = AtomicLong(0)

  override fun close() {
    logger.info { "TOTAL SIZE {$size}" }
  }

  override fun start(
    destinationConfig: WorkerDestinationConfig?,
    jobRoot: Path?,
  ) {
  }

  override fun accept(message: AirbyteMessage?) {
    counter.incrementAndGet()
    val serialize = Jsons.serialize(message)
    size += serialize.length
  }

  override fun notifyEndOfInput() {
  }

  override fun isFinished(): Boolean = counter.get() > MAX_RECORDS

  override fun getExitValue(): Int = 0

  override fun attemptRead(): Optional<AirbyteMessage> = Optional.empty()

  override fun cancel() {
  }
}
