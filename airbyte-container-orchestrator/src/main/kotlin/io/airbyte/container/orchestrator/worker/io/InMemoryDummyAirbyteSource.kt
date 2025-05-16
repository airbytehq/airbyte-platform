/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.config.WorkerSourceConfig
import io.airbyte.protocol.models.Jsons
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import org.joda.time.Instant
import java.nio.file.Path
import java.util.Optional
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

const val MAX_RECORDS = 50_000_000
const val STRING_VALUE = "valuevaluevaluevaluevalue1"
const val FIELD_1 = "field1"
const val FIELD_2 = "field2"
const val FIELD_3 = "field3"
const val FIELD_4 = "field4"
const val FIELD_5 = "field5"

private const val STREAM = "stream1"

class InMemoryDummyAirbyteSource : AirbyteSource {
  val counter = AtomicLong(0)

  override fun close() {
    counter.set(50_000_001)
  }

  override fun start(
    sourceConfig: WorkerSourceConfig,
    jobRoot: Path?,
    connectionId: UUID?,
  ) {
  }

  override val isFinished: Boolean
    get() = counter.get() > MAX_RECORDS

  override val exitValue: Int
    get() = 0

  override fun attemptRead(): Optional<AirbyteMessage> {
    while (counter.getAndIncrement() <= MAX_RECORDS) {
      val data =
        mutableMapOf(
          FIELD_1 to STRING_VALUE,
          FIELD_2 to STRING_VALUE,
          FIELD_3 to STRING_VALUE,
          FIELD_4 to STRING_VALUE,
          FIELD_5 to STRING_VALUE,
        )

      return Optional.of(
        AirbyteMessage()
          .withType(AirbyteMessage.Type.RECORD)
          .withRecord(
            AirbyteRecordMessage()
              .withStream(STREAM)
              .withEmittedAt(Instant.now().millis)
              .withData(Jsons.jsonNode(data)),
          ),
      )
    }

    return Optional.empty()
  }

  override fun cancel() {
    counter.set(50_000_001)
  }
}
