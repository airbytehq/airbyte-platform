/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.commons.protocol.AirbyteMessageVersionedMigrator
import io.airbyte.commons.protocol.serde.AirbyteMessageSerializer
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteMessage
import java.io.BufferedWriter
import java.util.Optional

/**
 * Write protocol objects in a specified version.
 *
 * @param <T> type of protocol object.
</T> */
class AirbyteMessageBufferedWriter<T : Any>(
  private val writer: BufferedWriter,
  private val serializer: AirbyteMessageSerializer<T>,
  private val migrator: AirbyteMessageVersionedMigrator<T>,
  private val configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
) {
  fun write(message: AirbyteMessage) {
    val downgradedMessage = migrator.downgrade(message, configuredAirbyteCatalog)
    writer.write(serializer.serialize(downgradedMessage))
    writer.newLine()
  }

  fun flush() {
    writer.flush()
  }

  fun close() {
    writer.close()
  }
}
