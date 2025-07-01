/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.commons.protocol.AirbyteMessageVersionedMigrator
import io.airbyte.commons.protocol.serde.AirbyteMessageSerializer
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteMessage
import java.io.BufferedWriter
import java.io.IOException
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
  @Throws(IOException::class)
  fun write(message: AirbyteMessage) {
    val downgradedMessage = migrator.downgrade(message, configuredAirbyteCatalog)
    writer.write(serializer.serialize(downgradedMessage))
    writer.newLine()
  }

  @Throws(IOException::class)
  fun flush() {
    writer.flush()
  }

  @Throws(IOException::class)
  fun close() {
    writer.close()
  }
}
