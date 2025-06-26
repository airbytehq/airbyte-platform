/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.persistence.job.models.ReplicationInput
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.BufferedWriter
import java.util.Optional

private val logger = KotlinLogging.logger {}

/**
 * Factory for creating a writer that writes protocol messages in a specific protocol version.
 */
@Singleton
class AirbyteMessageBufferedWriterFactory(
  val replicationInput: ReplicationInput,
  private val serDeProvider: AirbyteMessageSerDeProvider,
  private val migratorFactory: AirbyteProtocolVersionedMigratorFactory,
) {
  private val configuredAirbyteCatalog: ConfiguredAirbyteCatalog? = replicationInput.catalog
  private val protocolVersion = replicationInput.destinationLauncherConfig.protocolVersion

  fun createWriter(bufferedWriter: BufferedWriter): AirbyteMessageBufferedWriter<*> {
    val needMigration = protocolVersion.majorVersion != migratorFactory.mostRecentVersion.majorVersion
    val additionalMessage =
      if (needMigration) {
        ", messages will be downgraded from protocol version ${migratorFactory.mostRecentVersion.serialize()}"
      } else {
        ""
      }
    logger.info { "Writing messages to protocol version ${protocolVersion.serialize()}$additionalMessage" }
    return AirbyteMessageBufferedWriter(
      writer = bufferedWriter,
      serDeProvider.getSerializer(protocolVersion).orElseThrow(),
      migratorFactory.getAirbyteMessageMigrator(protocolVersion),
      Optional.ofNullable(configuredAirbyteCatalog),
    )
  }
}
