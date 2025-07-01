/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.config

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.logging.MdcScope
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.commons.resources.Resources
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.connectorbuilder.commandrunner.SynchronousCdkCommandRunner
import io.airbyte.connectorbuilder.commandrunner.SynchronousPythonCdkCommandRunner
import io.airbyte.connectorbuilder.filewriter.AirbyteFileWriterImpl
import io.airbyte.metrics.MetricClient
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.File
import java.util.Optional

private const val PATH_TO_CONNECTORS = "/connectors"

/**
 * Defines the instantiation of handler classes.
 */
@Factory
class ApplicationBeanFactory(
  @Value("\${airbyte.connector-builder-server.capabilities.enable-unsafe-code}") private val enableUnsafeCodeGlobalOverride: Boolean,
  @Value("\${CDK_PYTHON}") private val cdkPython: String,
  @Value("\${CDK_ENTRYPOINT}") private val cdkEntrypoint: String,
) {
  @Singleton
  fun versionedAirbyteStreamFactory(
    metricClient: MetricClient,
    airbyteProtocolVersionedMigratorFactory: AirbyteProtocolVersionedMigratorFactory,
    airbyteMessageSerDeProvider: AirbyteMessageSerDeProvider,
    gsonPksExtractor: GsonPksExtractor,
  ): VersionedAirbyteStreamFactory<Any> =
    VersionedAirbyteStreamFactory(
      gsonPksExtractor = gsonPksExtractor,
      invalidLineFailureConfiguration = VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration(false),
      metricClient = metricClient,
      protocolVersion = AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION,
      serDeProvider = airbyteMessageSerDeProvider,
      migratorFactory = airbyteProtocolVersionedMigratorFactory,
      connectionId = Optional.empty(),
      configuredAirbyteCatalog = Optional.empty(),
      containerLogMdcBuilder = MdcScope.DEFAULT_BUILDER,
    )

  /**
   * Defines the instantiation of the SynchronousPythonCdkCommandRunner.
   */
  @Singleton
  fun synchronousPythonCdkCommandRunner(versionedAirbyteStreamFactory: VersionedAirbyteStreamFactory<Any>): SynchronousCdkCommandRunner =
    SynchronousPythonCdkCommandRunner(
      AirbyteFileWriterImpl(),
      versionedAirbyteStreamFactory,
      cdkPython,
      cdkEntrypoint,
      pythonPath(),
      enableUnsafeCodeGlobalOverride,
    )

  @Singleton
  @Named("buildCdkVersion")
  fun buildCdkVersion(): String = Resources.read("CDK_VERSION").trim()
}

@InternalForTesting
internal fun pythonPath(path: String = PATH_TO_CONNECTORS): String {
  val subDirs =
    File(path)
      .list { current, name -> File(current, name).isDirectory }
      ?.sorted() ?: emptyList()

  // Creates a `:`-separated path of all connector directories.
  // The connector directories that contain a python module can then be imported
  return subDirs.joinToString(":") { "$PATH_TO_CONNECTORS/$it" }
}
