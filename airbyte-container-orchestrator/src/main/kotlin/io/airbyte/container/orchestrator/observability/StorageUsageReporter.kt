/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.observability

import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.ReportConnectorDiskUsage
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.pod.FileConstants.DEST_DIR
import io.airbyte.workers.pod.FileConstants.SOURCE_DIR
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.io.BufferedReader
import java.io.InputStreamReader

private val logger = KotlinLogging.logger {}

/**
 * Periodically measures disk usage for connector dirs and reports a metric to DD.
 * Can be enabled/disabled via FF in case of performance degradation.
 */
@Singleton
class StorageUsageReporter(
  @Value("\${airbyte.connection-id}") private val connectionId: String,
  @Value("\${airbyte.staging-dir}") private val stagingDir: String?,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
  private val input: ReplicationInput,
) {
  @Scheduled(fixedDelay = "60s")
  fun reportConnectorDiskUsage() {
    // NO-OP if FF disabled
    if (!featureFlagClient.boolVariation(ReportConnectorDiskUsage, Connection(connectionId))) {
      return
    }

    val sourceMbUsed = measureDirMbViaProc(SOURCE_DIR)

    val destMbUsed = measureDirMbViaProc(DEST_DIR)

    val connectionIdAttr = MetricAttribute(MetricTags.CONNECTION_ID, connectionId)
    sourceMbUsed?.let {
      logger.debug { "Disk used by source: $sourceMbUsed MB" }

      val typeAttr = MetricAttribute(MetricTags.CONNECTOR_TYPE, "source")
      val imageAttr = MetricAttribute(MetricTags.CONNECTOR_IMAGE, input.sourceLauncherConfig.dockerImage)
      metricClient.gauge(OssMetricsRegistry.CONNECTOR_STORAGE_USAGE_MB, it, typeAttr, imageAttr, connectionIdAttr)
    }
    destMbUsed?.let {
      logger.debug { "Disk used by dest: $destMbUsed MB" }

      val typeAttr = MetricAttribute(MetricTags.CONNECTOR_TYPE, "destination")
      val imageAttr = MetricAttribute(MetricTags.CONNECTOR_IMAGE, input.destinationLauncherConfig.dockerImage)
      metricClient.gauge(OssMetricsRegistry.CONNECTOR_STORAGE_USAGE_MB, it, typeAttr, imageAttr, connectionIdAttr)
    }

    // conditionally record staging usage as separate metric
    stagingDir?.let {
      val stagingMbUsed = measureDirMbViaProc(it)
      stagingMbUsed?.let {
        logger.debug { "Disk used by staging: $stagingMbUsed MB" }

        val sourceImageAttr = MetricAttribute(MetricTags.SOURCE_IMAGE, input.sourceLauncherConfig.dockerImage)
        val destImageAttr = MetricAttribute(MetricTags.DESTINATION_IMAGE, input.destinationLauncherConfig.dockerImage)
        metricClient.gauge(OssMetricsRegistry.CONNECTION_STAGING_STORAGE_USAGE_MB, it, sourceImageAttr, destImageAttr, connectionIdAttr)
      }
    }
  }

  private fun measureDirMbViaProc(dir: String): Double? {
    logger.debug { "Checking disk usage for dir: $dir" }

    val command =
      arrayOf(
        "/bin/sh",
        "-c",
        """du -cm $dir | tail -n 1 | grep -o '^[0-9]\+'""",
      )

    val proc = Runtime.getRuntime().exec(command)

    val exitCode = proc.waitFor()
    if (exitCode != 0) {
      return null
    }

    return try {
      val r = BufferedReader(InputStreamReader(proc.inputStream))
      val result = r.readLine()
      result.toDouble()
    } catch (e: Exception) {
      logger.error(e) { "Failure checking disk usage for dir: $dir" }
      null
    }
  }
}
