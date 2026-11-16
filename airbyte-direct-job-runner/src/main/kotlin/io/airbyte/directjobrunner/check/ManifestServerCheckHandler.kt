/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.check

import io.airbyte.commons.logging.DEFAULT_JOB_LOG_PATH_MDC_KEY
import io.airbyte.commons.logging.LogSource
import io.airbyte.config.WorkloadType
import io.airbyte.directjobrunner.connector.ManifestServerCheckRunner
import io.airbyte.directjobrunner.core.DirectJobHandler
import io.airbyte.directjobrunner.core.LauncherDataplaneGroup
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.airbyte.workload.api.domain.LogDeliveryMode
import io.airbyte.workload.api.domain.Workload
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Order
import jakarta.inject.Singleton
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Runs CHECK workloads for Airbyte's manifest-only connectors on the data plane's manifest server
 * ([ManifestServerCheckRunner]), with the manifest the connector registry publishes for the check's version.
 *
 * Checks are offered to it before [JarCheckJobHandler] (the [Order]). It takes only manifest-only connectors, which
 * have no jar, so the jar handler sees just the checks this one turns down, and its turn-down metrics stay about jars.
 */
@Singleton
@Order(-1)
@Requires(beans = [ManifestServerCheckRunner::class])
class ManifestServerCheckHandler(
  private val payloadDeserializer: PayloadDeserializer,
  private val secretsHydrator: ConnectorSecretsHydrator,
  private val manifestServer: ManifestServerCheckRunner,
  private val outputWriter: WorkloadOutputWriter,
  private val metricClient: MetricClient,
) : DirectJobHandler {
  override val workloadType: WorkloadType = WorkloadType.CHECK

  // The manifest server call's own timeout, and time to hydrate and write the result. A check whose runner died fails
  // this soon.
  override val runningDeadline: Duration = manifestServer.checkTimeout.plus(RESULT_MARGIN)

  // A check turned down here is offered to the jar handler, then runs in a pod. FLEX workloads upload logs to the
  // organization's own destination, which only the check pod's sidecar sets up.
  override fun accepts(
    workload: Workload,
    dataplaneGroup: LauncherDataplaneGroup,
  ): Boolean {
    if (workload.logDeliveryMode == LogDeliveryMode.FLEX) {
      return false
    }
    val image = payloadDeserializer.toCheckConnectionInput(workload.inputPayload).launcherConfig.dockerImage
    return manifestServer.runs(image)
  }

  override fun run(
    workloadId: String,
    inputPayload: String,
    logPath: String,
  ) = withLoggingContext(mapOf(DEFAULT_JOB_LOG_PATH_MDC_KEY to logPath) + LogSource.PLATFORM.toMdc()) {
    runCheck(workloadId, inputPayload)
  }

  private fun runCheck(
    workloadId: String,
    inputPayload: String,
  ) {
    val start = System.currentTimeMillis()
    val input = payloadDeserializer.toCheckConnectionInput(inputPayload)
    val image = input.launcherConfig.dockerImage

    val config = secretsHydrator.hydrateCheckConfig(input.checkConnectionInput)
    val hydratedAt = System.currentTimeMillis()

    val result = manifestServer.check(image, config, workloadId)
    val ranAt = System.currentTimeMillis()

    val output = toCheckJobOutput(result, input.checkConnectionInput.actorType)
    outputWriter.write(workloadId, output)

    metricClient.distribution(
      OssMetricsRegistry.DIRECT_CHECK_CONNECTOR_DURATION,
      (ranAt - hydratedAt).toDouble(),
      MetricAttribute(MetricTags.CONNECTOR_IMAGE, image),
      MetricAttribute(MetricTags.RESULT_TAG, result.javaClass.simpleName),
    )
    logger.info {
      "Check $workloadId image=$image result=${result::class.simpleName} status=${output.checkConnection?.status} " +
        "on=manifest-server hydrateMs=${hydratedAt - start} serverMs=${ranAt - hydratedAt} " +
        "writeMs=${System.currentTimeMillis() - ranAt} totalMs=${System.currentTimeMillis() - start}"
    }
  }
}
