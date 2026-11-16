/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.check

import io.airbyte.commons.logging.DEFAULT_JOB_LOG_PATH_MDC_KEY
import io.airbyte.commons.logging.LogSource
import io.airbyte.config.WorkloadType
import io.airbyte.directjobrunner.connector.DirectCheckAllowlist
import io.airbyte.directjobrunner.connector.DirectCheckRunner
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
import jakarta.inject.Singleton
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Runs CHECK workloads for the connectors in [DirectCheckAllowlist], at the version the check asks for, from that
 * version's fat jar (downloaded by the first check that needs it). Each check is its own connector process
 * ([DirectCheckRunner]), which exits when the check is done, so customers never share a process.
 *
 * Platform and connector logs go to the workload's log path, like a check pod's. Not handled: control-message config
 * updates, which a check pod applies to the saved actor.
 */
@Singleton
class JarCheckJobHandler(
  private val payloadDeserializer: PayloadDeserializer,
  private val secretsHydrator: ConnectorSecretsHydrator,
  private val directCheckRunner: DirectCheckRunner,
  private val allowlist: DirectCheckAllowlist,
  private val outputWriter: WorkloadOutputWriter,
  private val metricClient: MetricClient,
) : DirectJobHandler {
  override val workloadType: WorkloadType = WorkloadType.CHECK

  // Time to download a jar that isn't cached, the connector's own timeout, and time to hydrate and write the result. A
  // check whose runner died fails this soon.
  override val runningDeadline: Duration = directCheckRunner.jarDownloadAllowance.plus(directCheckRunner.checkTimeout).plus(RESULT_MARGIN)

  // A check turned down here runs in a pod. FLEX workloads upload logs to the organization's own destination, which only
  // the check pod's sidecar sets up. An allowlisted connector whose jar for this version isn't in the jar repository
  // can't run here.
  override fun accepts(
    workload: Workload,
    dataplaneGroup: LauncherDataplaneGroup,
  ): Boolean {
    if (workload.logDeliveryMode == LogDeliveryMode.FLEX) {
      return turnDown("flex_logs")
    }
    // The allowlist is per data plane group, so it is empty for a launcher that sent none.
    if (dataplaneGroup == LauncherDataplaneGroup.Unknown) {
      return turnDown("no_dataplane_group")
    }

    val image = payloadDeserializer.toCheckConnectionInput(workload.inputPayload).launcherConfig.dockerImage
    if (!allowlist.allows(image, dataplaneGroup)) {
      return turnDown("not_allowlisted")
    }

    if (!directCheckRunner.canRun(image)) {
      logger.info { "Turning down check ${workload.id}: no jar for $image in the jar repository; it runs in a pod" }
      return turnDown("jar_unavailable")
    }
    return true
  }

  // Always false: a turned-down check runs in a pod.
  private fun turnDown(reason: String): Boolean {
    metricClient.count(OssMetricsRegistry.DIRECT_CHECK_TURNED_DOWN, 1, MetricAttribute(MetricTags.ROUTE_REASON_TAG, reason))
    return false
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

    val run = directCheckRunner.check(image, config, workloadId)
    val result = run.result
    val ranAt = System.currentTimeMillis()

    val output = toCheckJobOutput(result, input.checkConnectionInput.actorType)
    outputWriter.write(workloadId, output)

    // The connector's run only: a jar download on a cache miss would otherwise skew it.
    metricClient.distribution(
      OssMetricsRegistry.DIRECT_CHECK_CONNECTOR_DURATION,
      run.processMs.toDouble(),
      MetricAttribute(MetricTags.CONNECTOR_IMAGE, image),
      MetricAttribute(MetricTags.RESULT_TAG, result.javaClass.simpleName),
      MetricAttribute(MetricTags.JAR_SOURCE_TAG, run.jarSource.name.lowercase()),
    )
    logger.info {
      "Check $workloadId image=$image result=${result::class.simpleName} status=${output.checkConnection?.status} " +
        "jar=${run.jarSource} hydrateMs=${hydratedAt - start} jarMs=${run.jarMs} processMs=${run.processMs} " +
        "writeMs=${System.currentTimeMillis() - ranAt} totalMs=${System.currentTimeMillis() - start}"
    }
  }
}
