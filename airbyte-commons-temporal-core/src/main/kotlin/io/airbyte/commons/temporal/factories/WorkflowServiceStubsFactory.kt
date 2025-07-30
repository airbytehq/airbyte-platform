/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.factories

import com.uber.m3.tally.RootScopeBuilder
import com.uber.m3.tally.StatsReporter
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.MeterRegistry
import io.temporal.common.reporter.MicrometerClientStatsReporter
import io.temporal.serviceclient.SimpleSslContextBuilder
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.time.Duration
import javax.net.ssl.SSLException

private val log = KotlinLogging.logger {}

/**
 * Defines the different timeouts to use with temporal.
 *
 * @property [rpcTimeout] sets the rpc timeout value for non-query and non-long-poll calls.
 * @property [rpcLongPollTimeout] sets the rpc timeout value for the following long poll based operations: PollWorkflowTaskQueue,
 * PollActivityTaskQueue, GetWorkflowExecutionHistory.
 * @property [rpcQueryTimeout] sets the rpc timeout value for query calls.
 * @property [maxTimeToConnect] sets how long to wait for the initialization of the connection when creating a temporal client.
 */
data class WorkflowServiceStubsTimeouts(
  val rpcTimeout: Duration = WorkflowServiceStubsOptions.DEFAULT_RPC_TIMEOUT,
  val rpcLongPollTimeout: Duration = WorkflowServiceStubsOptions.DEFAULT_POLL_RPC_TIMEOUT,
  val rpcQueryTimeout: Duration = WorkflowServiceStubsOptions.DEFAULT_QUERY_RPC_TIMEOUT,
  val maxTimeToConnect: Duration = WorkflowServiceStubsOptions.DEFAULT_RPC_TIMEOUT,
)

/**
 * Connection configuration when using temporal cloud.
 */
data class TemporalCloudConfig(
  val clientCert: String?,
  val clientKey: String?,
  val host: String?,
  val namespace: String?,
)

/**
 * Connection configuration when using a self-hosted version of temporal.
 */
data class TemporalSelfHostedConfig(
  val host: String?,
  val namespace: String?,
)

/**
 * Factory class to build WorkflowServiceStubs.
 *
 * @property [temporalCloudConfig] the temporal cloud configuration.
 * @property [temporalSelfHostedConfig] the temporal self-hosted configuration.
 * @property [temporalCloudEnabled] true if temporal cloud is enabled, false otherwise.
 */
class WorkflowServiceStubsFactory(
  private val temporalCloudConfig: TemporalCloudConfig,
  private val temporalSelfHostedConfig: TemporalSelfHostedConfig,
  private val temporalCloudEnabled: Boolean,
  private val meterRegistry: MeterRegistry?,
) {
  companion object {
    private const val REPORT_INTERVAL_SECONDS: Double = 120.0
  }

  fun createWorkflowService(timeoutOptions: WorkflowServiceStubsTimeouts): WorkflowServiceStubs {
    val options = createWorkflowServiceStubsOptions(timeoutOptions)
    return WorkflowServiceStubs.newConnectedServiceStubs(options, timeoutOptions.maxTimeToConnect)
  }

  fun createWorkflowServiceStubsOptions(timeoutOptions: WorkflowServiceStubsTimeouts): WorkflowServiceStubsOptions =
    if (temporalCloudEnabled) {
      createTemporalCloudOptions(timeoutOptions)
    } else {
      createTemporalSelfHostedOptions(timeoutOptions)
    }

  private fun createTemporalCloudOptions(timeoutOptions: WorkflowServiceStubsTimeouts): WorkflowServiceStubsOptions {
    val clientCert: InputStream = ByteArrayInputStream(temporalCloudConfig.clientCert.orEmpty().toByteArray(StandardCharsets.UTF_8))
    val clientKey: InputStream = ByteArrayInputStream(temporalCloudConfig.clientKey.orEmpty().toByteArray(StandardCharsets.UTF_8))

    val sslContext =
      try {
        SimpleSslContextBuilder.forPKCS8(clientCert, clientKey).build()
      } catch (e: SSLException) {
        log.error("SSL Exception occurred attempting to establish Temporal Cloud options.", e)
        throw RuntimeException(e)
      }

    val optionBuilder =
      WorkflowServiceStubsOptions
        .newBuilder()
        .setRpcTimeout(timeoutOptions.rpcTimeout)
        .setRpcLongPollTimeout(timeoutOptions.rpcLongPollTimeout)
        .setRpcQueryTimeout(timeoutOptions.rpcQueryTimeout)
        .setSslContext(sslContext)
        .setTarget(temporalCloudConfig.host)

    configureTemporalMeterRegistry(optionBuilder)

    return optionBuilder.build()
  }

  private fun configureTemporalMeterRegistry(optionBuilder: WorkflowServiceStubsOptions.Builder) {
    meterRegistry?.let {
      val reporter: StatsReporter = MicrometerClientStatsReporter(it)
      optionBuilder.setMetricsScope(
        RootScopeBuilder()
          .reporter(reporter)
          .reportEvery(
            com.uber.m3.util.Duration
              .ofSeconds(REPORT_INTERVAL_SECONDS),
          ),
      )
    }
  }

  private fun createTemporalSelfHostedOptions(timeoutOptions: WorkflowServiceStubsTimeouts): WorkflowServiceStubsOptions =
    WorkflowServiceStubsOptions
      .newBuilder()
      .setRpcTimeout(timeoutOptions.rpcTimeout)
      .setRpcLongPollTimeout(timeoutOptions.rpcLongPollTimeout)
      .setRpcQueryTimeout(timeoutOptions.rpcQueryTimeout)
      .setTarget(temporalSelfHostedConfig.host)
      .build()
}
