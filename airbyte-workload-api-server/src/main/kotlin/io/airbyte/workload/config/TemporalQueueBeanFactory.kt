package io.airbyte.workload.config

import io.airbyte.commons.temporal.WorkflowClientWrapped
import io.airbyte.commons.temporal.converter.AirbyteTemporalDataConverter
import io.airbyte.commons.temporal.factories.TemporalCloudConfig
import io.airbyte.commons.temporal.factories.TemporalSelfHostedConfig
import io.airbyte.commons.temporal.factories.WorkflowClientFactory
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsFactory
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsTimeouts
import io.airbyte.commons.temporal.queue.TemporalMessageProducer
import io.airbyte.config.messages.LauncherInputMessage
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricClientFactory
import io.airbyte.metrics.lib.MetricEmittingApps
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.opentracing.OpenTracingClientInterceptor
import jakarta.inject.Singleton
import java.time.Duration

@Factory
class TemporalQueueBeanFactory {
  @Singleton
  fun createMetricClient(): MetricClient {
    MetricClientFactory.initialize(MetricEmittingApps.WORKLOAD_API)
    return MetricClientFactory.getMetricClient()
  }

  @Singleton
  fun createTemporalCloudConfig(
    @Property(name = "temporal.cloud.client.cert") clientCert: String,
    @Property(name = "temporal.cloud.client.key") clientKey: String,
    @Property(name = "temporal.cloud.host") host: String,
    @Property(name = "temporal.cloud.namespace") namespace: String,
  ): TemporalCloudConfig =
    TemporalCloudConfig(
      clientCert = clientCert,
      clientKey = clientKey,
      host = host,
      namespace = namespace,
    )

  @Singleton
  fun createTemporalSelfHostedConfig(
    @Property(name = "temporal.self-hosted.host") host: String,
    @Property(name = "temporal.self-hosted.namespace") namespace: String,
  ): TemporalSelfHostedConfig = TemporalSelfHostedConfig(host = host, namespace = namespace)

  @Singleton
  fun createTemporalTimeouts(
    @Property(name = "temporal.sdk.timeouts.max-time-to-connect") maxTimeToConnect: Duration,
    @Property(name = "temporal.sdk.timeouts.rpc-timeout") rpcTimeout: Duration,
    @Property(name = "temporal.sdk.timeouts.rpc-long-poll-timeout") rpcLongPollTimeout: Duration,
    @Property(name = "temporal.sdk.timeouts.rpc-query-timeout") rpcQueryTimeout: Duration,
  ): WorkflowServiceStubsTimeouts =
    WorkflowServiceStubsTimeouts(
      rpcTimeout = rpcTimeout,
      rpcLongPollTimeout = rpcLongPollTimeout,
      rpcQueryTimeout = rpcQueryTimeout,
      maxTimeToConnect = maxTimeToConnect,
    )

  @Singleton
  fun createWorkflowClient(
    temporalCloudConfig: TemporalCloudConfig,
    temporalSelfHostedConfig: TemporalSelfHostedConfig,
    airbyteTemporalDataConveter: AirbyteTemporalDataConverter,
    temporalTimeouts: WorkflowServiceStubsTimeouts,
    @Property(name = "temporal.cloud.enabled", defaultValue = "false") temporalCloudEnabled: Boolean,
  ): WorkflowClient {
    val workflowServiceStub =
      WorkflowServiceStubsFactory(temporalCloudConfig, temporalSelfHostedConfig, temporalCloudEnabled)
        .createWorkflowService(temporalTimeouts)
    val namespace = if (temporalCloudEnabled) temporalCloudConfig.namespace else temporalSelfHostedConfig.namespace
    val workflowClientOptions =
      WorkflowClientOptions
        .newBuilder()
        .setNamespace(namespace.orEmpty())
        .setInterceptors(OpenTracingClientInterceptor())
        .setDataConverter(airbyteTemporalDataConveter)
        .build()

    return WorkflowClientFactory().createWorkflowClient(workflowServiceStub, workflowClientOptions)
  }

  @Singleton
  fun createTemporalQueueProducer(
    workflowClient: WorkflowClient,
    metricClient: MetricClient,
  ): TemporalMessageProducer<LauncherInputMessage> {
    val workflowClientWrapped = WorkflowClientWrapped(workflowClient, metricClient)
    return TemporalMessageProducer(workflowClientWrapped)
  }
}
