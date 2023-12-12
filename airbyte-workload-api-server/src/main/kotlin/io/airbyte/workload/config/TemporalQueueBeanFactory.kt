package io.airbyte.workload.config

import io.airbyte.commons.temporal.WorkflowClientWrapped
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
import io.temporal.serviceclient.WorkflowServiceStubs
import jakarta.inject.Singleton
import java.time.Duration

@Factory
class TemporalQueueBeanFactory {
  @Singleton
  fun createMetricClient(): MetricClient {
    MetricClientFactory.initialize(MetricEmittingApps.SERVER)
    return MetricClientFactory.getMetricClient()
  }

  @Singleton
  fun createTemporalCloudConfig(
    @Property(name = "temporal.cloud.client.cert") clientCert: String,
    @Property(name = "temporal.cloud.client.key") clientKey: String,
    @Property(name = "temporal.cloud.host") host: String,
    @Property(name = "temporal.cloud.namespace") namespace: String,
  ): TemporalCloudConfig {
    return TemporalCloudConfig(
      clientCert = clientCert,
      clientKey = clientKey,
      host = host,
      namespace = namespace,
    )
  }

  @Singleton
  fun createTemporalSelfHostedConfig(
    @Property(name = "temporal.self-hosted.host") host: String,
    @Property(name = "temporal.self-hosted.namespace") namespace: String,
  ): TemporalSelfHostedConfig {
    return TemporalSelfHostedConfig(host = host, namespace = namespace)
  }

  @Singleton
  fun createTemporalTimeouts(
    @Property(name = "temporal.sdk.timeouts.max-time-to-connect") maxTimeToConnect: Duration,
    @Property(name = "temporal.sdk.timeouts.rpc-timeout") rpcTimeout: Duration,
    @Property(name = "temporal.sdk.timeouts.rpc-long-poll-timeout") rpcLongPollTimeout: Duration,
    @Property(name = "temporal.sdk.timeouts.rpc-query-timeout") rpcQueryTimeout: Duration,
  ): WorkflowServiceStubsTimeouts {
    return WorkflowServiceStubsTimeouts(
      rpcTimeout = rpcTimeout,
      rpcLongPollTimeout = rpcLongPollTimeout,
      rpcQueryTimeout = rpcQueryTimeout,
      maxTimeToConnect = maxTimeToConnect,
    )
  }

  @Singleton
  fun createWorkflowServiceStub(
    temporalCloudConfig: TemporalCloudConfig,
    temporalSelfHostedConfig: TemporalSelfHostedConfig,
    temporalTimeouts: WorkflowServiceStubsTimeouts,
    @Property(name = "temporal.cloud.enabled", defaultValue = "false") temporalCloudEnabled: Boolean,
  ): WorkflowServiceStubs {
    return WorkflowServiceStubsFactory(temporalCloudConfig, temporalSelfHostedConfig, temporalCloudEnabled)
      .createWorkflowService(temporalTimeouts)
  }

  @Singleton
  fun createWorkflowClient(
    workflowServiceStub: WorkflowServiceStubs,
    temporalCloudConfig: TemporalCloudConfig,
    temporalSelfHostedConfig: TemporalSelfHostedConfig,
    @Property(name = "temporal.cloud.enabled", defaultValue = "false") temporalCloudEnabled: Boolean,
  ): WorkflowClient {
    val namespace = if (temporalCloudEnabled) temporalCloudConfig.namespace else temporalSelfHostedConfig.namespace
    val workflowClientOptions =
      WorkflowClientOptions.newBuilder()
        .setNamespace(namespace.orEmpty())
        .setInterceptors(OpenTracingClientInterceptor())
        .build()
    return WorkflowClientFactory().createWorkflowClient(workflowServiceStub, workflowClientOptions)
  }

  @Singleton
  fun createWorkflowClientWrapped(
    workflowClient: WorkflowClient,
    metricClient: MetricClient,
  ): WorkflowClientWrapped {
    return WorkflowClientWrapped(workflowClient, metricClient)
  }

  @Singleton
  fun createTemporalQueueProducer(worklowClientWrapped: WorkflowClientWrapped): TemporalMessageProducer<LauncherInputMessage> {
    return TemporalMessageProducer<LauncherInputMessage>(worklowClientWrapped)
  }
}
