/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import io.airbyte.analytics.TrackingClient
import io.airbyte.api.client.WebUrlHelper
import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.server.handlers.helpers.BuilderProjectUpdater
import io.airbyte.commons.server.handlers.helpers.CompositeBuilderProjectUpdater
import io.airbyte.commons.server.handlers.helpers.ConfigRepositoryBuilderProjectUpdater
import io.airbyte.commons.server.handlers.helpers.LocalFileSystemBuilderProjectUpdater
import io.airbyte.commons.server.limits.ProductLimitsProvider
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.commons.server.scheduler.TemporalEventRunner
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.commons.workers.config.WorkerConfigsProvider
import io.airbyte.config.Configs
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.config.persistence.StreamRefreshesRepository
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.DestinationTimeoutEnabled
import io.airbyte.featureflag.DestinationTimeoutSeconds
import io.airbyte.featureflag.FailSyncOnInvalidChecksum
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.FieldSelectionEnabled
import io.airbyte.featureflag.Flag
import io.airbyte.featureflag.LogConnectorMessages
import io.airbyte.featureflag.LogStateMsgs
import io.airbyte.featureflag.PrintLongRecordPks
import io.airbyte.featureflag.RemoveValidationLimit
import io.airbyte.featureflag.ReplicationBufferOverride
import io.airbyte.featureflag.ShouldFailSyncOnDestinationTimeout
import io.airbyte.featureflag.WorkloadHeartbeatRate
import io.airbyte.featureflag.WorkloadHeartbeatTimeout
import io.airbyte.metrics.MetricClient
import io.airbyte.oauth.OAuthImplementationFactory
import io.airbyte.persistence.job.DefaultJobCreator
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.persistence.job.factory.DefaultSyncJobFactory
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.factory.SyncJobFactory
import io.airbyte.persistence.job.tracker.JobTracker
import io.airbyte.workers.models.ReplicationFeatureFlags
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.net.http.HttpClient
import java.nio.file.Path
import java.util.List
import java.util.UUID
import java.util.function.Supplier

/**
 * Bean factory for the airbyte server micronaut app.
 */
@Factory
class ApplicationBeanFactory {
  @Singleton
  @Named("uuidGenerator")
  fun randomUUIDSupplier(): Supplier<UUID> = Supplier { UUID.randomUUID() }

  @Singleton
  fun eventRunner(temporalClient: TemporalClient?): EventRunner = TemporalEventRunner(temporalClient)

  @Singleton
  fun jobTracker(
    jobPersistence: JobPersistence?,
    trackingClient: TrackingClient?,
    actorDefinitionVersionHelper: ActorDefinitionVersionHelper?,
    sourceService: SourceService?,
    destinationService: DestinationService?,
    connectionService: ConnectionService?,
    operationService: OperationService?,
    workspaceService: WorkspaceService?,
  ): JobTracker =
    JobTracker(
      jobPersistence,
      trackingClient,
      actorDefinitionVersionHelper,
      sourceService,
      destinationService,
      connectionService,
      operationService,
      workspaceService,
    )

  @Singleton
  fun jobNotifier(
    trackingClient: TrackingClient?,
    webUrlHelper: WebUrlHelper?,
    workspaceHelper: WorkspaceHelper?,
    actorDefinitionVersionHelper: ActorDefinitionVersionHelper?,
    sourceService: SourceService?,
    destinationService: DestinationService?,
    connectionService: ConnectionService?,
    workspaceService: WorkspaceService?,
    metricClient: MetricClient?,
  ): JobNotifier =
    JobNotifier(
      webUrlHelper,
      connectionService,
      sourceService,
      destinationService,
      workspaceService,
      workspaceHelper,
      trackingClient,
      actorDefinitionVersionHelper,
      metricClient,
    )

  @Singleton
  fun defaultJobCreator(
    jobPersistence: JobPersistence?,
    workerConfigsProvider: WorkerConfigsProvider?,
    featureFlagClient: FeatureFlagClient?,
    streamRefreshesRepository: StreamRefreshesRepository?,
    @Value("\${airbyte.worker.kube-job-config-variant-override}") variantOverride: String?,
  ): DefaultJobCreator = DefaultJobCreator(jobPersistence, workerConfigsProvider, featureFlagClient, streamRefreshesRepository, variantOverride)

  @Singleton
  fun jobFactory(
    jobPersistence: JobPersistence?,
    @Property(
      name = "airbyte.connector.specific-resource-defaults-enabled",
      defaultValue = "false",
    ) connectorSpecificResourceDefaultsEnabled: Boolean,
    jobCreator: DefaultJobCreator?,
    oAuthConfigSupplier: OAuthConfigSupplier?,
    configInjector: ConfigInjector?,
    actorDefinitionVersionHelper: ActorDefinitionVersionHelper?,
    sourceService: SourceService?,
    destinationService: DestinationService?,
    connectionService: ConnectionService?,
    operationService: OperationService?,
    workspaceService: WorkspaceService?,
  ): SyncJobFactory =
    DefaultSyncJobFactory(
      connectorSpecificResourceDefaultsEnabled,
      jobCreator,
      oAuthConfigSupplier,
      configInjector,
      WorkspaceHelper(jobPersistence, connectionService, sourceService, destinationService, operationService, workspaceService),
      actorDefinitionVersionHelper,
      sourceService,
      destinationService,
      connectionService,
      operationService,
      workspaceService,
    )

  @Singleton
  @Named("workspaceRoot")
  fun workspaceRoot(
    @Value("\${airbyte.workspace.root}") workspaceRoot: String,
  ): Path = Path.of(workspaceRoot)

  @Singleton
  @Named("airbyteSupportEmailDomains")
  fun airbyteSupportEmailDomains(
    airbyteEdition: Configs.AirbyteEdition,
    @Value("\${airbyte.support-email-domains.oss}") ossSupportEmailDomains: String,
    @Value("\${airbyte.support-email-domains.cloud}") cloudSupportEmailDomains: String?,
  ): Set<String> {
    val supportEmailDomains =
      if (airbyteEdition == Configs.AirbyteEdition.CLOUD) cloudSupportEmailDomains!! else ossSupportEmailDomains
    if (supportEmailDomains.isEmpty()) {
      return setOf()
    }
    return supportEmailDomains
      .split(",")
      .map { it.trim() }
      .filter { it.isNotEmpty() }
      .toSet()
  }

  @Singleton
  fun jsonSecretsProcessor(): JsonSecretsProcessor = JsonSecretsProcessor(false)

  @Singleton
  @Named("jsonSecretsProcessorWithCopy")
  fun jsonSecretsProcessorWithCopy(): JsonSecretsProcessor = JsonSecretsProcessor(true)

  @Singleton
  @Named("oauthHttpClient")
  fun httpClient(): HttpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()

  @Singleton
  @Named("oauthImplementationFactory")
  fun oauthImplementationFactory(): OAuthImplementationFactory =
    OAuthImplementationFactory(HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build())

  @Singleton
  fun builderProjectUpdater(connectorBuilderService: ConnectorBuilderService?): BuilderProjectUpdater {
    val pathToConnectors = EnvVar.PATH_TO_CONNECTORS.fetch()
    val configRepositoryProjectUpdater = ConfigRepositoryBuilderProjectUpdater(connectorBuilderService)
    return if (pathToConnectors == null || pathToConnectors.isEmpty()) {
      configRepositoryProjectUpdater
    } else {
      CompositeBuilderProjectUpdater(
        listOf(
          configRepositoryProjectUpdater,
          LocalFileSystemBuilderProjectUpdater(),
        ),
      )
    }
  }

  @Singleton
  fun kubernetesClient(): KubernetesClient = KubernetesClientBuilder().build()

  @Singleton
  fun defaultWorkspaceLimits(
    @Value("\${airbyte.server.limits.connections}") maxConnections: Long,
    @Value("\${airbyte.server.limits.sources}") maxSources: Long,
    @Value("\${airbyte.server.limits.destinations}") maxDestinations: Long,
  ): ProductLimitsProvider.WorkspaceLimits = ProductLimitsProvider.WorkspaceLimits(maxConnections, maxSources, maxDestinations)

  @Singleton
  fun defaultOrganizationLimits(
    @Value("\${airbyte.server.limits.workspaces}") maxWorkspaces: Long,
    @Value("\${airbyte.server.limits.users}") maxUsers: Long,
  ): ProductLimitsProvider.OrganizationLimits = ProductLimitsProvider.OrganizationLimits(maxWorkspaces, maxUsers)

  /**
   * This bean is duplicated from the bean in the config of the airbyte workers module.
   * This duplication has been made to avoid moving this bean to the common module.
   * In the future we should only need this bean.
   */
  @Singleton
  @Named("replicationFeatureFlags")
  fun replicationFeatureFlags(): ReplicationFeatureFlags {
    val featureFlags =
      List.of<Flag<*>>(
        DestinationTimeoutEnabled,
        DestinationTimeoutSeconds,
        FailSyncOnInvalidChecksum,
        FieldSelectionEnabled,
        LogConnectorMessages,
        LogStateMsgs,
        PrintLongRecordPks,
        RemoveValidationLimit,
        ReplicationBufferOverride,
        ShouldFailSyncOnDestinationTimeout,
        WorkloadHeartbeatRate,
        WorkloadHeartbeatTimeout,
      )
    return ReplicationFeatureFlags(featureFlags)
  }

  @Singleton
  @Named("outputDocumentStore")
  fun workloadStorageClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.WORKLOAD_OUTPUT)
}
