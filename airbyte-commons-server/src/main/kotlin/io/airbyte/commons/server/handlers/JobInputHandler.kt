/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody
import io.airbyte.api.model.generated.SyncInput
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.constants.WorkerConstants
import io.airbyte.commons.converters.ConfigReplacer
import io.airbyte.commons.converters.StateConverter.toInternal
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.handlers.helpers.ContextBuilder
import io.airbyte.commons.temporal.TemporalWorkflowUtils.createJobRunConfig
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AttemptSyncConfig
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.JobWebhookConfig
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.SourceActorConfig
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.State
import io.airbyte.config.helpers.StateMessageHelper.getState
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.toInlined
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.NetworkSecurityTokenKey
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.workers.models.JobInput
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.Nullable
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * Handler that allow to fetch a job input.
 */
@Singleton
open class JobInputHandler(
  private val jobPersistence: JobPersistence,
  private val featureFlagClient: FeatureFlagClient,
  private val oAuthConfigSupplier: OAuthConfigSupplier,
  private val configInjector: ConfigInjector,
  private val attemptHandler: AttemptHandler,
  private val stateHandler: StateHandler,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val contextBuilder: ContextBuilder,
  private val connectionService: ConnectionService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val apiPojoConverters: ApiPojoConverters,
  private val scopedConfigurationService: ScopedConfigurationService,
  private val secretReferenceService: SecretReferenceService,
) {
  /**
   * Generate a job input.
   */
  fun getJobInput(input: SyncInput): Any {
    try {
      addTagsToTrace(java.util.Map.of(ATTEMPT_NUMBER_KEY, input.attemptNumber, JOB_ID_KEY, input.jobId))
      val jobId = input.jobId
      val attempt = Math.toIntExact(input.attemptNumber.toLong())

      val job = jobPersistence.getJob(jobId)
      val config = getJobSyncConfig(jobId, job.config)

      var sourceVersion: ActorDefinitionVersion? = null

      val connectionId = UUID.fromString(job.scope)
      val standardSync = connectionService.getStandardSync(connectionId)

      val attemptSyncConfig = AttemptSyncConfig()
      getCurrentConnectionState(connectionId).ifPresent { state: State? ->
        attemptSyncConfig.state =
          state
      }

      val jobConfigType = job.config.configType

      if (Job.SYNC_REPLICATION_TYPES.contains(jobConfigType)) {
        val source = sourceService.getSourceConnection(standardSync.sourceId)
        sourceVersion =
          actorDefinitionVersionHelper.getSourceVersion(
            sourceService.getStandardSourceDefinition(source.sourceDefinitionId),
            source.workspaceId,
            source.sourceId,
          )
        val sourceConfiguration = getSourceConfiguration(source)
        val sourceConfigWithInlinedRefs: JsonNode = sourceConfiguration.toInlined().value
        attemptSyncConfig.sourceConfiguration = sourceConfigWithInlinedRefs
      } else if (ConfigType.RESET_CONNECTION == jobConfigType) {
        val resetConnection = job.config.resetConnection
        val resetSourceConfiguration = resetConnection.resetSourceConfiguration
        attemptSyncConfig.sourceConfiguration =
          if (resetSourceConfiguration == null) Jsons.emptyObject() else Jsons.jsonNode(resetSourceConfiguration)
      }

      val jobRunConfig = createJobRunConfig(jobId, attempt)

      val destination = destinationService.getDestinationConnection(standardSync.destinationId)
      val destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(
          destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId),
          destination.workspaceId,
          destination.destinationId,
        )
      val destinationConfiguration = getDestinationConfiguration(destination)
      val destinationConfigWithInlinedRefs: JsonNode = destinationConfiguration.toInlined().value
      attemptSyncConfig.destinationConfiguration = destinationConfigWithInlinedRefs

      val sourceLauncherConfig =
        getSourceIntegrationLauncherConfig(
          jobId,
          attempt,
          connectionId,
          config,
          sourceVersion,
          attemptSyncConfig.sourceConfiguration,
        )

      val destinationLauncherConfig =
        getDestinationIntegrationLauncherConfig(
          jobId,
          attempt,
          connectionId,
          config,
          destinationVersion,
          attemptSyncConfig.destinationConfiguration,
          java.util.Map.of(),
        )

      val featureFlagContext: MutableList<Context> = ArrayList()
      featureFlagContext.add(Workspace(config.workspaceId))
      if (standardSync.connectionId != null) {
        featureFlagContext.add(Connection(standardSync.connectionId))
      }

      val connectionContext = contextBuilder.fromConnectionId(connectionId)

      val shouldIncludeFiles = shouldIncludeFiles(config, sourceVersion, destinationVersion)
      val isDeprecatedFileTransfer = isDeprecatedFileTransfer(attemptSyncConfig.sourceConfiguration)

      val syncInput =
        StandardSyncInput()
          .withNamespaceDefinition(config.namespaceDefinition)
          .withNamespaceFormat(config.namespaceFormat)
          .withPrefix(config.prefix)
          .withSourceId(standardSync.sourceId)
          .withDestinationId(standardSync.destinationId)
          .withSourceConfiguration(attemptSyncConfig.sourceConfiguration)
          .withDestinationConfiguration(attemptSyncConfig.destinationConfiguration)
          .withOperationSequence(config.operationSequence)
          .withWebhookOperationConfigs(config.webhookOperationConfigs)
          .withSyncResourceRequirements(config.syncResourceRequirements)
          .withConnectionId(connectionId)
          .withWorkspaceId(config.workspaceId)
          .withIsReset(ConfigType.RESET_CONNECTION == jobConfigType)
          .withConnectionContext(connectionContext)
          .withUseAsyncReplicate(true)
          .withUseAsyncActivities(true)
          .withNetworkSecurityTokens(getNetworkSecurityTokens(config.workspaceId))
          .withIncludesFiles(shouldIncludeFiles || isDeprecatedFileTransfer)
          .withOmitFileTransferEnvVar(shouldIncludeFiles)

      saveAttemptSyncConfig(jobId, attempt, connectionId, attemptSyncConfig)
      return JobInput(jobRunConfig, sourceLauncherConfig, destinationLauncherConfig, syncInput)
    } catch (e: Exception) {
      throw RetryableException(e)
    }
  }

  private fun saveAttemptSyncConfig(
    jobId: Long,
    attemptNumber: Int,
    connectionId: UUID,
    attemptSyncConfig: AttemptSyncConfig,
  ) {
    attemptHandler.saveSyncConfig(
      SaveAttemptSyncConfigRequestBody()
        .jobId(jobId)
        .attemptNumber(attemptNumber)
        .syncConfig(apiPojoConverters.attemptSyncConfigToApi(attemptSyncConfig, connectionId)),
    )
  }

  @Throws(IOException::class)
  fun getJobWebhookConfig(jobId: Long): JobWebhookConfig {
    val job = jobPersistence.getJob(jobId)
    val jobConfig = job.config
    checkNotNull(jobConfig) { "Job config is null" }
    return getJobWebhookConfig(jobId, jobConfig)
  }

  private fun getJobWebhookConfig(
    jobId: Long,
    jobConfig: JobConfig,
  ): JobWebhookConfig {
    val jobConfigType = jobConfig.configType
    return if (ConfigType.SYNC == jobConfigType) {
      JobWebhookConfig()
        .withOperationSequence(jobConfig.sync.operationSequence)
        .withWebhookOperationConfigs(jobConfig.sync.webhookOperationConfigs)
    } else if (ConfigType.RESET_CONNECTION == jobConfigType) {
      JobWebhookConfig()
        .withOperationSequence(jobConfig.resetConnection.operationSequence)
        .withWebhookOperationConfigs(jobConfig.resetConnection.webhookOperationConfigs)
    } else if (ConfigType.REFRESH == jobConfigType) {
      JobWebhookConfig()
        .withOperationSequence(jobConfig.refresh.operationSequence)
        .withWebhookOperationConfigs(jobConfig.refresh.webhookOperationConfigs)
    } else {
      throw IllegalStateException(
        String.format(
          "Unexpected config type %s for job %d. The only supported config types for this activity are (%s)",
          jobConfigType,
          jobId,
          Job.REPLICATION_TYPES,
        ),
      )
    }
  }

  /**
   * Returns a Job's JobSyncConfig, converting it from a JobResetConnectionConfig if necessary.
   */
  private fun getJobSyncConfig(
    jobId: Long,
    jobConfig: JobConfig,
  ): JobSyncConfig {
    val jobConfigType = jobConfig.configType
    if (ConfigType.SYNC == jobConfigType) {
      return jobConfig.sync
    } else if (ConfigType.RESET_CONNECTION == jobConfigType) {
      val resetConnection = jobConfig.resetConnection

      return JobSyncConfig()
        .withNamespaceDefinition(resetConnection.namespaceDefinition)
        .withNamespaceFormat(resetConnection.namespaceFormat)
        .withPrefix(resetConnection.prefix)
        .withSourceDockerImage(WorkerConstants.RESET_JOB_SOURCE_DOCKER_IMAGE_STUB)
        .withDestinationDockerImage(resetConnection.destinationDockerImage)
        .withDestinationProtocolVersion(resetConnection.destinationProtocolVersion)
        .withConfiguredAirbyteCatalog(resetConnection.configuredAirbyteCatalog)
        .withOperationSequence(resetConnection.operationSequence)
        .withSyncResourceRequirements(resetConnection.syncResourceRequirements)
        .withIsSourceCustomConnector(resetConnection.isSourceCustomConnector)
        .withIsDestinationCustomConnector(resetConnection.isDestinationCustomConnector)
        .withWebhookOperationConfigs(resetConnection.webhookOperationConfigs)
        .withWorkspaceId(resetConnection.workspaceId)
    } else if (ConfigType.REFRESH == jobConfigType) {
      val refreshConfig = jobConfig.refresh

      return JobSyncConfig()
        .withNamespaceDefinition(refreshConfig.namespaceDefinition)
        .withNamespaceFormat(refreshConfig.namespaceFormat)
        .withPrefix(refreshConfig.prefix)
        .withSourceDockerImage(refreshConfig.sourceDockerImage)
        .withSourceProtocolVersion(refreshConfig.sourceProtocolVersion)
        .withSourceDefinitionVersionId(refreshConfig.sourceDefinitionVersionId)
        .withDestinationDockerImage(refreshConfig.destinationDockerImage)
        .withDestinationProtocolVersion(refreshConfig.destinationProtocolVersion)
        .withDestinationDefinitionVersionId(refreshConfig.destinationDefinitionVersionId)
        .withConfiguredAirbyteCatalog(refreshConfig.configuredAirbyteCatalog)
        .withOperationSequence(refreshConfig.operationSequence)
        .withSyncResourceRequirements(refreshConfig.syncResourceRequirements)
        .withIsSourceCustomConnector(refreshConfig.isSourceCustomConnector)
        .withIsDestinationCustomConnector(refreshConfig.isDestinationCustomConnector)
        .withWebhookOperationConfigs(refreshConfig.webhookOperationConfigs)
        .withWorkspaceId(refreshConfig.workspaceId)
    } else {
      throw IllegalStateException(
        String.format(
          "Unexpected config type %s for job %d. The only supported config types for this activity are (%s)",
          jobConfigType,
          jobId,
          Job.REPLICATION_TYPES,
        ),
      )
    }
  }

  @Throws(IOException::class)
  private fun getCurrentConnectionState(connectionId: UUID): Optional<State> {
    val state = stateHandler.getState(ConnectionIdRequestBody().connectionId(connectionId))

    if (state.stateType == ConnectionStateType.NOT_SET) {
      return Optional.empty()
    }

    val internalState = toInternal(state)
    return Optional.of(getState(internalState))
  }

  @Throws(IOException::class)
  private fun getSourceIntegrationLauncherConfig(
    jobId: Long,
    attempt: Int,
    connectionId: UUID,
    config: JobSyncConfig,
    @Nullable sourceVersion: ActorDefinitionVersion?,
    sourceConfiguration: JsonNode,
  ): IntegrationLauncherConfig {
    val configReplacer = ConfigReplacer()

    val sourceLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withAttemptId(attempt.toLong())
        .withConnectionId(connectionId)
        .withWorkspaceId(config.workspaceId)
        .withDockerImage(config.sourceDockerImage)
        .withProtocolVersion(config.sourceProtocolVersion)
        .withIsCustomConnector(config.isSourceCustomConnector)

    if (sourceVersion != null) {
      sourceLauncherConfig.allowedHosts = configReplacer.getAllowedHosts(sourceVersion.allowedHosts, sourceConfiguration)
    }

    return sourceLauncherConfig
  }

  @Throws(IOException::class)
  private fun getDestinationIntegrationLauncherConfig(
    jobId: Long,
    attempt: Int,
    connectionId: UUID,
    config: JobSyncConfig,
    destinationVersion: ActorDefinitionVersion,
    destinationConfiguration: JsonNode,
    additionalEnviornmentVariables: Map<String, String>,
  ): IntegrationLauncherConfig {
    val configReplacer = ConfigReplacer()

    return IntegrationLauncherConfig()
      .withJobId(jobId.toString())
      .withAttemptId(attempt.toLong())
      .withConnectionId(connectionId)
      .withWorkspaceId(config.workspaceId)
      .withDockerImage(config.destinationDockerImage)
      .withProtocolVersion(config.destinationProtocolVersion)
      .withIsCustomConnector(config.isDestinationCustomConnector)
      .withAllowedHosts(configReplacer.getAllowedHosts(destinationVersion.allowedHosts, destinationConfiguration))
      .withAdditionalEnvironmentVariables(additionalEnviornmentVariables)
  }

  @Throws(IOException::class)
  private fun getSourceConfiguration(source: SourceConnection): ConfigWithSecretReferences {
    val injectedConfig =
      configInjector.injectConfig(
        oAuthConfigSupplier.injectSourceOAuthParameters(
          source.sourceDefinitionId,
          source.sourceId,
          source.workspaceId,
          source.configuration,
        ),
        source.sourceDefinitionId,
      )
    return secretReferenceService.getConfigWithSecretReferences(ActorId(source.sourceId), injectedConfig, WorkspaceId(source.workspaceId))
  }

  @Throws(IOException::class)
  private fun getDestinationConfiguration(destination: DestinationConnection): ConfigWithSecretReferences {
    val injectedConfig =
      configInjector.injectConfig(
        oAuthConfigSupplier.injectDestinationOAuthParameters(
          destination.destinationDefinitionId,
          destination.destinationId,
          destination.workspaceId,
          destination.configuration,
        ),
        destination.destinationDefinitionId,
      )
    return secretReferenceService.getConfigWithSecretReferences(
      ActorId(destination.destinationId),
      injectedConfig,
      WorkspaceId(destination.workspaceId),
    )
  }

  private fun getNetworkSecurityTokens(workspaceId: UUID): List<String> {
    val scopes = java.util.Map.of(ConfigScopeType.WORKSPACE, workspaceId)
    try {
      val podLabelConfigurations =
        scopedConfigurationService.getScopedConfigurations(NetworkSecurityTokenKey, scopes)
      return podLabelConfigurations.stream().map { obj: ScopedConfiguration -> obj.value }.toList()
    } catch (e: IllegalArgumentException) {
      log.error(e.message)
      return emptyList()
    }
  }

  @InternalForTesting
  fun shouldIncludeFiles(
    jobSyncConfig: JobSyncConfig,
    sourceAdv: ActorDefinitionVersion?,
    destinationAdv: ActorDefinitionVersion?,
  ): Boolean {
    // TODO add compatibility check with sourceAdv and destinationAdv to avoid scanning through all
    // catalogs for nothing
    return jobSyncConfig.configuredAirbyteCatalog.streams
      .stream()
      .anyMatch(ConfiguredAirbyteStream::includeFiles)
  }

  private fun isDeprecatedFileTransfer(sourceConfig: JsonNode?): Boolean {
    if (sourceConfig == null) {
      return false
    }

    val typedSourceConfig = Jsons.`object`(sourceConfig, SourceActorConfig::class.java)
    return typedSourceConfig.useFileTransfer ||
      (typedSourceConfig.deliveryMethod != null && "use_file_transfer" == typedSourceConfig.deliveryMethod.deliveryType)
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
