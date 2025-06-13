/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.hash.Hashing
import io.airbyte.commons.converters.ConfigReplacer
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.helpers.ContextBuilder
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorContext
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AllowedHosts
import io.airbyte.config.Attempt
import io.airbyte.config.CatalogDiff
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus.TERMINAL_STATUSES
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.JobTypeResourceLimit
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.SourceActorConfig
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.config.helpers.ResourceRequirementsUtils
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.config.secrets.InlinedConfigWithSecretRefs
import io.airbyte.config.secrets.toInlined
import io.airbyte.data.repositories.ActorDefinitionRepository
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.AttemptService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.NetworkSecurityTokenKey
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.Destination
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Source
import io.airbyte.featureflag.SourceType
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.ReplicationFeatureFlags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.server.exceptions.NotFoundException
import jakarta.inject.Singleton
import java.util.UUID
import kotlin.jvm.optionals.getOrNull

val log = KotlinLogging.logger { }

@Singleton
class JobInputService(
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val actorRepository: ActorRepository,
  private val actorDefinitionRepository: ActorDefinitionRepository,
  private val oAuthConfigSupplier: OAuthConfigSupplier,
  private val configInjector: ConfigInjector,
  private val secretReferenceService: SecretReferenceService,
  private val contextBuilder: ContextBuilder,
  private val scopedConfigurationService: ScopedConfigurationService,
  private val connectionService: ConnectionService,
  private val jobService: JobService,
  private val replicationFeatureFlags: ReplicationFeatureFlags,
  private val featureFlagClient: FeatureFlagClient,
  private val attemptService: AttemptService,
) {
  companion object {
    private val HASH_FUNCTION = Hashing.md5()
  }

  fun getCheckInput(
    actorId: UUID,
    jobId: String?,
    attemptId: Long?,
  ): CheckConnectionInput {
    val actor =
      actorRepository.findByActorId(actorId)
        ?: throw NotFoundException() // Better exception?
    return when (actor.actorType) {
      ActorType.source -> getCheckInputBySourceId(actorId, jobId, attemptId)
      ActorType.destination -> getCheckInputByDestinationId(actorId, jobId, attemptId)
      else -> throw IllegalStateException("Actor type ${actor.actorType} not supported")
    }
  }

  fun getCheckInput(
    actorDefinitionId: UUID,
    workspaceId: UUID,
    configuration: JsonNode,
  ): CheckConnectionInput {
    val actorDefinition =
      actorDefinitionRepository.findByActorDefinitionId(actorDefinitionId)
        ?: throw NotFoundException() // Better exception?

    return when (actorDefinition.actorType) {
      ActorType.source -> getCheckInputBySourceDefinitionId(actorDefinitionId, workspaceId, configuration)
      ActorType.destination -> getCheckInputByDestinationDefinitionId(actorDefinitionId, workspaceId, configuration)
      else -> throw IllegalStateException("Actor type ${actorDefinition.actorType} not supported")
    }
  }

  fun getReplicationInput(
    connectionId: UUID,
    appliedCatalogDiff: CatalogDiff?,
    signalInput: String?,
    jobId: Long,
    attemptNumber: Long,
  ): ReplicationActivityInput {
    val (
      connection: StandardSync,
      source: SourceConnection,
      sourceDefinition: StandardSourceDefinition,
      sourceDefinitionVersion: ActorDefinitionVersion,
      destination: DestinationConnection,
      destinationDefinition: StandardDestinationDefinition,
      destinationDefinitionVersion: ActorDefinitionVersion,
    ) = getConnectionAndActors(connectionId)

    val (currentJob: Job, currentAttempt: Attempt) = getCurrentJobAndAttempt(jobId, attemptNumber)

    val sourceIntegrationLauncherConfig =
      getIntegrationLauncherConfig(
        jobId = currentJob.id.toString(),
        workspaceId = source.workspaceId,
        dockerImage = ActorDefinitionVersionHelper.getDockerImageName(sourceDefinitionVersion),
        protocolVersion = Version(sourceDefinitionVersion.protocolVersion),
        isCustomConnector = sourceDefinition.custom,
        attemptId = attemptNumber,
        allowedHosts = sourceDefinitionVersion.allowedHosts,
        connectionId = connectionId,
      )

    val destinationIntegrationLauncherConfig =
      getIntegrationLauncherConfig(
        jobId = currentJob.id.toString(),
        workspaceId = destination.workspaceId,
        dockerImage = ActorDefinitionVersionHelper.getDockerImageName(destinationDefinitionVersion),
        protocolVersion = Version(destinationDefinitionVersion.protocolVersion),
        isCustomConnector = destinationDefinition.custom,
        attemptId = attemptNumber,
        allowedHosts = destinationDefinitionVersion.allowedHosts,
        connectionId = connectionId,
      )

    val jobConfigData = getJobConfig(currentJob, currentAttempt)

    val featureFlags =
      resolveFeatureFlags(
        workspaceId = source.workspaceId,
        connectionId = connectionId,
        sourceId = source.sourceId,
        destinationId = destination.destinationId,
        sourceType = sourceDefinition.sourceType,
      )

    return ReplicationActivityInput(
      sourceId = connection.sourceId,
      destinationId = connection.destinationId,
      sourceConfiguration = source.configuration,
      destinationConfiguration = destination.configuration,
      jobRunConfig =
        JobRunConfig()
          .withJobId(currentJob.id.toString())
          .withAttemptId(attemptNumber),
      sourceLauncherConfig = sourceIntegrationLauncherConfig,
      destinationLauncherConfig = destinationIntegrationLauncherConfig,
      syncResourceRequirements = jobConfigData.syncResourceRequirements,
      workspaceId = source.workspaceId,
      connectionId = connectionId,
      taskQueue = currentAttempt.processingTaskQueue,
      isReset = currentJob.config.configType == JobConfig.ConfigType.RESET_CONNECTION || currentJob.config.configType == JobConfig.ConfigType.CLEAR,
      namespaceDefinition = jobConfigData.namespaceDefinition,
      namespaceFormat = jobConfigData.namespaceFormat,
      prefix = jobConfigData.prefix,
      connectionContext = contextBuilder.fromConnectionId(connectionId),
      signalInput = signalInput,
      networkSecurityTokens = getNetworkSecurityTokens(workspaceId = source.workspaceId),
      includesFiles = jobConfigData.includeFiles,
      omitFileTransferEnvVar = jobConfigData.omitFileTransferEnvVar,
      featureFlags = featureFlags,
      heartbeatMaxSecondsBetweenMessages = sourceDefinition.maxSecondsBetweenMessages,
      supportsRefreshes = destinationDefinitionVersion.supportsRefreshes,
      schemaRefreshOutput = appliedCatalogDiff?.let { RefreshSchemaActivityOutput(appliedDiff = it) },
      sourceIPCOptions = sourceDefinitionVersion.connectorIPCOptions,
      destinationIPCOptions = destinationDefinitionVersion.connectorIPCOptions,
    )
  }

  private data class JobConfigData(
    val syncResourceRequirements: SyncResourceRequirements,
    val namespaceDefinition: JobSyncConfig.NamespaceDefinitionType?,
    val namespaceFormat: String?,
    val prefix: String?,
    val includeFiles: Boolean,
    val omitFileTransferEnvVar: Boolean,
  )

  private fun getJobConfig(
    currentJob: Job,
    currentAttempt: Attempt,
  ): JobConfigData {
    val isDeprecatedFileTransfer = isDeprecatedFileTransfer(currentAttempt.syncConfig.map { it.sourceConfiguration }.getOrNull())
    return when (currentJob.config.configType) {
      JobConfig.ConfigType.SYNC -> {
        val includeFiles =
          currentJob.config.sync.configuredAirbyteCatalog
            ?.streams
            ?.any { it.includeFiles } ?: false

        JobConfigData(
          syncResourceRequirements = currentJob.config.sync.syncResourceRequirements,
          namespaceDefinition = currentJob.config.sync.namespaceDefinition,
          namespaceFormat = currentJob.config.sync.namespaceFormat,
          prefix = currentJob.config.sync.prefix,
          includeFiles = includeFiles || isDeprecatedFileTransfer,
          omitFileTransferEnvVar = includeFiles,
        )
      }

      JobConfig.ConfigType.REFRESH -> {
        val includeFile =
          currentJob.config.refresh.configuredAirbyteCatalog
            ?.streams
            ?.any { it.includeFiles } ?: false
        JobConfigData(
          syncResourceRequirements = currentJob.config.refresh.syncResourceRequirements,
          namespaceDefinition = currentJob.config.refresh.namespaceDefinition,
          namespaceFormat = currentJob.config.refresh.namespaceFormat,
          prefix = currentJob.config.refresh.prefix,
          includeFiles = includeFile || isDeprecatedFileTransfer,
          omitFileTransferEnvVar = includeFile,
        )
      }

      JobConfig.ConfigType.CLEAR, JobConfig.ConfigType.RESET_CONNECTION -> {
        val includeFile =
          currentJob.config.resetConnection.configuredAirbyteCatalog
            ?.streams
            ?.any { it.includeFiles } ?: false
        JobConfigData(
          syncResourceRequirements = currentJob.config.resetConnection.syncResourceRequirements,
          namespaceDefinition = currentJob.config.resetConnection.namespaceDefinition,
          namespaceFormat = currentJob.config.resetConnection.namespaceFormat,
          prefix = currentJob.config.resetConnection.prefix,
          includeFiles = includeFile || isDeprecatedFileTransfer,
          omitFileTransferEnvVar = includeFile,
        )
      }
      else -> throw UnsupportedOperationException()
    }
  }

  private fun isDeprecatedFileTransfer(sourceConfig: JsonNode?): Boolean {
    if (sourceConfig == null) {
      return false
    }

    val typedSourceConfig = Jsons.`object`(sourceConfig, SourceActorConfig::class.java)
    return (
      typedSourceConfig.useFileTransfer ||
        (typedSourceConfig.deliveryMethod != null && "use_file_transfer" == typedSourceConfig.deliveryMethod.deliveryType)
    )
  }

  private fun getFeatureFlagContext(
    workspaceId: UUID,
    connectionId: UUID,
    sourceId: UUID,
    destinationId: UUID,
    sourceType: StandardSourceDefinition.SourceType?,
  ): Context {
    val contexts: MutableList<Context> = mutableListOf()
    if (workspaceId != null) {
      contexts.add(Workspace(workspaceId))
    }
    if (connectionId != null) {
      contexts.add(Connection(connectionId))
    }
    if (sourceId != null) {
      contexts.add(Source(sourceId))
    }
    if (destinationId != null) {
      contexts.add(Destination(destinationId))
    }
    if (sourceType != null) {
      contexts.add(SourceType(sourceType.value()))
    }
    return Multi(contexts)
  }

  private fun resolveFeatureFlags(
    workspaceId: UUID,
    connectionId: UUID,
    sourceId: UUID,
    destinationId: UUID,
    sourceType: StandardSourceDefinition.SourceType?,
  ): Map<String, Any> {
    val context =
      getFeatureFlagContext(
        workspaceId = workspaceId,
        connectionId = connectionId,
        sourceId = sourceId,
        destinationId = destinationId,
        sourceType = sourceType,
      )
    return replicationFeatureFlags.featureFlags.associate { flag -> flag.key to featureFlagClient.variation(flag, context)!! }
  }

  private data class JobAndAttempt(
    val latestJob: Job,
    val latestAttempt: Attempt,
  )

  private fun getCurrentJobAndAttempt(
    jobId: Long,
    attemptNumber: Long,
  ): JobAndAttempt {
    val job =
      jobService.findById(jobId)
        ?: throw NotFoundException()

    if (TERMINAL_STATUSES.contains(job.status)) {
      throw IllegalStateException("Cannot create replication input for a non-terminal job. Job status: ${job.status}")
    }

    val attempt = attemptService.getAttempt(jobId, attemptNumber)

    return JobAndAttempt(job, attempt)
  }

  private data class ConnectionAndActors(
    val connection: StandardSync,
    val source: SourceConnection,
    val sourceDefinition: StandardSourceDefinition,
    val sourceDefinitionVersion: ActorDefinitionVersion,
    val destination: DestinationConnection,
    val destinationDefinition: StandardDestinationDefinition,
    val destinationDefinitionVersion: ActorDefinitionVersion,
  )

  private fun getConnectionAndActors(connectionId: UUID): ConnectionAndActors {
    val connection = connectionService.getStandardSync(connectionId) ?: throw NotFoundException()
    val source = sourceService.getSourceConnection(connection.sourceId) ?: throw NotFoundException()
    val sourceDefinition = sourceService.getStandardSourceDefinition(source.sourceDefinitionId) ?: throw NotFoundException()
    val sourceDefinitionVersion =
      actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId) ?: throw NotFoundException()
    val destination = destinationService.getDestinationConnection(connection.destinationId) ?: throw NotFoundException()
    val destinationDefinition = destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId) ?: throw NotFoundException()
    val destinationDefinitionVersion =
      actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.workspaceId, destination.destinationId)
        ?: throw NotFoundException()
    return ConnectionAndActors(
      connection,
      source,
      sourceDefinition,
      sourceDefinitionVersion,
      destination,
      destinationDefinition,
      destinationDefinitionVersion,
    )
  }

  fun getDiscoverInput(
    actorId: UUID,
    jobId: String? = null,
    attemptId: Long? = null,
  ): DiscoverCommandInput.DiscoverCatalogInput {
    if ((jobId == null && attemptId != null) || (jobId != null && attemptId == null)) {
      throw IllegalStateException("jobId and attemptId must be both null or both not null; jobId: $jobId, attemptId: $attemptId")
    }

    val actor =
      actorRepository.findByActorId(actorId)
        ?: throw NotFoundException() // Better exception?
    return when (actor.actorType) {
      ActorType.source ->
        if (jobId == null && attemptId == null) {
          getDiscoverInputBySourceId(sourceId = actorId, jobId = UUID.randomUUID().toString(), attemptId = 0L, true)
        } else {
          getDiscoverInputBySourceId(actorId, jobId!!, attemptId!!, false)
        }
      ActorType.destination -> throw IllegalArgumentException("Discovery is not supported for destination, actorId: $actorId")
      else -> throw IllegalStateException("Actor type ${actor.actorType} not supported")
    }
  }

  private fun getCheckInputBySourceId(
    sourceId: UUID,
    jobId: String?,
    attemptId: Long?,
  ): CheckConnectionInput {
    val (source, sourceDefinition, sourceDefinitionVersion, resourceRequirements) = getSourceInformation(sourceId)

    val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(sourceDefinitionVersion)
    val configWithOauthParams: JsonNode =
      oAuthConfigSupplier.injectSourceOAuthParameters(
        sourceDefinition.sourceDefinitionId,
        source.sourceId,
        source.workspaceId,
        source.configuration,
      )

    return buildJobCheckConnectionConfig(
      actorType = io.airbyte.config.ActorType.SOURCE,
      definitionId = source.sourceDefinitionId,
      actorId = source.sourceId,
      workspaceId = source.workspaceId,
      configuration = configWithOauthParams,
      dockerImage = dockerImage,
      protocolVersion = Version(sourceDefinitionVersion.protocolVersion),
      isCustomConnector = sourceDefinition.custom,
      resourceRequirements = resourceRequirements,
      allowedHosts = sourceDefinitionVersion.allowedHosts,
      actorContext = contextBuilder.fromSource(source),
      jobId = jobId,
      attemptId = attemptId,
    )
  }

  private fun getCheckInputBySourceDefinitionId(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
    configuration: JsonNode,
  ): CheckConnectionInput {
    val (sourceDefinition, sourceDefinitionVersion, resourceRequirements) = getSourceInformationByDefinitionId(sourceDefinitionId, workspaceId)

    val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(sourceDefinitionVersion)
    val configWithOauthParams: JsonNode =
      oAuthConfigSupplier.maskSourceOAuthParameters(
        sourceDefinition.sourceDefinitionId,
        workspaceId,
        configuration,
        sourceDefinitionVersion.spec,
      )

    val jobId = UUID.randomUUID().toString()
    val attemptId = 0L
    val actorType = io.airbyte.config.ActorType.SOURCE

    return buildJobCheckConnectionConfig(
      actorType = actorType,
      definitionId = sourceDefinition.sourceDefinitionId,
      actorId = null,
      workspaceId = workspaceId,
      configuration = configWithOauthParams,
      dockerImage = dockerImage,
      protocolVersion = Version(sourceDefinitionVersion.protocolVersion),
      isCustomConnector = sourceDefinition.custom,
      resourceRequirements = resourceRequirements,
      allowedHosts = sourceDefinitionVersion.allowedHosts,
      actorContext = contextBuilder.fromActorDefinitionId(sourceDefinition.sourceDefinitionId, actorType, workspaceId),
      jobId = jobId,
      attemptId = attemptId,
    )
  }

  private fun getCheckInputByDestinationDefinitionId(
    destinationDefinitionId: UUID,
    workspaceId: UUID,
    configuration: JsonNode,
  ): CheckConnectionInput {
    val destinationInformation = getDestinationInformationByDefinitionId(destinationDefinitionId, workspaceId)
    val destinationDefinition = destinationInformation.destinationDefinition

    val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(destinationInformation.destinationDefinitionVersion)
    val configWithOauthParams: JsonNode =
      oAuthConfigSupplier.maskDestinationOAuthParameters(
        destinationDefinition.destinationDefinitionId,
        workspaceId,
        configuration,
        destinationInformation.destinationDefinitionVersion.spec,
      )

    val jobId = UUID.randomUUID().toString()
    val attemptId = 0L
    val actorType = io.airbyte.config.ActorType.DESTINATION

    return buildJobCheckConnectionConfig(
      actorType = actorType,
      definitionId = destinationDefinition.destinationDefinitionId,
      actorId = null,
      workspaceId = workspaceId,
      configuration = configWithOauthParams,
      dockerImage = dockerImage,
      protocolVersion = Version(destinationInformation.destinationDefinitionVersion.protocolVersion),
      isCustomConnector = destinationInformation.destinationDefinition.custom,
      resourceRequirements = destinationInformation.resourceRequirements,
      allowedHosts = destinationInformation.destinationDefinitionVersion.allowedHosts,
      actorContext = contextBuilder.fromActorDefinitionId(destinationDefinition.destinationDefinitionId, actorType, workspaceId),
      jobId = jobId,
      attemptId = attemptId,
    )
  }

  private fun getCheckInputByDestinationId(
    destinationId: UUID,
    jobId: String?,
    attemptId: Long?,
  ): CheckConnectionInput {
    val destinationInformation = getDestinationInformation(destinationId)
    val destination = destinationInformation.destination
    val destinationDefinition = destinationInformation.destinationDefinition

    val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(destinationInformation.destinationDefinitionVersion)
    val configWithOauthParams: JsonNode =
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        destinationDefinition.destinationDefinitionId,
        destination!!.destinationId,
        destination!!.workspaceId,
        destination!!.configuration,
      )

    return buildJobCheckConnectionConfig(
      actorType = io.airbyte.config.ActorType.DESTINATION,
      definitionId = destination.destinationDefinitionId,
      actorId = destination.destinationId,
      workspaceId = destination.workspaceId,
      configuration = configWithOauthParams,
      dockerImage = dockerImage,
      protocolVersion = Version(destinationInformation.destinationDefinitionVersion.protocolVersion),
      isCustomConnector = destinationInformation.destinationDefinition.custom,
      resourceRequirements = destinationInformation.resourceRequirements,
      allowedHosts = destinationInformation.destinationDefinitionVersion.allowedHosts,
      actorContext = contextBuilder.fromDestination(destination),
      jobId = jobId,
      attemptId = attemptId,
    )
  }

  private fun buildJobCheckConnectionConfig(
    actorType: io.airbyte.config.ActorType,
    definitionId: UUID,
    actorId: UUID?,
    workspaceId: UUID,
    configuration: JsonNode,
    dockerImage: String,
    protocolVersion: Version,
    isCustomConnector: Boolean,
    resourceRequirements: ResourceRequirements?,
    allowedHosts: AllowedHosts?,
    actorContext: ActorContext,
    jobId: String?,
    attemptId: Long?,
  ): CheckConnectionInput {
    val injectedConfig: JsonNode = configInjector.injectConfig(configuration, definitionId)

    val inlinedConfigWithSecrets: InlinedConfigWithSecretRefs =
      if (actorId == null) {
        InlinedConfigWithSecretRefs(injectedConfig)
      } else {
        secretReferenceService
          .getConfigWithSecretReferences(
            ActorId(actorId),
            injectedConfig,
            WorkspaceId(workspaceId),
          ).toInlined()
      }

    val jobId = jobId ?: UUID.randomUUID().toString()
    val attemptId = attemptId ?: 0L

    return CheckConnectionInput(
      jobRunConfig =
        JobRunConfig()
          .withJobId(jobId)
          .withAttemptId(attemptId),
      launcherConfig =
        getIntegrationLauncherConfig(
          jobId = jobId,
          workspaceId = workspaceId,
          dockerImage = dockerImage,
          protocolVersion = protocolVersion,
          isCustomConnector = isCustomConnector,
          attemptId = attemptId,
          allowedHosts = allowedHosts,
          connectionId = null,
        ),
      checkConnectionInput =
        StandardCheckConnectionInput()
          .withActorType(actorType)
          .withActorId(actorId)
          .withConnectionConfiguration(inlinedConfigWithSecrets.value)
          .withResourceRequirements(resourceRequirements)
          .withActorContext(actorContext)
          .withNetworkSecurityTokens(getNetworkSecurityTokens(workspaceId)),
    )
  }

  private fun getIntegrationLauncherConfig(
    jobId: String,
    workspaceId: UUID,
    dockerImage: String,
    protocolVersion: Version,
    isCustomConnector: Boolean,
    attemptId: Long,
    allowedHosts: AllowedHosts?,
    connectionId: UUID?,
  ): IntegrationLauncherConfig =
    IntegrationLauncherConfig()
      .withJobId(jobId)
      .withWorkspaceId(workspaceId)
      .withDockerImage(dockerImage)
      .withProtocolVersion(protocolVersion)
      .withIsCustomConnector(isCustomConnector)
      .withAttemptId(attemptId)
      .withAllowedHosts(allowedHosts)
      .withConnectionId(connectionId)

  private fun buildJobDiscoverConfig(
    actorType: io.airbyte.config.ActorType,
    definitionId: UUID,
    actorId: UUID?,
    workspaceId: UUID,
    configuration: JsonNode,
    hashedConfiguration: String,
    dockerImage: String,
    dockerTag: String,
    protocolVersion: Version,
    isCustomConnector: Boolean,
    resourceRequirements: ResourceRequirements?,
    allowedHosts: AllowedHosts?,
    actorContext: ActorContext?,
    jobId: String,
    attemptId: Long,
    isManual: Boolean,
  ): DiscoverCommandInput.DiscoverCatalogInput {
    val injectedConfig: JsonNode = configInjector.injectConfig(configuration, definitionId)

    val inlinedConfigWithSecrets: InlinedConfigWithSecretRefs =
      if (actorId == null) {
        InlinedConfigWithSecretRefs(injectedConfig)
      } else {
        secretReferenceService
          .getConfigWithSecretReferences(
            ActorId(actorId),
            injectedConfig,
            WorkspaceId(workspaceId),
          ).toInlined()
      }

    val configReplacer = ConfigReplacer()

    return DiscoverCommandInput.DiscoverCatalogInput(
      jobRunConfig =
        JobRunConfig()
          .withJobId(jobId)
          .withAttemptId(attemptId),
      integrationLauncherConfig =
        IntegrationLauncherConfig()
          .withJobId(jobId)
          .withWorkspaceId(workspaceId)
          .withDockerImage(dockerImage)
          .withProtocolVersion(protocolVersion)
          .withIsCustomConnector(isCustomConnector)
          .withAttemptId(attemptId)
          .withAllowedHosts(configReplacer.getAllowedHosts(allowedHosts, configuration)),
      discoverCatalogInput =
        StandardDiscoverCatalogInput()
          .withSourceId(actorId.toString())
          .withConnectorVersion(dockerTag)
          .withConnectionConfiguration(inlinedConfigWithSecrets.value)
          .withConfigHash(hashedConfiguration)
          .withManual(isManual)
          .withResourceRequirements(resourceRequirements)
          .withActorContext(actorContext)
          .withNetworkSecurityTokens(getNetworkSecurityTokens(workspaceId)),
    )
  }

  private fun getDiscoverInputBySourceId(
    sourceId: UUID,
    jobId: String,
    attemptId: Long,
    isManual: Boolean,
  ): DiscoverCommandInput.DiscoverCatalogInput {
    val (source, sourceDefinition, sourceDefinitionVersion, resourceRequirements) = getSourceInformation(sourceId)

    val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(sourceDefinitionVersion)
    val configWithOauthParams: JsonNode =
      oAuthConfigSupplier.injectSourceOAuthParameters(
        sourceDefinition.sourceDefinitionId,
        source.sourceId,
        source.workspaceId,
        source.configuration,
      )
    val hashedConfiguration = HASH_FUNCTION.hashBytes(Jsons.serialize(source.configuration).toByteArray(Charsets.UTF_8)).toString()

    return buildJobDiscoverConfig(
      actorType = io.airbyte.config.ActorType.SOURCE,
      definitionId = source.sourceDefinitionId,
      actorId = source.sourceId,
      workspaceId = source.workspaceId,
      configuration = configWithOauthParams,
      hashedConfiguration = hashedConfiguration,
      dockerImage = dockerImage,
      dockerTag = sourceDefinitionVersion.dockerImageTag,
      protocolVersion = Version(sourceDefinitionVersion.protocolVersion),
      isCustomConnector = sourceDefinition.custom,
      resourceRequirements = resourceRequirements,
      allowedHosts = sourceDefinitionVersion.allowedHosts,
      actorContext = contextBuilder.fromSource(source),
      jobId = jobId,
      attemptId = attemptId,
      isManual = isManual,
    )
  }

  private fun getNetworkSecurityTokens(workspaceId: UUID): List<String> =
    try {
      scopedConfigurationService
        .getScopedConfigurations(NetworkSecurityTokenKey, mapOf(ConfigScopeType.WORKSPACE to workspaceId))
        .map { it.value }
        .toList()
    } catch (e: IllegalArgumentException) {
      log.error { e.message }
      emptyList()
    }

  private data class SourceInformation(
    val source: SourceConnection,
    val sourceDefinition: StandardSourceDefinition,
    val sourceDefinitionVersion: ActorDefinitionVersion,
    val resourceRequirements: ResourceRequirements?,
  )

  private data class SourceDefinitionInformation(
    val sourceDefinition: StandardSourceDefinition,
    val sourceDefinitionVersion: ActorDefinitionVersion,
    val resourceRequirements: ResourceRequirements?,
  )

  private fun getSourceInformation(sourceId: UUID): SourceInformation {
    val source = sourceService.getSourceConnection(sourceId)
    val sourceDefinition = sourceService.getStandardSourceDefinition(source.sourceDefinitionId)
    val sourceDefinitionVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, sourceId)
    val resourceRequirements =
      ResourceRequirementsUtils.getResourceRequirementsForJobType(
        sourceDefinition.resourceRequirements,
        JobTypeResourceLimit.JobType.CHECK_CONNECTION,
      )

    return SourceInformation(
      source,
      sourceDefinition,
      sourceDefinitionVersion,
      resourceRequirements,
    )
  }

  private fun getSourceInformationByDefinitionId(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
  ): SourceDefinitionInformation {
    val sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId)
    val sourceDefinitionVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, null)
    val resourceRequirements =
      ResourceRequirementsUtils.getResourceRequirementsForJobType(
        sourceDefinition.resourceRequirements,
        JobTypeResourceLimit.JobType.CHECK_CONNECTION,
      )

    return SourceDefinitionInformation(
      sourceDefinition,
      sourceDefinitionVersion,
      resourceRequirements,
    )
  }

  private data class DestinationInformation(
    val destination: DestinationConnection?,
    val destinationDefinition: StandardDestinationDefinition,
    val destinationDefinitionVersion: ActorDefinitionVersion,
    val resourceRequirements: ResourceRequirements?,
  )

  private fun getDestinationInformation(destinationId: UUID): DestinationInformation {
    val destination = destinationService.getDestinationConnection(destinationId)
    val destinationDefinition = destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId)
    val destinationDefinitionVersion =
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        destinationId,
      )
    val resourceRequirements =
      ResourceRequirementsUtils.getResourceRequirementsForJobType(
        destinationDefinition.resourceRequirements,
        JobTypeResourceLimit.JobType.CHECK_CONNECTION,
      )

    return DestinationInformation(
      destination,
      destinationDefinition,
      destinationDefinitionVersion,
      resourceRequirements,
    )
  }

  private fun getDestinationInformationByDefinitionId(
    destinationDefinitionId: UUID,
    workspaceId: UUID,
  ): DestinationInformation {
    val destinationDefinition = destinationService.getStandardDestinationDefinition(destinationDefinitionId)
    val destinationDefinitionVersion = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, null)
    val resourceRequirements =
      ResourceRequirementsUtils.getResourceRequirementsForJobType(
        destinationDefinition.resourceRequirements,
        JobTypeResourceLimit.JobType.CHECK_CONNECTION,
      )

    return DestinationInformation(
      null,
      destinationDefinition,
      destinationDefinitionVersion,
      resourceRequirements,
    )
  }
}
