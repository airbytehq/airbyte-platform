/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.converters.ConfigReplacer
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.security.md5
import io.airbyte.commons.server.errors.ConflictException
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
import io.airbyte.config.JobStatus.Companion.TERMINAL_STATUSES
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
import io.airbyte.workers.models.SpecInput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.server.exceptions.NotFoundException
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.config.ActorType as ConfigActorType

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
  fun getSpecInput(
    dockerImage: String,
    dockerImageTag: String,
    workspaceId: UUID,
    jobId: String?,
    attemptId: Long?,
    isCustomConnector: Boolean = true,
  ): SpecInput {
    val finalJobId = jobId ?: UUID.randomUUID().toString()
    val finalAttemptId = attemptId ?: 0L
    // Because the launcher config input concatenates image and tag
    val finalDockerImage = "$dockerImage:$dockerImageTag"

    return SpecInput(
      jobRunConfig =
        JobRunConfig()
          .withJobId(finalJobId)
          .withAttemptId(finalAttemptId),
      launcherConfig =
        IntegrationLauncherConfig()
          .withJobId(finalJobId)
          .withWorkspaceId(workspaceId)
          .withDockerImage(finalDockerImage)
          .withIsCustomConnector(isCustomConnector)
          .withAttemptId(finalAttemptId),
    )
  }

  fun getSpecInput(
    actorDefinitionId: UUID,
    dockerImageTag: String,
    workspaceId: UUID,
    jobId: String?,
    attemptId: Long?,
  ): SpecInput {
    val actorDefinition =
      actorDefinitionRepository.findByActorDefinitionId(actorDefinitionId)
        ?: throw NotFoundException()

    val (actorDefinitionVersion, isCustomConnector) =
      when (actorDefinition.actorType) {
        ActorType.source -> {
          val sourceDefinition = sourceService.getStandardSourceDefinition(actorDefinitionId)
          val version = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, null)
          Pair(version, sourceDefinition.custom)
        }
        ActorType.destination -> {
          val destinationDefinition = destinationService.getStandardDestinationDefinition(actorDefinitionId)
          val version = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, null)
          Pair(version, destinationDefinition.custom)
        }
        else -> throw IllegalStateException("Actor type ${actorDefinition.actorType} not supported")
      }

    val dockerImage = actorDefinitionVersion.dockerRepository

    return getSpecInput(
      dockerImage = dockerImage,
      dockerImageTag = dockerImageTag,
      workspaceId = workspaceId,
      jobId = jobId,
      attemptId = attemptId,
      isCustomConnector = isCustomConnector,
    )
  }

  /**
   * Unified method to create CheckConnectionInput that supports:
   * 1. Actor ID only (uses stored config)
   * 2. Definition ID + config (no stored actor)
   * 3. Actor ID + config override (hybrid: stored credentials + provided config)
   *
   * @param actorId Optional actor ID - if provided, will use stored credentials
   * @param actorDefinitionId Optional definition ID - required if actorId is null
   * @param workspaceId Optional workspace ID - required if actorId is null
   * @param configuration Optional config - if null and actorId provided, uses stored config
   * @param jobId Optional job ID for tracking
   * @param attemptId Optional attempt ID for tracking
   * @return CheckConnectionInput ready for execution
   */
  fun getCheckInput(
    actorId: UUID? = null,
    actorDefinitionId: UUID? = null,
    workspaceId: UUID? = null,
    configuration: JsonNode? = null,
    jobId: String? = null,
    attemptId: Long? = null,
  ): CheckConnectionInput {
    // Validation
    require(actorId != null || (actorDefinitionId != null && workspaceId != null)) {
      "Must provide either actorId OR (actorDefinitionId + workspaceId)"
    }

    // Gather actor information
    val actorInfo =
      if (actorId != null) {
        val actor =
          actorRepository.findByActorId(actorId)
            ?: throw NotFoundException()

        ActorInfo(
          actorType = actor.actorType,
          actorId = actorId,
          actorDefinitionId = actor.actorDefinitionId,
          workspaceId = actor.workspaceId,
          storedConfiguration =
            when (actor.actorType) {
              ActorType.source -> sourceService.getSourceConnection(actorId).configuration
              ActorType.destination -> destinationService.getDestinationConnection(actorId).configuration
              else -> throw IllegalStateException("Unsupported actor type: ${actor.actorType}")
            },
        )
      } else {
        val actorDefinition =
          actorDefinitionRepository.findByActorDefinitionId(actorDefinitionId!!)
            ?: throw NotFoundException()

        ActorInfo(
          actorType = actorDefinition.actorType,
          actorId = null,
          actorDefinitionId = actorDefinitionId,
          workspaceId = workspaceId!!,
          storedConfiguration = null,
        )
      }

    // Determine configuration to use
    val effectiveConfiguration =
      when {
        configuration != null && actorInfo.storedConfiguration != null -> {
          // Hybrid mode: merge provided config with stored config
          mergeConfigurations(actorInfo.storedConfiguration, configuration)
        }
        configuration != null -> configuration
        actorInfo.storedConfiguration != null -> actorInfo.storedConfiguration
        else -> throw IllegalArgumentException("No configuration provided and no stored configuration available")
      }

    // Get definition metadata
    val (definition, definitionVersion, resourceRequirements) =
      when (actorInfo.actorType) {
        ActorType.source -> {
          val info = getSourceInformationByDefinitionId(actorInfo.actorDefinitionId, actorInfo.workspaceId, actorInfo.actorId)
          Triple(info.sourceDefinition as Any, info.sourceDefinitionVersion, info.resourceRequirements)
        }
        ActorType.destination -> {
          val info = getDestinationInformationByDefinitionId(actorInfo.actorDefinitionId, actorInfo.workspaceId, actorInfo.actorId)
          Triple(info.destinationDefinition as Any, info.destinationDefinitionVersion, info.resourceRequirements)
        }
        else -> throw IllegalStateException("Unsupported actor type: ${actorInfo.actorType}")
      }

    // Inject OAuth parameters
    val configWithOAuth =
      when (actorInfo.actorType) {
        ActorType.source ->
          oAuthConfigSupplier.injectSourceOAuthParameters(
            actorInfo.actorDefinitionId,
            actorInfo.actorId,
            actorInfo.workspaceId,
            effectiveConfiguration,
          )
        ActorType.destination ->
          oAuthConfigSupplier.injectDestinationOAuthParameters(
            actorInfo.actorDefinitionId,
            actorInfo.actorId,
            actorInfo.workspaceId,
            effectiveConfiguration,
          )
        else -> effectiveConfiguration
      }

    // Build actor context
    val actorContext =
      when {
        actorInfo.actorId != null && actorInfo.actorType == ActorType.source -> {
          contextBuilder.fromSource(sourceService.getSourceConnection(actorInfo.actorId))
        }
        actorInfo.actorId != null && actorInfo.actorType == ActorType.destination -> {
          contextBuilder.fromDestination(destinationService.getDestinationConnection(actorInfo.actorId))
        }
        else -> {
          contextBuilder.fromActorDefinitionId(
            actorInfo.actorDefinitionId,
            when (actorInfo.actorType) {
              ActorType.source -> ConfigActorType.SOURCE
              ActorType.destination -> ConfigActorType.DESTINATION
              else -> throw IllegalStateException("Unsupported actor type")
            },
            actorInfo.workspaceId,
          )
        }
      }

    // Build final CheckConnectionInput
    return buildJobCheckConnectionConfig(
      actorType =
        when (actorInfo.actorType) {
          ActorType.source -> ConfigActorType.SOURCE
          ActorType.destination -> ConfigActorType.DESTINATION
          else -> throw IllegalStateException("Unsupported actor type")
        },
      definitionId = actorInfo.actorDefinitionId,
      actorId = actorInfo.actorId,
      workspaceId = actorInfo.workspaceId,
      configuration = configWithOAuth,
      dockerImage = ActorDefinitionVersionHelper.getDockerImageName(definitionVersion),
      protocolVersion = Version(definitionVersion.protocolVersion),
      isCustomConnector =
        when (actorInfo.actorType) {
          ActorType.source -> (definition as StandardSourceDefinition).custom
          ActorType.destination -> (definition as StandardDestinationDefinition).custom
          else -> false
        },
      resourceRequirements = resourceRequirements,
      allowedHosts = definitionVersion.allowedHosts,
      actorContext = actorContext,
      jobId = jobId,
      attemptId = attemptId,
    )
  }

  /**
   * Internal data class to hold actor information during check input creation
   */
  private data class ActorInfo(
    val actorType: ActorType,
    val actorId: UUID?,
    val actorDefinitionId: UUID,
    val workspaceId: UUID,
    val storedConfiguration: JsonNode?,
  )

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
    val isDeprecatedFileTransfer = isDeprecatedFileTransfer(currentAttempt.syncConfig?.sourceConfiguration)
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
      throw ConflictException(
        "Cannot create replication command for job $jobId in terminal state ${job.status}. Commands can only be created for active jobs.",
      )
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
      ActorType.destination ->
        if (jobId == null && attemptId == null) {
          getDiscoverInputByDestinationId(destinationId = actorId, jobId = UUID.randomUUID().toString(), attemptId = 0L, true)
        } else {
          getDiscoverInputByDestinationId(actorId, jobId!!, attemptId!!, false)
        }
      else -> throw IllegalStateException("Actor type ${actor.actorType} not supported")
    }
  }

  /**
   * Merges a provided configuration with stored configuration.
   * Strategy: Prioritize user-provided fields (including sanitized secrets),
   * but preserve secrets from stored config for fields NOT provided by user.
   *
   * This allows callers to test new configuration values (including updated secrets)
   * while using existing credentials for fields they don't specify.
   */
  @InternalForTesting
  internal fun mergeConfigurations(
    storedConfig: JsonNode,
    providedConfig: JsonNode,
  ): JsonNode {
    // Start with stored config (has full secrets)
    val merged = storedConfig.deepCopy() as com.fasterxml.jackson.databind.node.ObjectNode

    // Overlay all provided config fields (secrets are already sanitized by this point)
    providedConfig.fields().forEach { (key, value) ->
      merged.set<JsonNode>(key, value)
    }

    return merged
  }

  private fun buildJobCheckConnectionConfig(
    actorType: ConfigActorType,
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
    actorType: ConfigActorType,
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
    val hashedConfiguration = Jsons.serialize(source.configuration).toByteArray(Charsets.UTF_8).md5()

    return buildJobDiscoverConfig(
      actorType = ConfigActorType.SOURCE,
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

  private fun getDiscoverInputByDestinationId(
    destinationId: UUID,
    jobId: String,
    attemptId: Long,
    isManual: Boolean,
  ): DiscoverCommandInput.DiscoverCatalogInput {
    val destination = destinationService.getDestinationConnection(destinationId)
    val destinationInformation = getDestinationInformation(destinationId)
    val destinationDefinition = destinationInformation.destinationDefinition
    val destinationDefinitionVersion = destinationInformation.destinationDefinitionVersion

    val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(destinationDefinitionVersion)
    val configWithOauthParams: JsonNode =
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        destinationDefinition.destinationDefinitionId,
        destination.destinationId,
        destination.workspaceId,
        destination.configuration,
      )
    val hashedConfiguration = Jsons.serialize(destination.configuration).toByteArray(Charsets.UTF_8).md5()

    return buildJobDiscoverConfig(
      actorType = ConfigActorType.DESTINATION,
      definitionId = destination.destinationDefinitionId,
      actorId = destination.destinationId,
      workspaceId = destination.workspaceId,
      configuration = configWithOauthParams,
      hashedConfiguration = hashedConfiguration,
      dockerImage = dockerImage,
      dockerTag = destinationDefinitionVersion.dockerImageTag,
      protocolVersion = Version(destinationDefinitionVersion.protocolVersion),
      isCustomConnector = destinationDefinition.custom,
      resourceRequirements = destinationInformation.resourceRequirements,
      allowedHosts = destinationDefinitionVersion.allowedHosts,
      actorContext = contextBuilder.fromDestination(destination),
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
    actorId: UUID?,
  ): SourceDefinitionInformation {
    val sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId)
    val sourceDefinitionVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, actorId)
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
      destinationDefinition,
      destinationDefinitionVersion,
      resourceRequirements,
    )
  }

  private fun getDestinationInformationByDefinitionId(
    destinationDefinitionId: UUID,
    workspaceId: UUID,
    actorId: UUID?,
  ): DestinationInformation {
    val destinationDefinition = destinationService.getStandardDestinationDefinition(destinationDefinitionId)
    val destinationDefinitionVersion = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, actorId)
    val resourceRequirements =
      ResourceRequirementsUtils.getResourceRequirementsForJobType(
        destinationDefinition.resourceRequirements,
        JobTypeResourceLimit.JobType.CHECK_CONNECTION,
      )

    return DestinationInformation(
      destinationDefinition,
      destinationDefinitionVersion,
      resourceRequirements,
    )
  }
}
