/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler

import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Charsets
import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.lang.Exceptions
import io.airbyte.commons.server.handlers.helpers.ContextBuilder
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.commons.temporal.TemporalJobType
import io.airbyte.commons.temporal.TemporalResponse
import io.airbyte.commons.temporal.TemporalTaskQueueUtils.getTaskQueue
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.DestinationConnection
import io.airbyte.config.JobCheckConnectionConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobDiscoverCatalogConfig
import io.airbyte.config.JobGetSpecConfig
import io.airbyte.config.JobTypeResourceLimit
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.helpers.ResourceRequirementsUtils.getResourceRequirementsForJobType
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.Companion.getDockerImageName
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.config.secrets.buildConfigWithSecretRefsJava
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.persistence.job.errorreporter.ConnectorJobReportingContext
import io.airbyte.persistence.job.errorreporter.JobErrorReporter
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.tracker.JobTracker
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.Nullable
import java.io.IOException
import java.time.Instant
import java.util.UUID
import java.util.function.Function
import java.util.function.Supplier

/**
 * Temporal job client for synchronous jobs (i.e. spec, check, discover).
 */
class DefaultSynchronousSchedulerClient(
  private val temporalClient: TemporalClient,
  private val jobTracker: JobTracker,
  private val jobErrorReporter: JobErrorReporter,
  private val oAuthConfigSupplier: OAuthConfigSupplier,
  private val configInjector: ConfigInjector,
  private val contextBuilder: ContextBuilder,
  private val secretReferenceService: SecretReferenceService,
) : SynchronousSchedulerClient {
  @Throws(IOException::class)
  override fun createSourceCheckConnectionJob(
    source: SourceConnection,
    sourceVersion: ActorDefinitionVersion,
    isCustomConnector: Boolean,
    resourceRequirements: ResourceRequirements?,
  ): SynchronousResponse<StandardCheckConnectionOutput> {
    val dockerImage = getDockerImageName(sourceVersion)
    val configWithOauthParams =
      oAuthConfigSupplier.injectSourceOAuthParameters(
        source.sourceDefinitionId,
        source.sourceId,
        source.workspaceId,
        source.configuration,
      )
    val injectedConfig = configInjector.injectConfig(configWithOauthParams, source.sourceDefinitionId)

    val sourceConfig =
      if (source.sourceId == null) {
        buildConfigWithSecretRefsJava(injectedConfig)
      } else {
        secretReferenceService.getConfigWithSecretReferences(
          ActorId(source.sourceId),
          injectedConfig,
          WorkspaceId(source.workspaceId),
        )
      }

    val jobCheckConnectionConfig =
      JobCheckConnectionConfig()
        .withActorType(ActorType.SOURCE)
        .withActorId(source.sourceId)
        .withConnectionConfiguration(sourceConfig)
        .withDockerImage(dockerImage)
        .withProtocolVersion(Version(sourceVersion.protocolVersion))
        .withIsCustomConnector(isCustomConnector)
        .withResourceRequirements(resourceRequirements)

    val jobId = UUID.randomUUID()
    val jobReportingContext =
      ConnectorJobReportingContext(jobId, dockerImage, sourceVersion.releaseStage, sourceVersion.internalSupportLevel)
    val taskQueue = getTaskQueue(TemporalJobType.CHECK_CONNECTION)

    val context = contextBuilder.fromSource(source)

    return execute(
      ConfigType.CHECK_CONNECTION_SOURCE,
      jobReportingContext,
      source.sourceDefinitionId,
      { temporalClient.submitCheckConnection(UUID.randomUUID(), 0, source.workspaceId, taskQueue, jobCheckConnectionConfig, context) },
      { obj: ConnectorJobOutput -> obj.checkConnection },
      source.workspaceId,
      source.sourceId,
      ActorType.SOURCE,
    )
  }

  @Throws(IOException::class)
  override fun createDestinationCheckConnectionJob(
    destination: DestinationConnection,
    destinationVersion: ActorDefinitionVersion,
    isCustomConnector: Boolean,
    resourceRequirements: ResourceRequirements?,
  ): SynchronousResponse<StandardCheckConnectionOutput> {
    val dockerImage = getDockerImageName(destinationVersion)
    val configWithOauthParams =
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        destination.destinationDefinitionId,
        destination.destinationId,
        destination.workspaceId,
        destination.configuration,
      )
    val injectedConfig = configInjector.injectConfig(configWithOauthParams, destination.destinationDefinitionId)

    val destinationConfig =
      if (destination.destinationId == null) {
        buildConfigWithSecretRefsJava(injectedConfig)
      } else {
        secretReferenceService.getConfigWithSecretReferences(
          ActorId(destination.destinationId),
          injectedConfig,
          WorkspaceId(destination.workspaceId),
        )
      }

    val jobCheckConnectionConfig =
      JobCheckConnectionConfig()
        .withActorType(ActorType.DESTINATION)
        .withActorId(destination.destinationId)
        .withConnectionConfiguration(destinationConfig)
        .withDockerImage(dockerImage)
        .withProtocolVersion(Version(destinationVersion.protocolVersion))
        .withIsCustomConnector(isCustomConnector)
        .withResourceRequirements(resourceRequirements)

    val jobId = UUID.randomUUID()
    val jobReportingContext =
      ConnectorJobReportingContext(jobId, dockerImage, destinationVersion.releaseStage, destinationVersion.internalSupportLevel)
    val taskQueue = getTaskQueue(TemporalJobType.CHECK_CONNECTION)

    val context = contextBuilder.fromDestination(destination)

    return execute(
      ConfigType.CHECK_CONNECTION_DESTINATION,
      jobReportingContext,
      destination.destinationDefinitionId,
      { temporalClient.submitCheckConnection(jobId, 0, destination.workspaceId, taskQueue, jobCheckConnectionConfig, context) },
      { obj: ConnectorJobOutput -> obj.checkConnection },
      destination.workspaceId,
      destination.destinationId,
      ActorType.DESTINATION,
    )
  }

  @Throws(IOException::class)
  override fun createDiscoverSchemaJob(
    source: SourceConnection,
    sourceVersion: ActorDefinitionVersion,
    isCustomConnector: Boolean,
    actorDefinitionResourceRequirements: ResourceRequirements?,
    priority: WorkloadPriority?,
  ): SynchronousResponse<UUID> {
    val dockerImage = getDockerImageName(sourceVersion)
    val configWithOauthParams =
      oAuthConfigSupplier.injectSourceOAuthParameters(
        source.sourceDefinitionId,
        source.sourceId,
        source.workspaceId,
        source.configuration,
      )
    val injectedConfig = configInjector.injectConfig(configWithOauthParams, source.sourceDefinitionId)

    val sourceConfig =
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(source.sourceId),
        injectedConfig,
        WorkspaceId(source.workspaceId),
      )

    val jobDiscoverCatalogConfig =
      JobDiscoverCatalogConfig()
        .withConnectionConfiguration(sourceConfig)
        .withDockerImage(dockerImage)
        .withProtocolVersion(Version(sourceVersion.protocolVersion))
        .withSourceId(source.sourceId.toString())
        .withConfigHash(
          HASH_FUNCTION
            .hashBytes(
              Jsons.serialize(source.configuration).toByteArray(
                Charsets.UTF_8,
              ),
            ).toString(),
        ).withConnectorVersion(sourceVersion.dockerImageTag)
        .withIsCustomConnector(isCustomConnector)
        .withResourceRequirements(actorDefinitionResourceRequirements)

    val jobId = UUID.randomUUID()
    val jobReportingContext =
      ConnectorJobReportingContext(jobId, dockerImage, sourceVersion.releaseStage, sourceVersion.internalSupportLevel)

    val taskQueue = getTaskQueue(TemporalJobType.DISCOVER_SCHEMA)

    val context = contextBuilder.fromSource(source)

    return execute(
      ConfigType.DISCOVER_SCHEMA,
      jobReportingContext,
      source.sourceDefinitionId,
      { temporalClient.submitDiscoverSchema(jobId, 0, source.workspaceId, taskQueue, jobDiscoverCatalogConfig, context, priority) },
      { obj: ConnectorJobOutput -> obj.discoverCatalogId },
      source.workspaceId,
      source.sourceId,
      ActorType.SOURCE,
    )
  }

  @Throws(IOException::class)
  override fun createDestinationDiscoverJob(
    destination: DestinationConnection,
    destinationDefinition: StandardDestinationDefinition,
    destinationVersion: ActorDefinitionVersion,
  ): SynchronousResponse<UUID> {
    val dockerImage = getDockerImageName(destinationVersion)
    val configWithOauthParams =
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        destination.destinationDefinitionId,
        destination.destinationId,
        destination.workspaceId,
        destination.configuration,
      )
    val injectedConfig = configInjector.injectConfig(configWithOauthParams, destination.destinationDefinitionId)

    val destinationConfig =
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(destination.destinationId),
        injectedConfig,
        WorkspaceId(destination.workspaceId),
      )

    val actorDefResourceRequirements =
      getResourceRequirementsForJobType(destinationDefinition.resourceRequirements, JobTypeResourceLimit.JobType.DISCOVER_SCHEMA)

    val jobDiscoverCatalogConfig =
      JobDiscoverCatalogConfig()
        .withConnectionConfiguration(destinationConfig)
        .withDockerImage(dockerImage)
        .withProtocolVersion(Version(destinationVersion.protocolVersion))
        .withSourceId(destination.destinationId.toString())
        .withConfigHash(
          HASH_FUNCTION
            .hashBytes(
              Jsons.serialize(destination.configuration).toByteArray(
                Charsets.UTF_8,
              ),
            ).toString(),
        ).withConnectorVersion(destinationVersion.dockerImageTag)
        .withIsCustomConnector(destinationDefinition.custom)
        .withResourceRequirements(actorDefResourceRequirements)

    val jobId = UUID.randomUUID()
    val jobReportingContext =
      ConnectorJobReportingContext(jobId, dockerImage, destinationVersion.releaseStage, destinationVersion.internalSupportLevel)

    val taskQueue = getTaskQueue(TemporalJobType.DISCOVER_SCHEMA)

    val context = contextBuilder.fromDestination(destination)

    return execute(
      ConfigType.DISCOVER_SCHEMA,
      jobReportingContext,
      destination.destinationDefinitionId,
      {
        temporalClient.submitDiscoverSchema(
          jobId,
          0,
          destination.workspaceId,
          taskQueue,
          jobDiscoverCatalogConfig,
          context,
          WorkloadPriority.HIGH,
        )
      },
      { obj: ConnectorJobOutput -> obj.discoverCatalogId },
      destination.workspaceId,
      destination.destinationId,
      ActorType.DESTINATION,
    )
  }

  override fun createGetSpecJob(
    dockerImage: String,
    isCustomConnector: Boolean,
    workspaceId: UUID,
  ): SynchronousResponse<ConnectorSpecification> {
    val jobSpecConfig = JobGetSpecConfig().withDockerImage(dockerImage).withIsCustomConnector(isCustomConnector)

    val jobId = UUID.randomUUID()
    val jobReportingContext = ConnectorJobReportingContext(jobId, dockerImage, null, null)

    return execute(
      ConfigType.GET_SPEC,
      jobReportingContext,
      null,
      { temporalClient.submitGetSpec(jobId, 0, workspaceId, jobSpecConfig) },
      { obj: ConnectorJobOutput -> obj.spec },
      null,
      null,
      null,
    )
  }

  @VisibleForTesting
  fun <T> execute(
    configType: ConfigType,
    jobContext: ConnectorJobReportingContext,
    @Nullable connectorDefinitionId: UUID?,
    executor: Supplier<TemporalResponse<ConnectorJobOutput>>,
    outputMapper: Function<ConnectorJobOutput, T>,
    @Nullable workspaceId: UUID?,
    @Nullable actorId: UUID?,
    @Nullable actorType: ActorType?,
  ): SynchronousResponse<T> {
    val createdAt = Instant.now().toEpochMilli()
    val jobId = jobContext.jobId
    try {
      track<Any>(jobId, configType, connectorDefinitionId, workspaceId, actorId, actorType, JobTracker.JobState.STARTED, null)
      val temporalResponse = executor.get()
      val jobOutput = temporalResponse.getOutput()
      val outputState = if (temporalResponse.metadata.succeeded) JobTracker.JobState.SUCCEEDED else JobTracker.JobState.FAILED

      track<Any>(jobId, configType, connectorDefinitionId, workspaceId, actorId, actorType, outputState, jobOutput.orElse(null))

      if (outputState == JobTracker.JobState.FAILED && jobOutput.isPresent) {
        reportError<Any, ConnectorJobOutput>(configType, jobContext, jobOutput.get(), connectorDefinitionId, workspaceId, actorType)
      }

      val endedAt = Instant.now().toEpochMilli()
      return SynchronousResponse.fromTemporalResponse(
        temporalResponse,
        outputMapper,
        jobId,
        configType,
        connectorDefinitionId,
        createdAt,
        endedAt,
      )
    } catch (e: RuntimeException) {
      track<Any>(jobId, configType, connectorDefinitionId, workspaceId, actorId, actorType, JobTracker.JobState.FAILED, null)
      throw e
    }
  }

    /*
     * @param connectorDefinitionId either source or destination definition id
     */
  private fun <T> track(
    jobId: UUID,
    configType: ConfigType,
    connectorDefinitionId: UUID?,
    @Nullable workspaceId: UUID?,
    @Nullable actorId: UUID?,
    @Nullable actorType: ActorType?,
    jobState: JobTracker.JobState,
    @Nullable jobOutput: ConnectorJobOutput?,
  ) {
    when (configType) {
      ConfigType.CHECK_CONNECTION_SOURCE ->
        jobTracker.trackCheckConnectionSource<Any>(
          jobId,
          connectorDefinitionId!!,
          workspaceId!!,
          actorId,
          jobState,
          jobOutput,
        )

      ConfigType.CHECK_CONNECTION_DESTINATION ->
        jobTracker.trackCheckConnectionDestination<Any>(
          jobId,
          connectorDefinitionId!!,
          workspaceId!!,
          actorId,
          jobState,
          jobOutput,
        )

      ConfigType.DISCOVER_SCHEMA ->
        jobTracker.trackDiscover(
          jobId,
          connectorDefinitionId!!,
          workspaceId!!,
          actorId,
          actorType,
          jobState,
          jobOutput,
        )
      ConfigType.GET_SPEC -> {
        // skip tracking for get spec to avoid noise.
      }

      else -> throw IllegalArgumentException(
        String.format("Jobs of type %s cannot be processed here. They should be consumed in the JobSubmitter.", configType),
      )
    }
  }

  private fun <S, T> reportError(
    configType: ConfigType,
    jobContext: ConnectorJobReportingContext,
    jobOutput: T,
    connectorDefinitionId: UUID?,
    workspaceId: UUID?,
    @Nullable actorType: ActorType?,
  ) {
    Exceptions.swallow {
      when (configType) {
        ConfigType.CHECK_CONNECTION_SOURCE ->
          jobErrorReporter.reportSourceCheckJobFailure(
            connectorDefinitionId!!,
            workspaceId,
            (jobOutput as ConnectorJobOutput).failureReason,
            jobContext,
          )

        ConfigType.CHECK_CONNECTION_DESTINATION ->
          jobErrorReporter.reportDestinationCheckJobFailure(
            connectorDefinitionId!!,
            workspaceId,
            (jobOutput as ConnectorJobOutput).failureReason,
            jobContext,
          )

        ConfigType.DISCOVER_SCHEMA ->
          jobErrorReporter.reportDiscoverJobFailure(
            connectorDefinitionId!!,
            actorType!!,
            workspaceId,
            (jobOutput as ConnectorJobOutput).failureReason,
            jobContext,
          )

        ConfigType.GET_SPEC ->
          jobErrorReporter.reportSpecJobFailure(
            (jobOutput as ConnectorJobOutput).failureReason,
            jobContext,
          )

        else ->
          log.error(
            "Tried to report job failure for type {}, but this job type is not supported",
            configType,
          )
      }
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}

    private val HASH_FUNCTION: HashFunction = Hashing.md5()
  }
}
