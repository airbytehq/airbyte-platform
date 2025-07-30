/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.tracker

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions
import io.airbyte.analytics.TrackingClient
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.lang.Exceptions
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptSyncConfig
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobConfigProxy
import io.airbyte.config.RefreshStream
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import java.io.IOException
import java.util.Collections
import java.util.List
import java.util.Locale
import java.util.Optional
import java.util.Set
import java.util.UUID
import java.util.stream.Collectors

/**
 * Tracking calls to each job type.
 */
class JobTracker
  @VisibleForTesting
  internal constructor(
    private val jobPersistence: JobPersistence,
    private val workspaceHelper: WorkspaceHelper,
    private val trackingClient: TrackingClient,
    private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
    private val sourceService: SourceService,
    private val destinationService: DestinationService,
    private val connectionService: ConnectionService,
    private val operationService: OperationService,
    private val workspaceService: WorkspaceService,
  ) {
    /**
     * Job state.
     */
    enum class JobState {
      STARTED,
      SUCCEEDED,
      FAILED,
    }

    constructor(
      jobPersistence: JobPersistence,
      trackingClient: TrackingClient,
      actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
      sourceService: SourceService,
      destinationService: DestinationService,
      connectionService: ConnectionService,
      operationService: OperationService,
      workspaceService: WorkspaceService,
    ) : this(
      jobPersistence,
      WorkspaceHelper(jobPersistence, connectionService, sourceService, destinationService, operationService, workspaceService),
      trackingClient,
      actorDefinitionVersionHelper,
      sourceService,
      destinationService,
      connectionService,
      operationService,
      workspaceService,
    )

    /**
     * Track telemetry for check connection.
     *
     * @param jobId job id
     * @param sourceDefinitionId source definition id
     * @param workspaceId workspace id
     * @param jobState job state
     * @param jobOutput job output, if available
     */
    fun <T> trackCheckConnectionSource(
      jobId: UUID,
      sourceDefinitionId: UUID,
      workspaceId: UUID,
      actorId: UUID?,
      jobState: JobState,
      jobOutput: ConnectorJobOutput?,
    ) {
      val responseOutput = jobOutput?.checkConnection
      val failureReason = jobOutput?.failureReason

      Exceptions.swallow {
        val checkConnMetadata = generateCheckConnectionMetadata(responseOutput)
        val failureReasonMetadata = generateFailureReasonMetadata(failureReason)
        val jobMetadata =
          generateJobMetadata(jobId.toString(), ConfigType.CHECK_CONNECTION_SOURCE)
        val sourceDefMetadata =
          generateSourceDefinitionMetadata(sourceDefinitionId, workspaceId, actorId)
        val stateMetadata = generateStateMetadata(jobState)
        track(
          workspaceId,
          CHECK_CONNECTION_SOURCE_EVENT,
          checkConnMetadata + failureReasonMetadata + jobMetadata + sourceDefMetadata + stateMetadata,
        )
      }
    }

    /**
     * Track telemetry for check connection.
     *
     * @param jobId job id
     * @param destinationDefinitionId defintion definition id
     * @param workspaceId workspace id
     * @param jobState job state
     * @param jobOutput job output, if available
     */
    fun <T> trackCheckConnectionDestination(
      jobId: UUID,
      destinationDefinitionId: UUID,
      workspaceId: UUID,
      actorId: UUID?,
      jobState: JobState,
      jobOutput: ConnectorJobOutput?,
    ) {
      val responseOutput = jobOutput?.checkConnection
      val failureReason = jobOutput?.failureReason

      Exceptions.swallow {
        val checkConnMetadata = generateCheckConnectionMetadata(responseOutput)
        val failureReasonMetadata = generateFailureReasonMetadata(failureReason)
        val jobMetadata =
          generateJobMetadata(jobId.toString(), ConfigType.CHECK_CONNECTION_DESTINATION)
        val destinationDefinitionMetadata =
          generateDestinationDefinitionMetadata(destinationDefinitionId, workspaceId, actorId)
        val stateMetadata = generateStateMetadata(jobState)
        track(
          workspaceId,
          CHECK_CONNECTION_DESTINATION_EVENT,
          checkConnMetadata + failureReasonMetadata + jobMetadata + destinationDefinitionMetadata + stateMetadata,
        )
      }
    }

    /**
     * Track telemetry for discover.
     *
     * @param jobId job id
     * @param actorDefinitionId actor definition id
     * @param workspaceId workspace id
     * @param jobState job state
     * @param jobOutput job output, if available
     */
    fun trackDiscover(
      jobId: UUID,
      actorDefinitionId: UUID,
      workspaceId: UUID,
      actorId: UUID?,
      actorType: ActorType?,
      jobState: JobState,
      jobOutput: ConnectorJobOutput?,
    ) {
      val failureReason = jobOutput?.failureReason

      Exceptions.swallow {
        val jobMetadata = generateJobMetadata(jobId.toString(), ConfigType.DISCOVER_SCHEMA)
        val failureReasonMetadata = generateFailureReasonMetadata(failureReason)
        val actorDefMetadata =
          if (actorType == ActorType.SOURCE) {
            generateSourceDefinitionMetadata(actorDefinitionId, workspaceId, actorId)
          } else {
            generateDestinationDefinitionMetadata(actorDefinitionId, workspaceId, actorId)
          }
        val stateMetadata = generateStateMetadata(jobState)
        track(
          workspaceId,
          DISCOVER_EVENT,
          jobMetadata + failureReasonMetadata + actorDefMetadata + stateMetadata,
        )
      }
    }

    /**
     * Used for tracking all asynchronous jobs (sync and reset).
     *
     * @param job job to track
     * @param jobState job state
     */
    fun trackSync(
      job: Job,
      jobState: JobState,
    ) {
      Exceptions.swallow {
        val jobConfig = JobConfigProxy(job.config)
        val configType = job.configType
        val allowedJob = Job.REPLICATION_TYPES.contains(configType)
        Preconditions.checkArgument(allowedJob, "Job type $configType is not allowed!")
        val jobId = job.id
        val lastAttempt = job.getLastAttempt()
        val attemptSyncConfig =
          lastAttempt.flatMap { obj: Attempt -> obj.getSyncConfig() }

        val connectionId = UUID.fromString(job.scope)
        val workspaceId = workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(jobId)
        val standardSync = connectionService.getStandardSync(connectionId)
        val sourceDefinition = sourceService.getSourceDefinitionFromConnection(connectionId)
        val sourceVersion =
          actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, standardSync.sourceId)
        val destinationDefinition = destinationService.getDestinationDefinitionFromConnection(connectionId)
        val destinationVersion =
          actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, standardSync.destinationId)

        val jobsHistory =
          jobPersistence.listJobsIncludingId(
            Set.of(ConfigType.SYNC, ConfigType.RESET_CONNECTION, ConfigType.REFRESH),
            connectionId.toString(),
            jobId,
            2,
          )

        val previousJob = jobsHistory.stream().filter { jobHistory: Job -> jobHistory.id != jobId }.findFirst()

        val jobMetadata = generateJobMetadata(jobId.toString(), configType, job.getAttemptsCount(), previousJob)
        val jobAttemptMetadata = generateJobAttemptMetadata(jobId, jobState)
        val sourceDefMetadata = generateSourceDefinitionMetadata(sourceDefinition, sourceVersion)
        val destinationDefMetadata = generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion)
        val syncMetadata = generateSyncMetadata(standardSync)
        val stateMetadata = generateStateMetadata(jobState)
        val syncConfigMetadata =
          generateSyncConfigMetadata(
            jobConfig,
            attemptSyncConfig.orElse(null),
            sourceVersion.spec.connectionSpecification,
            destinationVersion.spec.connectionSpecification,
          )
        val refreshMetadata = generateRefreshMetadata(jobConfig)
        track(
          workspaceId,
          SYNC_EVENT,
          jobMetadata +
            jobAttemptMetadata +
            sourceDefMetadata +
            destinationDefMetadata +
            syncMetadata +
            stateMetadata +
            syncConfigMetadata +
            refreshMetadata,
        )
      }
    }

    /**
     * Track sync for internal system failure.
     *
     * @param jobId job id
     * @param connectionId connection id
     * @param attempts attempts
     * @param jobState job state
     * @param e the exception that was thrown
     */
    fun trackSyncForInternalFailure(
      jobId: Long,
      connectionId: UUID,
      attempts: Int,
      jobState: JobState,
      e: Exception,
    ) {
      Exceptions.swallow {
        val workspaceId = workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(jobId)
        val standardSync = connectionService.getStandardSync(connectionId)
        val sourceDefinition = sourceService.getSourceDefinitionFromConnection(connectionId)
        val destinationDefinition =
          destinationService.getDestinationDefinitionFromConnection(connectionId)
        val sourceVersion =
          actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, standardSync.sourceId)
        val destinationVersion =
          actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, standardSync.destinationId)

        val jobMetadata =
          generateJobMetadata(jobId.toString(), null, attempts, Optional.empty())
        val jobAttemptMetadata = generateJobAttemptMetadata(jobId, jobState)
        val sourceDefMetadata =
          generateSourceDefinitionMetadata(sourceDefinition, sourceVersion)
        val destinationDefMetadata =
          generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion)
        val syncMetadata = generateSyncMetadata(standardSync)
        val stateMetadata = generateStateMetadata(jobState)
        val generalMetadata =
          java.util.Map.of<String, Any?>(
            "connection_id",
            connectionId,
            "internal_error_cause",
            e.message,
            "internal_error_type",
            e.javaClass.name,
          )
        track(
          workspaceId,
          INTERNAL_FAILURE_SYNC_EVENT,
          jobMetadata + jobAttemptMetadata + sourceDefMetadata + destinationDefMetadata + syncMetadata + stateMetadata + generalMetadata,
        )
      }
    }

    private fun generateRefreshMetadata(jobConfig: JobConfigProxy): Map<String, Any?> {
      if (jobConfig.configType == ConfigType.REFRESH) {
        val refreshTypes =
          jobConfig
            .raw
            ?.getRefresh()
            ?.streamsToRefresh
            ?.stream()
            ?.map { obj: RefreshStream -> obj.refreshType }
            ?.collect(Collectors.toSet())
            ?.stream()
            ?.map { obj: RefreshStream.RefreshType -> obj.value() }
            ?.toList()
        return java.util.Map.of<String, Any?>("refresh_types", refreshTypes)
      }
      return java.util.Map.of()
    }

    private fun generateSyncConfigMetadata(
      config: JobConfigProxy,
      attemptSyncConfig: AttemptSyncConfig?,
      sourceConfigSchema: JsonNode,
      destinationConfigSchema: JsonNode,
    ): Map<String, Any?> {
      if (Job.SYNC_REPLICATION_TYPES.contains(config.configType)) {
        val actorConfigMetadata: MutableMap<String, Any?> = HashMap()

        if (attemptSyncConfig != null) {
          val sourceConfiguration = attemptSyncConfig.sourceConfiguration
          val destinationConfiguration = attemptSyncConfig.destinationConfiguration

          actorConfigMetadata[CONFIG + ".source"] =
            mapToJsonString(configToMetadata(sourceConfiguration, sourceConfigSchema))

          actorConfigMetadata[CONFIG + ".destination"] =
            mapToJsonString(configToMetadata(destinationConfiguration, destinationConfigSchema))
        }

        val configuredCatalog = config.configuredCatalog
        val catalogMetadata: Map<String, Any?>
        if (configuredCatalog != null) {
          catalogMetadata = getCatalogMetadata(configuredCatalog)
        } else {
          // This is not possible
          throw IllegalStateException("This should not be reacheable")
        }
        return actorConfigMetadata + catalogMetadata
      } else {
        return emptyMap<String, Any>()
      }
    }

    private fun getCatalogMetadata(catalog: ConfiguredAirbyteCatalog): Map<String, Any?> {
      val output: MutableMap<String, Any?> = HashMap()

      for ((_, syncMode, destinationSyncMode) in catalog.streams) {
        output[CATALOG + ".sync_mode." + syncMode.name.lowercase(Locale.getDefault())] =
          SET
        output[CATALOG + ".destination_sync_mode." + destinationSyncMode.name.lowercase(Locale.getDefault())] =
          SET
      }

      return output
    }

    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    private fun generateSyncMetadata(standardSync: StandardSync): Map<String, Any?> {
      val operationUsage: MutableMap<String, Any?> = HashMap()
      for (operationId in standardSync.operationIds) {
        val operation = operationService.getStandardSyncOperation(operationId)
        if (operation != null) {
          val usageCount = operationUsage.getOrDefault(OPERATION + operation.operatorType, 0) as Int?
          operationUsage[OPERATION + operation.operatorType] = usageCount!! + 1
        }
      }

      val streamCountData: MutableMap<String, Any?> = HashMap()
      val streamCount = standardSync.catalog.streams.size
      streamCountData["number_of_streams"] = streamCount

      return TrackingMetadata.generateSyncMetadata(standardSync) + operationUsage + streamCountData
    }

    /**
     * The CheckConnection jobs (both source and destination) of the
     * [io.airbyte.scheduler.client.SynchronousSchedulerClient] interface can have a successful
     * job with a failed check. Because of this, tracking just the job attempt status does not capture
     * the whole picture. The `check_connection_outcome` field tracks this.
     */
    private fun generateCheckConnectionMetadata(output: StandardCheckConnectionOutput?): Map<String, Any?> {
      val metadata: MutableMap<String, Any?> = HashMap()

      if (output == null) {
        return metadata
      }

      if (output.message != null) {
        metadata["check_connection_message"] = output.message
      }
      metadata["check_connection_outcome"] = output.status.toString()
      return Collections.unmodifiableMap(metadata)
    }

    private fun generateFailureReasonMetadata(failureReason: FailureReason?): Map<String, Any?> {
      if (failureReason == null) {
        return java.util.Map.of()
      }
      return java.util.Map.of<String, Any?>("failure_reason", TrackingMetadata.failureReasonAsJson(failureReason).toString())
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    private fun generateDestinationDefinitionMetadata(
      destinationDefinitionId: UUID,
      workspaceId: UUID,
      actorId: UUID?,
    ): Map<String, Any?> {
      val destinationDefinition = destinationService.getStandardDestinationDefinition(destinationDefinitionId)
      val destinationVersion = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, actorId)
      return generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion)
    }

    private fun generateDestinationDefinitionMetadata(
      destinationDefinition: StandardDestinationDefinition,
      destinationVersion: ActorDefinitionVersion,
    ): Map<String, Any?> = TrackingMetadata.generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion)

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    private fun generateSourceDefinitionMetadata(
      sourceDefinitionId: UUID,
      workspaceId: UUID,
      actorId: UUID?,
    ): Map<String, Any?> {
      val sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId)
      val sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, actorId)
      return generateSourceDefinitionMetadata(sourceDefinition, sourceVersion)
    }

    private fun generateSourceDefinitionMetadata(
      sourceDefinition: StandardSourceDefinition,
      sourceVersion: ActorDefinitionVersion,
    ): Map<String, Any?> = TrackingMetadata.generateSourceDefinitionMetadata(sourceDefinition, sourceVersion)

    private fun generateJobMetadata(
      jobId: String,
      configType: ConfigType,
    ): Map<String, Any?> = generateJobMetadata(jobId, configType, 0, Optional.empty())

    @VisibleForTesting
    fun generateJobMetadata(
      jobId: String?,
      configType: ConfigType?,
      attempt: Int,
      previousJob: Optional<Job>,
    ): Map<String, Any?> {
      val metadata: MutableMap<String, Any?> = HashMap()
      if (configType != null) {
        // This is a cosmetic fix for our job tracking.
        // https://github.com/airbytehq/airbyte-internal-issues/issues/7671 tracks the more complete
        // refactoring. Once that is done, this should no longer be needed as we can directly log
        // configType.
        val eventConfigType = if (configType == ConfigType.RESET_CONNECTION) ConfigType.CLEAR else configType
        metadata["job_type"] = eventConfigType
      }
      metadata["job_id"] = jobId
      metadata["attempt_id"] = attempt
      previousJob.ifPresent { job: Job ->
        if (job.configType != null) {
          metadata["previous_job_type"] = job.configType
        }
      }
      return Collections.unmodifiableMap(metadata)
    }

    @Throws(IOException::class)
    private fun generateJobAttemptMetadata(
      jobId: Long,
      jobState: JobState,
    ): Map<String, Any?> {
      val job = jobPersistence.getJob(jobId)
      return if (jobState != JobState.STARTED) {
        TrackingMetadata.generateJobAttemptMetadata(job)
      } else {
        java.util.Map.of()
      }
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    private fun track(
      workspaceId: UUID?,
      action: String,
      metadata: Map<String, Any?>,
    ) {
      // unfortunate but in the case of jobs that cannot be linked to a workspace there not a sensible way
      // track it.
      if (workspaceId != null) {
        val standardWorkspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)
        if (standardWorkspace?.name != null) {
          val standardTrackingMetadata =
            java.util.Map.of<String, Any?>(
              "workspace_id",
              workspaceId,
              "workspace_name",
              standardWorkspace.name,
            )

          trackingClient.track(workspaceId, ScopeType.WORKSPACE, action, metadata + standardTrackingMetadata)
        }
      }
    }

    companion object {
      const val SYNC_EVENT: String = "Sync Jobs"
      const val CHECK_CONNECTION_SOURCE_EVENT: String = "Check Connection Source Jobs"
      const val CHECK_CONNECTION_DESTINATION_EVENT: String = "Check Connection Destination Jobs"
      const val DISCOVER_EVENT: String = "Discover Jobs"
      const val INTERNAL_FAILURE_SYNC_EVENT: String = "Sync Jobs Internal Failure"
      const val CONFIG: String = "config"
      const val CATALOG: String = "catalog"
      const val OPERATION: String = "operation."
      const val SET: String = "set"

      private val OBJECT_MAPPER = ObjectMapper()

      private fun mapToJsonString(map: Map<String?, Any?>): String =
        try {
          OBJECT_MAPPER.writeValueAsString(map)
        } catch (e: JsonProcessingException) {
          "<failed to convert to JSON>"
        }

      /**
       * Does the actually interesting bits of configToMetadata. If config is an object, returns a
       * flattened map. If config is _not_ an object (i.e. it's a primitive string/number/etc, or it's an
       * array) then returns a map of {null: toMetadataValue(config)}.
       */
      @JvmStatic
      fun configToMetadata(
        config: JsonNode,
        schema: JsonNode,
      ): Map<String?, Any?> {
        if (schema.hasNonNull("const") || schema.hasNonNull("enum")) {
          // If this schema is a const or an enum, then just dump it into a map:
          // * If it's an object, flatten it
          // * Otherwise, do some basic conversions to value-ish data.
          // It would be a weird thing to declare const: null, but in that case we don't want to report null
          // anyway, so explicitly use hasNonNull.
          return Jsons.flatten(config)
        } else if (schema.has("oneOf")) {
          // If this schema is a oneOf, then find the first sub-schema which the config matches
          // and use that sub-schema to convert the config to a map
          val validator = JsonSchemaValidator()
          val it = schema["oneOf"].elements()
          while (it.hasNext()) {
            val subSchema = it.next()
            if (validator.test(subSchema, config)) {
              return configToMetadata(config, subSchema)
            }
          }
          // If we didn't match any of the subschemas, then something is wrong. Bail out silently.
          return emptyMap()
        } else if (config.isObject) {
          // If the schema is not a oneOf, but the config is an object (i.e. the schema has "type": "object")
          // then we need to recursively convert each field of the object to a map.
          val output: MutableMap<String?, Any?> = HashMap()
          val maybeProperties = schema["properties"]

          // If additionalProperties is not set, or it's a boolean, then there's no schema for additional
          // properties. Use the accept-all schema.
          // Otherwise, it's an actual schema.
          val maybeAdditionalProperties = schema["additionalProperties"]
          val additionalPropertiesSchema =
            if (maybeAdditionalProperties == null || maybeAdditionalProperties.isBoolean) {
              OBJECT_MAPPER.createObjectNode()
            } else {
              maybeAdditionalProperties
            }

          val it = config.fields()
          while (it.hasNext()) {
            val entry = it.next()
            val field = entry.key
            val value = entry.value
            val propertySchema =
              if (maybeProperties != null && maybeProperties.hasNonNull(field)) {
                // If this property is explicitly declared, then use its schema
                maybeProperties[field]
              } else {
                // otherwise, use the additionalProperties schema
                additionalPropertiesSchema
              }

            Jsons.mergeMaps(output, field, configToMetadata(value, propertySchema))
          }
          return output
        } else if (config.isBoolean) {
          return Collections.singletonMap<String?, Any>(null, config.asBoolean())
        } else if ((!config.isTextual && !config.isNull) || (config.isTextual && !config.asText().isEmpty())) {
          // This is either non-textual (e.g. integer, array, etc) or non-empty text
          return Collections.singletonMap<String?, Any>(null, SET)
        } else {
          // Otherwise, this is an empty string, so just ignore it
          return emptyMap()
        }
      }

      private fun generateStateMetadata(jobState: JobState): Map<String, Any?> {
        val metadata: MutableMap<String, Any?> = HashMap()

        if (JobState.STARTED == jobState) {
          metadata["attempt_stage"] = "STARTED"
        } else if (List.of<JobState>(JobState.SUCCEEDED, JobState.FAILED).contains(jobState)) {
          metadata["attempt_stage"] = "ENDED"
          metadata["attempt_completion_status"] = jobState
        }

        return Collections.unmodifiableMap(metadata)
      }
    }
  }
