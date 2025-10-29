/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.tracker

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.helpers.ScheduleHelpers.getIntervalInSecond
import io.micronaut.core.util.StringUtils
import java.util.Optional
import java.util.concurrent.TimeUnit
import kotlin.math.roundToInt

/**
 * Helpers to fetch stats / metadata about Airbyte domain models and turn them into flat maps that
 * can be appended to telemetry calls.
 */
object TrackingMetadata {
  /**
   * Map sync metadata / stats to a string-to-object map so it can be attached to telemetry calls.
   *
   * @param standardSync connection
   * @return metadata / stats as a string-to-object map.
   */
  @JvmStatic
  fun generateSyncMetadata(standardSync: StandardSync): Map<String, Any> =
    buildMap {
      put("connection_id", standardSync.connectionId)
      put("source_id", standardSync.sourceId)
      put("destination_id", standardSync.destinationId)

      val frequencyString: String =
        if (standardSync.scheduleType != null) {
          getFrequencyStringFromScheduleType(standardSync.scheduleType, standardSync.scheduleData)
        } else if (standardSync.manual) {
          "manual"
        } else {
          val intervalInMinutes = TimeUnit.SECONDS.toMinutes(getIntervalInSecond(standardSync.schedule))
          "$intervalInMinutes min"
        }
      put("frequency", frequencyString)

      val operationCount = if (standardSync.operationIds != null) standardSync.operationIds.size else 0
      put("operation_count", operationCount)
      if (standardSync.namespaceDefinition != null) {
        put("namespace_definition", standardSync.namespaceDefinition)
      }

      val isUsingPrefix = standardSync.prefix != null && !standardSync.prefix.isBlank()
      put("table_prefix", isUsingPrefix)

      val resourceRequirements = standardSync.resourceRequirements

      if (resourceRequirements != null) {
        if (!resourceRequirements.cpuRequest.isNullOrBlank()) {
          put("sync_cpu_request", resourceRequirements.cpuRequest)
        }
        if (!resourceRequirements.cpuLimit.isNullOrBlank()) {
          put("sync_cpu_limit", resourceRequirements.cpuLimit)
        }
        if (!resourceRequirements.memoryRequest.isNullOrBlank()) {
          put("sync_memory_request", resourceRequirements.memoryRequest)
        }
        if (!resourceRequirements.memoryLimit.isNullOrBlank()) {
          put("sync_memory_limit", resourceRequirements.memoryLimit)
        }
      }
    }

  /**
   * Map destination definition metadata / stats to a string-to-object map so it can be attached to
   * telemetry calls.
   *
   * @param destinationDefinition destination definition
   * @return metadata / stats as a string-to-object map.
   */
  @JvmStatic
  fun generateDestinationDefinitionMetadata(
    destinationDefinition: StandardDestinationDefinition,
    destinationVersion: ActorDefinitionVersion,
  ): Map<String, Any> =
    buildMap {
      put("connector_destination", destinationDefinition.name)
      put("connector_destination_definition_id", destinationDefinition.destinationDefinitionId)
      putAll(generateActorDefinitionVersionMetadata("connector_destination_", destinationVersion))
    }

  /**
   * Map source definition metadata / stats to a string-to-object map so it can be attached to
   * telemetry calls.
   *
   * @param sourceDefinition source definition
   * @return metadata / stats as a string-to-object map.
   */
  @JvmStatic
  fun generateSourceDefinitionMetadata(
    sourceDefinition: StandardSourceDefinition,
    sourceVersion: ActorDefinitionVersion,
  ): Map<String, Any> =
    buildMap {
      put("connector_source", sourceDefinition.name)
      put("connector_source_definition_id", sourceDefinition.sourceDefinitionId)
      putAll(generateActorDefinitionVersionMetadata("connector_source_", sourceVersion))
    }

  private fun generateActorDefinitionVersionMetadata(
    metaPrefix: String,
    sourceVersion: ActorDefinitionVersion,
  ): Map<String, Any> =
    buildMap {
      put(metaPrefix + "docker_repository", sourceVersion.dockerRepository)
      val imageTag = sourceVersion.dockerImageTag
      if (!StringUtils.isEmpty(imageTag)) {
        put(metaPrefix + "version", imageTag)
      }
    }

  /**
   * Map job metadata / stats to a string-to-object map so it can be attached to telemetry calls.
   *
   * @param job job
   * @return metadata / stats as a string-to-object map.
   */
  @JvmStatic
  fun generateJobAttemptMetadata(job: Job?): Map<String, Any> =
    buildMap {
      // Early returns in case we're missing the relevant stats.
      if (job == null) {
        return@buildMap
      }
      val attempts = job.attempts
      if (attempts.isEmpty()) {
        return@buildMap
      }
      val lastAttempt: Attempt = attempts.last()
      if (lastAttempt.getOutput() == null || lastAttempt.getOutput().isEmpty) {
        return@buildMap
      }
      val jobOutput = lastAttempt.getOutput().get()
      if (jobOutput.sync == null) {
        return@buildMap
      }
      val syncSummary = jobOutput.sync.standardSyncSummary
      val totalStats = syncSummary.totalStats
      if (syncSummary.startTime != null) {
        put("sync_start_time", syncSummary.startTime)
      }
      if (syncSummary.endTime != null && syncSummary.startTime != null) {
        put("duration", ((syncSummary.endTime - syncSummary.startTime) / 1000.0).roundToInt())
      }
      if (syncSummary.bytesSynced != null) {
        put("volume_mb", syncSummary.bytesSynced)
      }
      if (syncSummary.recordsSynced != null) {
        put("volume_rows", syncSummary.recordsSynced)
      }
      totalStats?.let {
        if (totalStats.sourceStateMessagesEmitted != null) {
          put("count_state_messages_from_source", totalStats.sourceStateMessagesEmitted)
        }
        if (totalStats.destinationStateMessagesEmitted != null) {
          put("count_state_messages_from_destination", totalStats.destinationStateMessagesEmitted)
        }
        if (totalStats.maxSecondsBeforeSourceStateMessageEmitted != null) {
          put(
            "max_seconds_before_source_state_message_emitted",
            totalStats.maxSecondsBeforeSourceStateMessageEmitted,
          )
        }
        if (totalStats.meanSecondsBeforeSourceStateMessageEmitted != null) {
          put(
            "mean_seconds_before_source_state_message_emitted",
            totalStats.meanSecondsBeforeSourceStateMessageEmitted,
          )
        }
        if (totalStats.maxSecondsBetweenStateMessageEmittedandCommitted != null) {
          put(
            "max_seconds_between_state_message_emit_and_commit",
            totalStats.maxSecondsBetweenStateMessageEmittedandCommitted,
          )
        }
        if (totalStats.meanSecondsBetweenStateMessageEmittedandCommitted != null) {
          put(
            "mean_seconds_between_state_message_emit_and_commit",
            totalStats.meanSecondsBetweenStateMessageEmittedandCommitted,
          )
        }

        if (totalStats.replicationStartTime != null) {
          put("replication_start_time", totalStats.replicationStartTime)
        }
        if (totalStats.replicationEndTime != null) {
          put("replication_end_time", totalStats.replicationEndTime)
        }
        if (totalStats.sourceReadStartTime != null) {
          put("source_read_start_time", totalStats.sourceReadStartTime)
        }
        if (totalStats.sourceReadEndTime != null) {
          put("source_read_end_time", totalStats.sourceReadEndTime)
        }
        if (totalStats.destinationWriteStartTime != null) {
          put("destination_write_start_time", totalStats.destinationWriteStartTime)
        }
        if (totalStats.destinationWriteEndTime != null) {
          put("destination_write_end_time", totalStats.destinationWriteEndTime)
        }
      }

      val failureReasons = failureReasonsList(attempts)
      if (!failureReasons.isEmpty()) {
        put("failure_reasons", failureReasonsListAsJson(failureReasons).toString())
        put("main_failure_reason", failureReasonAsJson(failureReasons.first()).toString())
      }
    }

  private fun failureReasonsList(attempts: List<Attempt>): List<FailureReason> =
    attempts
      .stream()
      .map { obj: Attempt -> obj.getFailureSummary() }
      .flatMap { obj: Optional<AttemptFailureSummary> -> obj.stream() }
      .map { obj: AttemptFailureSummary -> obj.failures }
      .flatMap { obj: List<FailureReason> -> obj.stream() }
      .sorted(
        Comparator.comparing { f: FailureReason ->
          val timestamp = f.timestamp
          timestamp ?: 0L
        },
      ).toList()

  private fun failureReasonsListAsJson(failureReasons: List<FailureReason>): ArrayNode =
    Jsons.arrayNode().addAll(
      failureReasons
        .stream()
        .map { obj: FailureReason -> failureReasonAsJson(obj) }
        .toList(),
    )

  /**
   * Map a FailureReason to a string-to-object map, so it can be attached to telemetry calls.
   *
   * @param failureReason failure reason
   * @return failure reason as a string-to-object map.
   */
  fun failureReasonAsJson(failureReason: FailureReason): JsonNode {
    // we want the json to always include failureOrigin and failureType, even when they are null
    val linkedHashMap: MutableMap<String, Any> = LinkedHashMap()
    linkedHashMap["failureOrigin"] = failureReason.failureOrigin
    linkedHashMap["failureType"] = failureReason.failureType
    linkedHashMap["internalMessage"] = failureReason.internalMessage
    linkedHashMap["externalMessage"] = failureReason.externalMessage
    linkedHashMap["metadata"] = failureReason.metadata
    linkedHashMap["retryable"] = failureReason.retryable
    linkedHashMap["timestamp"] = failureReason.timestamp

    return Jsons.jsonNode<Map<String, Any>>(linkedHashMap)
  }

  private fun getFrequencyStringFromScheduleType(
    scheduleType: StandardSync.ScheduleType,
    scheduleData: ScheduleData?,
  ): String =
    when (scheduleType) {
      StandardSync.ScheduleType.MANUAL -> {
        "manual"
      }

      StandardSync.ScheduleType.BASIC_SCHEDULE -> {
        TimeUnit.SECONDS.toMinutes(getIntervalInSecond(scheduleData!!.basicSchedule)).toString() + " min"
      }

      StandardSync.ScheduleType.CRON -> {
        // TODO(https://github.com/airbytehq/airbyte/issues/2170): consider something more detailed.
        "cron"
      }

      else -> {
        throw RuntimeException("Unexpected schedule type")
      }
    }
}
