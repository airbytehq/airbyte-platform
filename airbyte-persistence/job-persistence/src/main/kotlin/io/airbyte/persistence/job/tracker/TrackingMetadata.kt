/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.tracker

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.google.common.base.Strings
import com.google.common.collect.ImmutableMap
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
  fun generateSyncMetadata(standardSync: StandardSync): Map<String, Any> {
    val metadata = ImmutableMap.builder<String, Any>()
    metadata.put("connection_id", standardSync.connectionId)
    metadata.put("source_id", standardSync.sourceId)
    metadata.put("destination_id", standardSync.destinationId)

    val frequencyString: String
    if (standardSync.scheduleType != null) {
      frequencyString = getFrequencyStringFromScheduleType(standardSync.scheduleType, standardSync.scheduleData)
    } else if (standardSync.manual) {
      frequencyString = "manual"
    } else {
      val intervalInMinutes = TimeUnit.SECONDS.toMinutes(getIntervalInSecond(standardSync.schedule))
      frequencyString = "$intervalInMinutes min"
    }
    metadata.put("frequency", frequencyString)

    val operationCount = if (standardSync.operationIds != null) standardSync.operationIds.size else 0
    metadata.put("operation_count", operationCount)
    if (standardSync.namespaceDefinition != null) {
      metadata.put("namespace_definition", standardSync.namespaceDefinition)
    }

    val isUsingPrefix = standardSync.prefix != null && !standardSync.prefix.isBlank()
    metadata.put("table_prefix", isUsingPrefix)

    val resourceRequirements = standardSync.resourceRequirements

    if (resourceRequirements != null) {
      if (!Strings.isNullOrEmpty(resourceRequirements.cpuRequest)) {
        metadata.put("sync_cpu_request", resourceRequirements.cpuRequest)
      }
      if (!Strings.isNullOrEmpty(resourceRequirements.cpuLimit)) {
        metadata.put("sync_cpu_limit", resourceRequirements.cpuLimit)
      }
      if (!Strings.isNullOrEmpty(resourceRequirements.memoryRequest)) {
        metadata.put("sync_memory_request", resourceRequirements.memoryRequest)
      }
      if (!Strings.isNullOrEmpty(resourceRequirements.memoryLimit)) {
        metadata.put("sync_memory_limit", resourceRequirements.memoryLimit)
      }
    }
    return metadata.build()
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
  ): Map<String, Any> {
    val metadata = ImmutableMap.builder<String, Any>()
    metadata.put("connector_destination", destinationDefinition.name)
    metadata.put("connector_destination_definition_id", destinationDefinition.destinationDefinitionId)
    metadata.putAll(generateActorDefinitionVersionMetadata("connector_destination_", destinationVersion))
    return metadata.build()
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
  ): Map<String, Any> {
    val metadata = ImmutableMap.builder<String, Any>()
    metadata.put("connector_source", sourceDefinition.name)
    metadata.put("connector_source_definition_id", sourceDefinition.sourceDefinitionId)
    metadata.putAll(generateActorDefinitionVersionMetadata("connector_source_", sourceVersion))
    return metadata.build()
  }

  private fun generateActorDefinitionVersionMetadata(
    metaPrefix: String,
    sourceVersion: ActorDefinitionVersion,
  ): Map<String, Any> {
    val metadata = ImmutableMap.builder<String, Any>()
    metadata.put(metaPrefix + "docker_repository", sourceVersion.dockerRepository)
    val imageTag = sourceVersion.dockerImageTag
    if (!StringUtils.isEmpty(imageTag)) {
      metadata.put(metaPrefix + "version", imageTag)
    }
    return metadata.build()
  }

  /**
   * Map job metadata / stats to a string-to-object map so it can be attached to telemetry calls.
   *
   * @param job job
   * @return metadata / stats as a string-to-object map.
   */
  @JvmStatic
  fun generateJobAttemptMetadata(job: Job?): Map<String, Any> {
    val metadata = ImmutableMap.builder<String, Any>()
    // Early returns in case we're missing the relevant stats.
    if (job == null) {
      return metadata.build()
    }
    val attempts = job.attempts
    if (attempts == null || attempts.isEmpty()) {
      return metadata.build()
    }
    val lastAttempt: Attempt = attempts.last()
    if (lastAttempt.getOutput() == null || lastAttempt.getOutput().isEmpty) {
      return metadata.build()
    }
    val jobOutput = lastAttempt.getOutput().get()
    if (jobOutput.sync == null) {
      return metadata.build()
    }
    val syncSummary = jobOutput.sync.standardSyncSummary
    val totalStats = syncSummary.totalStats
    if (syncSummary.startTime != null) {
      metadata.put("sync_start_time", syncSummary.startTime)
    }
    if (syncSummary.endTime != null && syncSummary.startTime != null) {
      metadata.put("duration", Math.round((syncSummary.endTime - syncSummary.startTime) / 1000.0))
    }
    if (syncSummary.bytesSynced != null) {
      metadata.put("volume_mb", syncSummary.bytesSynced)
    }
    if (syncSummary.recordsSynced != null) {
      metadata.put("volume_rows", syncSummary.recordsSynced)
    }
    if (totalStats.sourceStateMessagesEmitted != null) {
      metadata.put("count_state_messages_from_source", syncSummary.totalStats.sourceStateMessagesEmitted)
    }
    if (totalStats.destinationStateMessagesEmitted != null) {
      metadata.put("count_state_messages_from_destination", syncSummary.totalStats.destinationStateMessagesEmitted)
    }
    if (totalStats.maxSecondsBeforeSourceStateMessageEmitted != null) {
      metadata.put(
        "max_seconds_before_source_state_message_emitted",
        totalStats.maxSecondsBeforeSourceStateMessageEmitted,
      )
    }
    if (totalStats.meanSecondsBeforeSourceStateMessageEmitted != null) {
      metadata.put(
        "mean_seconds_before_source_state_message_emitted",
        totalStats.meanSecondsBeforeSourceStateMessageEmitted,
      )
    }
    if (totalStats.maxSecondsBetweenStateMessageEmittedandCommitted != null) {
      metadata.put(
        "max_seconds_between_state_message_emit_and_commit",
        totalStats.maxSecondsBetweenStateMessageEmittedandCommitted,
      )
    }
    if (totalStats.meanSecondsBetweenStateMessageEmittedandCommitted != null) {
      metadata.put(
        "mean_seconds_between_state_message_emit_and_commit",
        totalStats.meanSecondsBetweenStateMessageEmittedandCommitted,
      )
    }

    if (totalStats.replicationStartTime != null) {
      metadata.put("replication_start_time", totalStats.replicationStartTime)
    }
    if (totalStats.replicationEndTime != null) {
      metadata.put("replication_end_time", totalStats.replicationEndTime)
    }
    if (totalStats.sourceReadStartTime != null) {
      metadata.put("source_read_start_time", totalStats.sourceReadStartTime)
    }
    if (totalStats.sourceReadEndTime != null) {
      metadata.put("source_read_end_time", totalStats.sourceReadEndTime)
    }
    if (totalStats.destinationWriteStartTime != null) {
      metadata.put("destination_write_start_time", totalStats.destinationWriteStartTime)
    }
    if (totalStats.destinationWriteEndTime != null) {
      metadata.put("destination_write_end_time", totalStats.destinationWriteEndTime)
    }

    val failureReasons = failureReasonsList(attempts)
    if (!failureReasons.isEmpty()) {
      metadata.put("failure_reasons", failureReasonsListAsJson(failureReasons).toString())
      metadata.put("main_failure_reason", failureReasonAsJson(failureReasons.first()).toString())
    }
    return metadata.build()
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
