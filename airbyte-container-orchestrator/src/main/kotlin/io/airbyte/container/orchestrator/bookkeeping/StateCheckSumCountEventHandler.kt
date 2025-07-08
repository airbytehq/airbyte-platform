/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import com.google.common.hash.Hashing
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.analytics.Deployment
import io.airbyte.analytics.DeploymentFetcher
import io.airbyte.analytics.TrackingIdentity
import io.airbyte.analytics.TrackingIdentityFetcher
import io.airbyte.commons.json.Jsons
import io.airbyte.config.FailureReason
import io.airbyte.config.ScopeType
import io.airbyte.container.orchestrator.worker.exception.InvalidChecksumException
import io.airbyte.container.orchestrator.worker.model.StateCheckSumCountEvent
import io.airbyte.container.orchestrator.worker.model.attachIdToStateMessageFromSource
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.EmitStateStatsToSegment
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.LogStateMsgs
import io.airbyte.featureflag.LogStreamNamesInSateMessage
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags.ATTEMPT_NUMBER
import io.airbyte.metrics.lib.MetricTags.CONNECTION_ID
import io.airbyte.metrics.lib.MetricTags.DESTINATION_IMAGE
import io.airbyte.metrics.lib.MetricTags.FAILURE_ORIGIN
import io.airbyte.metrics.lib.MetricTags.JOB_ID
import io.airbyte.metrics.lib.MetricTags.SOURCE_IMAGE
import io.airbyte.metrics.lib.MetricTags.WORKSPACE_ID
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.workers.models.ArchitectureConstants
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.util.UUID
import java.util.function.Supplier

private val logger = KotlinLogging.logger { }

private const val MAX_MISSING_EVENTS = 25
private const val MAX_MISMATCH_EVENTS = 25
private const val MAX_SUCCESS_EVENTS = 3

@Singleton
class StateCheckSumCountEventHandler(
  @Named("pubSubWriter") private val pubSubWriter: StateCheckSumEventPubSubWriter?,
  private val featureFlagClient: FeatureFlagClient,
  private val deploymentFetcher: DeploymentFetcher,
  private val trackingIdentityFetcher: TrackingIdentityFetcher,
  private val stateCheckSumReporter: StateCheckSumErrorReporter,
  @Named("connectionId") private val connectionId: UUID,
  @Named("workspaceId") private val workspaceId: UUID,
  @Value("\${airbyte.job-id}") private val jobId: Long,
  @Named("attemptId") private val attemptNumber: Int,
  @Named("epochMilliSupplier") private val epochMilliSupplier: Supplier<Long>,
  @Named("idSupplier") private val idSupplier: Supplier<UUID>,
  @Value("\${airbyte.platform-mode}") private val platformMode: String,
  private val metricClient: MetricClient,
  private val replicationInput: ReplicationInput,
) {
  private val emitStatsCounterFlag: Boolean by lazy {
    val connectionContext = Multi(listOf(Connection(connectionId), Workspace(workspaceId)))
    featureFlagClient.boolVariation(EmitStateStatsToSegment, connectionContext)
  }

  // Temp piece of code for debug
  val logIncomingStreamNames: Boolean by lazy {
    val connectionContext = Multi(listOf(Connection(connectionId), Workspace(workspaceId)))
    featureFlagClient.boolVariation(LogStreamNamesInSateMessage, connectionContext)
  }

  private val deployment: Deployment by lazy { retry { deploymentFetcher.get() } }

  private val trackingIdentity: TrackingIdentity by lazy { retry { trackingIdentityFetcher.apply(workspaceId, ScopeType.WORKSPACE) } }

  private fun shouldEmitStateStatsToSegment(): Boolean = emitStatsCounterFlag

  private fun isStateTypeSupported(stateMessage: AirbyteStateMessage) =
    (
      stateMessage.type == AirbyteStateMessage.AirbyteStateType.STREAM ||
        stateMessage.type == AirbyteStateMessage.AirbyteStateType.GLOBAL
    )

  private val isBookkeeperMode: Boolean = platformMode == ArchitectureConstants.BOOKKEEPER

  @Volatile
  private var noCheckSumError = true

  @Volatile
  private var sourceStateMessageSeen = false

  @Volatile
  private var isClosed = false

  @Volatile
  private var destinationStateMessageSeen = false

  @Volatile
  private var totalSuccessEvents = 0

  @Volatile
  private var totalMissingEvents = 0

  @Volatile
  private var totalMismatchEvents = 0

  fun getCurrentTimeInMicroSecond() = epochMilliSupplier.get() * 1000

  private fun trackStateCountMetrics(
    eventsToPublish: List<StateCheckSumCountEvent>,
    eventType: EventType,
  ) {
    try {
      if (!shouldEmitStateStatsToSegment()) {
        return
      }

      val maxEvents: Int =
        when (eventType) {
          EventType.SUCCESS -> MAX_SUCCESS_EVENTS
          EventType.MISSING -> MAX_MISSING_EVENTS
          EventType.MISMATCH -> MAX_MISMATCH_EVENTS
        }

      var totalEvents: Int =
        when (eventType) {
          EventType.SUCCESS -> totalSuccessEvents
          EventType.MISSING -> totalMissingEvents
          EventType.MISMATCH -> totalMismatchEvents
        }

      if (totalEvents <= maxEvents) {
        pubSubWriter?.publishEvent(eventsToPublish)
        totalEvents += eventsToPublish.size
      }

      when (eventType) {
        EventType.SUCCESS -> totalSuccessEvents = totalEvents
        EventType.MISSING -> totalMissingEvents = totalEvents
        EventType.MISMATCH -> totalMismatchEvents = totalEvents
      }
    } catch (e: Exception) {
      logger.error(e) { "Exception while trying to emit state count metrics" }
    }
  }

  private fun stateCheckSumCountEvent(
    recordCount: Double,
    stateMessage: AirbyteStateMessage,
    stateOrigin: String,
    validData: Boolean,
  ): StateCheckSumCountEvent {
    var streamName: String? = null
    var streamNamespace: String? = null

    if (stateMessage.type == AirbyteStateMessage.AirbyteStateType.STREAM) {
      val nameNamespacePair = getNameNamespacePair(stateMessage)
      if (nameNamespacePair.namespace != null) {
        streamNamespace = nameNamespacePair.namespace
      }
      streamName = nameNamespacePair.name
    }

    val stateCheckSumCountEvent =
      StateCheckSumCountEvent(
        deployment.getDeploymentVersion(),
        attemptNumber.toLong(),
        connectionId.toString(),
        deployment.getDeploymentId().toString(),
        deployment.getDeploymentMode(),
        trackingIdentity.email,
        idSupplier.get().toString(),
        jobId,
        recordCount.toLong(),
        stateMessage.getStateHashCode(Hashing.murmur3_32_fixed()).toString(),
        stateMessage.getStateIdForStatsTracking().toString(),
        stateOrigin,
        stateMessage.type.toString(),
        streamName,
        streamNamespace,
        getCurrentTimeInMicroSecond(),
        validData,
      )
    return stateCheckSumCountEvent
  }

  fun validateStateChecksum(
    stateMessage: AirbyteStateMessage,
    platformRecordCount: Double,
    origin: AirbyteMessageOrigin,
    failOnInvalidChecksum: Boolean,
    checksumValidationEnabled: Boolean,
    includeStreamInLogs: Boolean = true,
    streamPlatformRecordCounts: Map<AirbyteStreamNameNamespacePair, Long> = emptyMap(),
    filteredOutRecords: Double = 0.0,
  ) {
    if (isBookkeeperMode || !isStateTypeSupported(stateMessage)) {
      return
    }
    setStateMessageSeenAttributes(origin)
    val stats: AirbyteStateStats? = extractStats(origin, stateMessage)
    if (stats != null) {
      stats.recordCount?.let { stateRecordCount ->
        if (origin == AirbyteMessageOrigin.DESTINATION) {
          val destinationTotalCount = stateRecordCount + (stats.rejectedRecordCount ?: 0.0)
          val sourceStats: AirbyteStateStats? = stateMessage.sourceStats
          if (sourceStats != null) {
            sourceStats.recordCount?.let { sourceRecordCount ->
              if ((
                  sourceRecordCount.minus(
                    filteredOutRecords,
                  )
                ) != destinationTotalCount ||
                (platformRecordCount.minus(filteredOutRecords)) != destinationTotalCount
              ) {
                misMatchWhenAllThreeCountsArePresent(
                  origin = origin,
                  sourceRecordCount = sourceRecordCount,
                  destinationRecordCount = destinationTotalCount,
                  platformRecordCount = platformRecordCount,
                  includeStreamInLogs = includeStreamInLogs,
                  stateMessage = stateMessage,
                  failOnInvalidChecksum = failOnInvalidChecksum,
                  validData = checksumValidationEnabled,
                )
              } else {
                val shouldIncludeStreamInLogs = includeStreamInLogs || featureFlagClient.boolVariation(LogStateMsgs, Connection(connectionId))
                checksumIsValid(origin, shouldIncludeStreamInLogs, stateMessage, checksumValidationEnabled)
              }
            }
          } else {
            if (platformRecordCount != destinationTotalCount) {
              misMatchBetweenStateCountAndPlatformCount(
                origin = origin,
                stateRecordCount = destinationTotalCount,
                platformRecordCount = platformRecordCount,
                includeStreamInLogs = includeStreamInLogs,
                stateMessage = stateMessage,
                failOnInvalidChecksum = failOnInvalidChecksum,
                validData = checksumValidationEnabled,
                streamPlatformRecordCounts = streamPlatformRecordCounts,
              )
            }
            sourceIsMissingButDestinationIsPresent(destinationTotalCount, platformRecordCount, stateMessage, checksumValidationEnabled)
          }
        } else if (stateRecordCount != platformRecordCount) {
          misMatchBetweenStateCountAndPlatformCount(
            origin = origin,
            stateRecordCount = stateRecordCount,
            platformRecordCount = platformRecordCount,
            includeStreamInLogs = includeStreamInLogs,
            stateMessage = stateMessage,
            failOnInvalidChecksum = failOnInvalidChecksum,
            validData = checksumValidationEnabled,
            streamPlatformRecordCounts = streamPlatformRecordCounts,
          )
        } else {
          val shouldIncludeStreamInLogs = includeStreamInLogs || featureFlagClient.boolVariation(LogStateMsgs, Connection(connectionId))
          checksumIsValid(origin, shouldIncludeStreamInLogs, stateMessage, checksumValidationEnabled)
        }
      }
    } else {
      if (origin == AirbyteMessageOrigin.DESTINATION) {
        val sourceStats: AirbyteStateStats? = stateMessage.sourceStats
        if (sourceStats != null) {
          sourceStats.recordCount?.let { sourceRecordCount ->
            destinationIsMissingButSourceIsPresent(sourceRecordCount, platformRecordCount, stateMessage, checksumValidationEnabled)
          }
        } else {
          sourceDestinationBothMissing(platformRecordCount, stateMessage, checksumValidationEnabled)
        }
      }
    }
  }

  private fun setStateMessageSeenAttributes(origin: AirbyteMessageOrigin) {
    if (!sourceStateMessageSeen && origin == AirbyteMessageOrigin.SOURCE) {
      sourceStateMessageSeen = true
    } else if (!destinationStateMessageSeen && origin == AirbyteMessageOrigin.DESTINATION) {
      destinationStateMessageSeen = true
    }
  }

  private fun sourceDestinationBothMissing(
    platformRecordCount: Double,
    stateMessage: AirbyteStateMessage,
    validData: Boolean,
  ) {
    noCheckSumError = false
    trackStateCountMetrics(
      listOf(
        stateCheckSumCountEvent(platformRecordCount, stateMessage, AirbyteMessageOrigin.INTERNAL.toString(), validData),
      ),
      EventType.MISSING,
    )
  }

  private fun destinationIsMissingButSourceIsPresent(
    sourceRecordCount: Double,
    platformRecordCount: Double,
    stateMessage: AirbyteStateMessage,
    validData: Boolean,
  ) {
    noCheckSumError = false
    trackStateCountMetrics(
      listOf(
        stateCheckSumCountEvent(sourceRecordCount, stateMessage, AirbyteMessageOrigin.SOURCE.toString(), validData),
        stateCheckSumCountEvent(platformRecordCount, stateMessage, AirbyteMessageOrigin.INTERNAL.toString(), validData),
      ),
      EventType.MISSING,
    )
  }

  private fun sourceIsMissingButDestinationIsPresent(
    destinationRecordCount: Double,
    platformRecordCount: Double,
    stateMessage: AirbyteStateMessage,
    validData: Boolean,
  ) {
    noCheckSumError = false
    trackStateCountMetrics(
      listOf(
        stateCheckSumCountEvent(platformRecordCount, stateMessage, AirbyteMessageOrigin.INTERNAL.toString(), validData),
        stateCheckSumCountEvent(destinationRecordCount, stateMessage, AirbyteMessageOrigin.DESTINATION.toString(), validData),
      ),
      EventType.MISSING,
    )
  }

  private fun misMatchWhenAllThreeCountsArePresent(
    origin: AirbyteMessageOrigin,
    sourceRecordCount: Double,
    destinationRecordCount: Double,
    platformRecordCount: Double,
    includeStreamInLogs: Boolean,
    stateMessage: AirbyteStateMessage,
    failOnInvalidChecksum: Boolean,
    validData: Boolean,
  ) {
    noCheckSumError = false
    trackStateCountMetrics(
      listOf(
        stateCheckSumCountEvent(sourceRecordCount, stateMessage, AirbyteMessageOrigin.SOURCE.toString(), validData),
        stateCheckSumCountEvent(platformRecordCount, stateMessage, AirbyteMessageOrigin.INTERNAL.toString(), validData),
        stateCheckSumCountEvent(destinationRecordCount, stateMessage, AirbyteMessageOrigin.DESTINATION.toString(), validData),
      ),
      EventType.MISMATCH,
    )

    logAndFailIfRequired(
      misMatchMessageWhenAllCountsThreeArePresent(
        origin,
        sourceRecordCount,
        destinationRecordCount,
        platformRecordCount,
        includeStreamInLogs,
        stateMessage,
        validData,
      ),
      failOnInvalidChecksum,
      validData,
      origin,
      stateMessage,
    )
  }

  private fun misMatchBetweenStateCountAndPlatformCount(
    origin: AirbyteMessageOrigin,
    stateRecordCount: Double,
    platformRecordCount: Double,
    includeStreamInLogs: Boolean,
    stateMessage: AirbyteStateMessage,
    failOnInvalidChecksum: Boolean,
    validData: Boolean,
    streamPlatformRecordCounts: Map<AirbyteStreamNameNamespacePair, Long>,
  ) {
    noCheckSumError = false
    logAndFailIfRequired(
      stateAndPlatformMismatchMessage(
        origin = origin,
        stateRecordCount = stateRecordCount,
        platformRecordCount = platformRecordCount,
        includeStreamInLogs = includeStreamInLogs,
        stateMessage = stateMessage,
        validData = validData,
        streamPlatformRecordCounts = streamPlatformRecordCounts,
      ),
      failOnInvalidChecksum,
      validData,
      origin,
      stateMessage,
    )
  }

  private fun logAndFailIfRequired(
    errorMessage: String,
    failOnInvalidChecksum: Boolean,
    validData: Boolean,
    origin: AirbyteMessageOrigin,
    stateMessage: AirbyteStateMessage,
  ) {
    logger.error { errorMessage }
    logger.error { "Raw state message with bad count ${Jsons.serialize(stateMessage)}" }
    if (failOnInvalidChecksum && validData) {
      throw InvalidChecksumException(errorMessage)
    } else if (validData) {
      val failureOrigin =
        when (origin) {
          AirbyteMessageOrigin.SOURCE -> FailureReason.FailureOrigin.SOURCE
          AirbyteMessageOrigin.DESTINATION -> FailureReason.FailureOrigin.DESTINATION
          else -> FailureReason.FailureOrigin.AIRBYTE_PLATFORM
        }
      stateCheckSumReporter.reportError(
        workspaceId,
        connectionId,
        jobId,
        attemptNumber,
        failureOrigin,
        errorMessage,
        "The sync appears to have dropped records",
        InvalidChecksumException(errorMessage),
        stateMessage,
      )
      val attributes =
        arrayOf(
          MetricAttribute(ATTEMPT_NUMBER, attemptNumber.toString()),
          MetricAttribute(CONNECTION_ID, connectionId.toString()),
          MetricAttribute(DESTINATION_IMAGE, replicationInput.destinationLauncherConfig.dockerImage),
          MetricAttribute(FAILURE_ORIGIN, failureOrigin.toString()),
          MetricAttribute(JOB_ID, jobId.toString()),
          MetricAttribute(SOURCE_IMAGE, replicationInput.sourceLauncherConfig.dockerImage),
          MetricAttribute(WORKSPACE_ID, workspaceId.toString()),
        )
      metricClient.count(
        metric = OssMetricsRegistry.STATE_CHECKSUM_COUNT_ERROR,
        value = 1L,
        attributes = attributes,
      )
    }
  }

  fun close(completedSuccessfully: Boolean) {
    logger.info { "Closing StateCheckSumCountEventHandler" }
    if (completedSuccessfully && !isClosed && sourceStateMessageSeen && destinationStateMessageSeen && noCheckSumError) {
      logger.info { "No checksum errors were reported in the entire sync." }
      val dummyState = DUMMY_STATE_MESSAGE
      trackStateCountMetrics(
        listOf(
          stateCheckSumCountEvent(1.0, dummyState, AirbyteMessageOrigin.SOURCE.toString(), true),
          stateCheckSumCountEvent(1.0, dummyState, AirbyteMessageOrigin.INTERNAL.toString(), true),
          stateCheckSumCountEvent(1.0, dummyState, AirbyteMessageOrigin.DESTINATION.toString(), true),
        ),
        EventType.SUCCESS,
      )
      isClosed = true
    }
    pubSubWriter?.close()
  }

  companion object {
    private fun stateAndPlatformMismatchMessage(
      origin: AirbyteMessageOrigin,
      stateRecordCount: Double,
      platformRecordCount: Double,
      includeStreamInLogs: Boolean,
      stateMessage: AirbyteStateMessage,
      validData: Boolean,
      streamPlatformRecordCounts: Map<AirbyteStreamNameNamespacePair, Long>,
    ): String =
      "${origin.name.lowercase().replaceFirstChar { it.uppercase() }} state message checksum is invalid: state " +
        "record count $stateRecordCount does not equal platform tracked record count $platformRecordCount" +
        if (includeStreamInLogs) {
          " for stream ${getNameNamespacePair(stateMessage)}."
        } else {
          "."
        } +
        if (validData) {
          " No hash collisions were observed."
        } else {
          " Hash collisions were observed so count comparison result may be wrong."
        } +
        if (includeStreamInLogs) {
          val namesAndCounts =
            streamPlatformRecordCounts
              .map { (name, count) ->
                " $name : $count"
              }.joinToString("\n")
          " Observed the following record counts per stream: \n$namesAndCounts"
        } else {
          ""
        }

    private fun misMatchMessageWhenAllCountsThreeArePresent(
      origin: AirbyteMessageOrigin,
      sourceRecordCount: Double,
      destinationRecordCount: Double,
      platformRecordCount: Double,
      includeStreamInLogs: Boolean,
      stateMessage: AirbyteStateMessage,
      validData: Boolean,
    ): String =
      "${origin.name.lowercase().replaceFirstChar { it.uppercase() }} state message checksum is invalid: " +
        "source record count $sourceRecordCount , destination record count " +
        "$destinationRecordCount and platform record count $platformRecordCount does not equal each other" +
        if (includeStreamInLogs) {
          " for stream ${getNameNamespacePair(stateMessage)}."
        } else {
          "."
        } +
        if (validData) {
          " No hash collisions were observed."
        } else {
          " Hash collisions were observed so count comparison result may be wrong."
        }

    private fun checksumIsValid(
      origin: AirbyteMessageOrigin,
      includeStreamInLogs: Boolean,
      stateMessage: AirbyteStateMessage,
      validData: Boolean,
    ) {
      logger.debug {
        "${
          origin.name.lowercase().replaceFirstChar { it.uppercase() }
        } state message checksum is valid" +
          if (includeStreamInLogs) {
            " for stream ${getNameNamespacePair(stateMessage)}."
          } else {
            "."
          } +
          if (validData) {
            " No hash collisions were observed."
          } else {
            " Hash collisions were observed so count comparison result may be wrong."
          }
      }
    }

    private fun extractStats(
      origin: AirbyteMessageOrigin,
      stateMessage: AirbyteStateMessage,
    ): AirbyteStateStats? {
      val stats: AirbyteStateStats? =
        when (origin) {
          AirbyteMessageOrigin.SOURCE -> stateMessage.sourceStats
          AirbyteMessageOrigin.DESTINATION -> stateMessage.destinationStats
          else -> null
        }
      return stats
    }

    @JvmStatic
    val DUMMY_STATE_MESSAGE: AirbyteStateMessage =
      attachIdToStateMessageFromSource(
        AirbyteStateMessage()
          .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
          .withStream(
            AirbyteStreamState()
              .withStreamState(Jsons.jsonNode(mapOf("cursor" to "value")))
              .withStreamDescriptor(StreamDescriptor().withNamespace("dummy-namespace").withName("dummy-name")),
          ).withSourceStats(AirbyteStateStats().withRecordCount(1.0))
          .withDestinationStats(AirbyteStateStats().withRecordCount(1.0)),
      )

    private fun <T> retry(supplier: CheckedSupplier<T>): T =
      Failsafe
        .with(
          RetryPolicy
            .builder<T>()
            .withBackoff(Duration.ofMillis(10), Duration.ofMillis(100))
            .withMaxRetries(5)
            .build(),
        ).get(supplier)
  }

  enum class EventType {
    MISSING,
    MISMATCH,
    SUCCESS,
  }
}
