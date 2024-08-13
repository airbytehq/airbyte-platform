package io.airbyte.workers.general

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
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.EmitStateStatsToSegment
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.LogStateMsgs
import io.airbyte.featureflag.LogStreamNamesInSateMessage
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Workspace
import io.airbyte.protocol.models.AirbyteStateMessage
import io.airbyte.protocol.models.AirbyteStateStats
import io.airbyte.protocol.models.AirbyteStreamState
import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.workers.exception.InvalidChecksumException
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin
import io.airbyte.workers.internal.bookkeeping.getNameNamespacePair
import io.airbyte.workers.internal.bookkeeping.getStateHashCode
import io.airbyte.workers.internal.bookkeeping.getStateIdForStatsTracking
import io.airbyte.workers.models.StateCheckSumCountEvent
import io.airbyte.workers.models.StateWithId
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Parameter
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

private val logger = KotlinLogging.logger { }

private const val MAX_MISSING_EVENTS = 25
private const val MAX_MISMATCH_EVENTS = 25
private const val MAX_SUCCESS_EVENTS = 3

@Singleton
class StateCheckSumCountEventHandler(
  @Named("pubSubWriter") private val pubSubWriter: Optional<StateCheckSumEventPubSubWriter>,
  private val featureFlagClient: FeatureFlagClient,
  private val deploymentFetcher: DeploymentFetcher,
  private val trackingIdentityFetcher: TrackingIdentityFetcher,
  private val stateCheckSumReporter: StateCheckSumErrorReporter,
  @param:Parameter private val connectionId: UUID,
  @param:Parameter private val workspaceId: UUID,
  @param:Parameter private val jobId: Long,
  @param:Parameter private val attemptNumber: Int,
  @param:Parameter private val epochMilliSupplier: Supplier<Long>? = Supplier { Instant.now().toEpochMilli() },
  @param:Parameter private val idSupplier: Supplier<UUID>? = Supplier { UUID.randomUUID() },
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

  private val trackingIdentity: TrackingIdentity by lazy { retry { trackingIdentityFetcher.apply(workspaceId) } }

  private fun shouldEmitStateStatsToSegment(): Boolean = emitStatsCounterFlag

  private fun isStateTypeSupported(stateMessage: AirbyteStateMessage) =
    (
      stateMessage.type == AirbyteStateMessage.AirbyteStateType.STREAM ||
        stateMessage.type == AirbyteStateMessage.AirbyteStateType.GLOBAL
    )

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

  fun getCurrentTimeInMicroSecond() = epochMilliSupplier!!.get() * 1000

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
        pubSubWriter.ifPresent { it.publishEvent(eventsToPublish) }
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
        idSupplier!!.get().toString(),
        jobId.toString(),
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
  ) {
    if (!isStateTypeSupported(stateMessage)) {
      return
    }
    setStateMessageSeenAttributes(origin)
    val stats: AirbyteStateStats? = extractStats(origin, stateMessage)
    if (stats != null) {
      stats.recordCount?.let { stateRecordCount ->
        if (origin == AirbyteMessageOrigin.DESTINATION) {
          val sourceStats: AirbyteStateStats? = stateMessage.sourceStats
          if (sourceStats != null) {
            sourceStats.recordCount?.let { sourceRecordCount ->
              if (sourceRecordCount != stateRecordCount || platformRecordCount != stateRecordCount) {
                misMatchWhenAllThreeCountsArePresent(
                  origin,
                  sourceRecordCount,
                  stateRecordCount,
                  platformRecordCount,
                  includeStreamInLogs,
                  stateMessage,
                  failOnInvalidChecksum,
                  checksumValidationEnabled,
                )
              } else {
                val shouldIncludeStreamInLogs = includeStreamInLogs || featureFlagClient.boolVariation(LogStateMsgs, Connection(connectionId))
                checksumIsValid(origin, shouldIncludeStreamInLogs, stateMessage, checksumValidationEnabled)
              }
            }
          } else {
            if (platformRecordCount != stateRecordCount) {
              misMatchBetweenStateCountAndPlatformCount(
                origin,
                stateRecordCount,
                platformRecordCount,
                includeStreamInLogs,
                stateMessage,
                failOnInvalidChecksum,
                checksumValidationEnabled,
              )
            }
            sourceIsMissingButDestinationIsPresent(stateRecordCount, platformRecordCount, stateMessage, checksumValidationEnabled)
          }
        } else if (stateRecordCount != platformRecordCount) {
          misMatchBetweenStateCountAndPlatformCount(
            origin,
            stateRecordCount,
            platformRecordCount,
            includeStreamInLogs,
            stateMessage,
            failOnInvalidChecksum,
            checksumValidationEnabled,
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
  ) {
    noCheckSumError = false
    logAndFailIfRequired(
      stateAndPlatformMismatchMessage(origin, stateRecordCount, platformRecordCount, includeStreamInLogs, stateMessage, validData),
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
    pubSubWriter.ifPresent { it.close() }
  }

  companion object {
    private fun stateAndPlatformMismatchMessage(
      origin: AirbyteMessageOrigin,
      stateRecordCount: Double,
      platformRecordCount: Double,
      includeStreamInLogs: Boolean,
      stateMessage: AirbyteStateMessage,
      validData: Boolean,
    ): String {
      return "${origin.name.lowercase().replaceFirstChar { it.uppercase() }} state message checksum is invalid: state " +
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
        }
    }

    private fun misMatchMessageWhenAllCountsThreeArePresent(
      origin: AirbyteMessageOrigin,
      sourceRecordCount: Double,
      destinationRecordCount: Double,
      platformRecordCount: Double,
      includeStreamInLogs: Boolean,
      stateMessage: AirbyteStateMessage,
      validData: Boolean,
    ): String {
      return "${origin.name.lowercase().replaceFirstChar { it.uppercase() }} state message checksum is invalid: " +
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
      StateWithId.attachIdToStateMessageFromSource(
        AirbyteStateMessage()
          .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
          .withStream(
            AirbyteStreamState()
              .withStreamState(Jsons.jsonNode(mapOf("cursor" to "value")))
              .withStreamDescriptor(StreamDescriptor().withNamespace("dummy-namespace").withName("dummy-name")),
          )
          .withSourceStats(AirbyteStateStats().withRecordCount(1.0))
          .withDestinationStats(AirbyteStateStats().withRecordCount(1.0)),
      )

    private fun <T> retry(supplier: CheckedSupplier<T>): T {
      return Failsafe.with(
        RetryPolicy.builder<T>()
          .withBackoff(Duration.ofMillis(10), Duration.ofMillis(100))
          .withMaxRetries(5)
          .build(),
      )
        .get(supplier)
    }
  }

  enum class EventType {
    MISSING,
    MISMATCH,
    SUCCESS,
  }
}
