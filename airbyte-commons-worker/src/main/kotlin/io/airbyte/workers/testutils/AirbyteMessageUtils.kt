/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.testutils

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteAnalyticsTraceMessage
import io.airbyte.protocol.models.v0.AirbyteControlConnectorConfigMessage
import io.airbyte.protocol.models.v0.AirbyteControlMessage
import io.airbyte.protocol.models.v0.AirbyteErrorTraceMessage
import io.airbyte.protocol.models.v0.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteLogMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.Config
import io.airbyte.protocol.models.v0.StreamDescriptor
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

/**
 * Static utility methods for creating [AirbyteMessage]s.
 */
object AirbyteMessageUtils {
  fun createRecordMessage(
    tableName: String?,
    record: JsonNode?,
    timeExtracted: Instant,
  ): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.RECORD)
      .withRecord(
        AirbyteRecordMessage()
          .withData(record)
          .withStream(tableName)
          .withEmittedAt(timeExtracted.epochSecond),
      )

  @JvmStatic
  fun createRecordMessage(
    tableName: String?,
    key: String,
    value: String,
  ): AirbyteMessage = createRecordMessage(tableName, ImmutableMap.of(key, value))

  fun createRecordMessage(
    tableName: String?,
    key: String,
    value: Int,
  ): AirbyteMessage = createRecordMessage(tableName, ImmutableMap.of(key, value))

  fun createRecordMessage(
    tableName: String?,
    key: String,
    value: BigInteger,
  ): AirbyteMessage = createRecordMessage(tableName, ImmutableMap.of(key, value))

  fun createRecordMessage(
    tableName: String?,
    key: String,
    value: BigDecimal,
  ): AirbyteMessage = createRecordMessage(tableName, ImmutableMap.of(key, value))

  fun createRecordMessage(
    tableName: String?,
    record: Map<String, *>?,
  ): AirbyteMessage = createRecordMessage(tableName, Jsons.jsonNode(record), Instant.EPOCH)

  fun createRecordMessage(
    streamName: String?,
    recordData: Int,
  ): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.RECORD)
      .withRecord(AirbyteRecordMessage().withStream(streamName).withData(Jsons.jsonNode(recordData)))

  fun createLogMessage(
    level: AirbyteLogMessage.Level?,
    message: String?,
  ): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.LOG)
      .withLog(
        AirbyteLogMessage()
          .withLevel(level)
          .withMessage(message),
      )

  @JvmStatic
  fun createStateMessage(stateData: Int): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.STATE)
      .withState(AirbyteStateMessage().withData(Jsons.jsonNode(stateData)))

  fun createStateMessage(
    streamName: String?,
    key: String,
    value: String,
  ): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.STATE)
      .withState(
        AirbyteStateMessage()
          .withStream(createStreamState(streamName))
          .withData(Jsons.jsonNode(ImmutableMap.of(key, value))),
      )

  fun createStreamStateMessage(
    streamName: String?,
    stateData: Int,
  ): AirbyteStateMessage =
    AirbyteStateMessage()
      .withType(AirbyteStateType.STREAM)
      .withStream(createStreamState(streamName).withStreamState(Jsons.jsonNode(stateData)))

  fun createGlobalStateMessage(
    stateData: Int,
    vararg streamNames: String?,
  ): AirbyteMessage {
    val streamStates: MutableList<AirbyteStreamState> = ArrayList()
    for (streamName in streamNames) {
      streamStates.add(createStreamState(streamName).withStreamState(Jsons.jsonNode(stateData)))
    }
    return AirbyteMessage()
      .withType(AirbyteMessage.Type.STATE)
      .withState(AirbyteStateMessage().withType(AirbyteStateType.GLOBAL).withGlobal(AirbyteGlobalState().withStreamStates(streamStates)))
  }

  fun createStreamState(streamName: String?): AirbyteStreamState = AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName(streamName))

  fun createStreamEstimateMessage(
    name: String?,
    namespace: String?,
    byteEst: Long,
    rowEst: Long,
  ): AirbyteMessage = createEstimateMessage(AirbyteEstimateTraceMessage.Type.STREAM, name, namespace, byteEst, rowEst)

  fun createSyncEstimateMessage(
    byteEst: Long,
    rowEst: Long,
  ): AirbyteMessage = createEstimateMessage(AirbyteEstimateTraceMessage.Type.SYNC, null, null, byteEst, rowEst)

  fun createEstimateMessage(
    type: AirbyteEstimateTraceMessage.Type?,
    name: String?,
    namespace: String?,
    byteEst: Long,
    rowEst: Long,
  ): AirbyteMessage {
    val est =
      AirbyteEstimateTraceMessage()
        .withType(type)
        .withByteEstimate(byteEst)
        .withRowEstimate(rowEst)

    if (name != null) {
      est.withName(name)
    }
    if (namespace != null) {
      est.withNamespace(namespace)
    }

    return AirbyteMessage()
      .withType(AirbyteMessage.Type.TRACE)
      .withTrace(
        AirbyteTraceMessage()
          .withType(AirbyteTraceMessage.Type.ESTIMATE)
          .withEstimate(est),
      )
  }

  fun createErrorMessage(
    message: String?,
    emittedAt: Double?,
  ): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.TRACE)
      .withTrace(createErrorTraceMessage(message, emittedAt))

  @JvmStatic
  @JvmOverloads
  fun createErrorTraceMessage(
    message: String?,
    emittedAt: Double?,
    failureType: AirbyteErrorTraceMessage.FailureType? = null,
  ): AirbyteTraceMessage {
    val msg =
      AirbyteTraceMessage()
        .withType(AirbyteTraceMessage.Type.ERROR)
        .withError(AirbyteErrorTraceMessage().withMessage(message))
        .withEmittedAt(emittedAt)

    if (failureType != null) {
      msg.error.withFailureType(failureType)
    }

    return msg
  }

  @JvmStatic
  fun createConfigControlMessage(
    config: Config?,
    emittedAt: Double?,
  ): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.CONTROL)
      .withControl(
        AirbyteControlMessage()
          .withEmittedAt(emittedAt)
          .withType(AirbyteControlMessage.Type.CONNECTOR_CONFIG)
          .withConnectorConfig(
            AirbyteControlConnectorConfigMessage()
              .withConfig(config),
          ),
      )

  @JvmOverloads
  fun createStatusTraceMessage(
    stream: StreamDescriptor?,
    status: AirbyteStreamStatus?,
    emittedAt: Long = System.currentTimeMillis(),
  ): AirbyteMessage {
    val airbyteStreamStatusTraceMessage =
      AirbyteStreamStatusTraceMessage()
        .withStatus(status)
        .withStreamDescriptor(stream)

    val airbyteTraceMessage =
      AirbyteTraceMessage()
        .withEmittedAt(emittedAt.toDouble())
        .withType(AirbyteTraceMessage.Type.STREAM_STATUS)
        .withStreamStatus(airbyteStreamStatusTraceMessage)

    return AirbyteMessage()
      .withType(AirbyteMessage.Type.TRACE)
      .withTrace(airbyteTraceMessage)
  }

  fun createStatusTraceMessage(
    stream: StreamDescriptor?,
    type: AirbyteTraceMessage.Type?,
  ): AirbyteMessage {
    val airbyteStreamStatusTraceMessage =
      AirbyteStreamStatusTraceMessage()
        .withStatus(AirbyteStreamStatus.STARTED)
        .withStreamDescriptor(stream)

    val airbyteTraceMessage =
      AirbyteTraceMessage()
        .withEmittedAt(System.currentTimeMillis().toDouble())
        .withType(type)
        .withStreamStatus(airbyteStreamStatusTraceMessage)

    if (type != null) {
      when (type) {
        AirbyteTraceMessage.Type.ERROR -> {
          val error = AirbyteErrorTraceMessage()
          airbyteTraceMessage.withError(error)
        }

        AirbyteTraceMessage.Type.ESTIMATE -> {
          val estimate = AirbyteEstimateTraceMessage()
          airbyteTraceMessage.withEstimate(estimate)
        }

        AirbyteTraceMessage.Type.STREAM_STATUS -> {
          val streamStatus = AirbyteStreamStatusTraceMessage()
          airbyteTraceMessage.withStreamStatus(streamStatus)
        }

        else -> {}
      }
    }

    return AirbyteMessage()
      .withType(AirbyteMessage.Type.TRACE)
      .withTrace(airbyteTraceMessage)
  }

  fun createStreamStatusTraceMessageWithType(
    stream: StreamDescriptor?,
    status: AirbyteStreamStatus?,
  ): AirbyteMessage {
    val airbyteStreamStatusTraceMessage =
      AirbyteStreamStatusTraceMessage()
        .withStatus(status)
        .withStreamDescriptor(stream)

    val airbyteTraceMessage =
      AirbyteTraceMessage()
        .withEmittedAt(null)
        .withType(AirbyteTraceMessage.Type.STREAM_STATUS)
        .withStreamStatus(airbyteStreamStatusTraceMessage)

    return AirbyteMessage()
      .withType(AirbyteMessage.Type.TRACE)
      .withTrace(airbyteTraceMessage)
  }

  fun createAnalyticsTraceMessage(
    type: String?,
    value: String?,
  ): AirbyteMessage {
    val airbyteAnalyticsTraceMessage =
      AirbyteAnalyticsTraceMessage()
        .withType(type)
        .withValue(value)

    val airbyteTraceMessage =
      AirbyteTraceMessage()
        .withEmittedAt(System.currentTimeMillis().toDouble())
        .withType(AirbyteTraceMessage.Type.ANALYTICS)
        .withAnalytics(airbyteAnalyticsTraceMessage)

    return AirbyteMessage()
      .withType(AirbyteMessage.Type.TRACE)
      .withTrace(airbyteTraceMessage)
  }
}
