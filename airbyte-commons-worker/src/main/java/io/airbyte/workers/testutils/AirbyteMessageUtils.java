/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.testutils;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.v0.AirbyteAnalyticsTraceMessage;
import io.airbyte.protocol.models.v0.AirbyteControlConnectorConfigMessage;
import io.airbyte.protocol.models.v0.AirbyteControlMessage;
import io.airbyte.protocol.models.v0.AirbyteErrorTraceMessage;
import io.airbyte.protocol.models.v0.AirbyteEstimateTraceMessage;
import io.airbyte.protocol.models.v0.AirbyteGlobalState;
import io.airbyte.protocol.models.v0.AirbyteLogMessage;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteMessage.Type;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.v0.AirbyteStreamState;
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage;
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus;
import io.airbyte.protocol.models.v0.AirbyteTraceMessage;
import io.airbyte.protocol.models.v0.Config;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Static utility methods for creating {@link AirbyteMessage}s.
 */
public class AirbyteMessageUtils {

  public static AirbyteMessage createRecordMessage(final String tableName,
                                                   final JsonNode record,
                                                   final Instant timeExtracted) {

    return new AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(new AirbyteRecordMessage()
            .withData(record)
            .withStream(tableName)
            .withEmittedAt(timeExtracted.getEpochSecond()));
  }

  public static AirbyteMessage createRecordMessage(final String tableName,
                                                   final String key,
                                                   final String value) {
    return createRecordMessage(tableName, ImmutableMap.of(key, value));
  }

  public static AirbyteMessage createRecordMessage(final String tableName,
                                                   final String key,
                                                   final Integer value) {
    return createRecordMessage(tableName, ImmutableMap.of(key, value));
  }

  public static AirbyteMessage createRecordMessage(final String tableName,
                                                   final String key,
                                                   final BigInteger value) {
    return createRecordMessage(tableName, ImmutableMap.of(key, value));
  }

  public static AirbyteMessage createRecordMessage(final String tableName,
                                                   final String key,
                                                   final BigDecimal value) {
    return createRecordMessage(tableName, ImmutableMap.of(key, value));
  }

  public static AirbyteMessage createRecordMessage(final String tableName,
                                                   final Map<String, ?> record) {
    return createRecordMessage(tableName, Jsons.jsonNode(record), Instant.EPOCH);
  }

  public static AirbyteMessage createRecordMessage(final String streamName, final int recordData) {
    return new AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(new AirbyteRecordMessage().withStream(streamName).withData(Jsons.jsonNode(recordData)));
  }

  public static AirbyteMessage createLogMessage(final AirbyteLogMessage.Level level,
                                                final String message) {

    return new AirbyteMessage()
        .withType(AirbyteMessage.Type.LOG)
        .withLog(new AirbyteLogMessage()
            .withLevel(level)
            .withMessage(message));
  }

  public static AirbyteMessage createStateMessage(final int stateData) {
    return new AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(new AirbyteStateMessage().withData(Jsons.jsonNode(stateData)));
  }

  public static AirbyteMessage createStateMessage(final String streamName, final String key, final String value) {
    return new AirbyteMessage()
        .withType(Type.STATE)
        .withState(new AirbyteStateMessage()
            .withStream(createStreamState(streamName))
            .withData(Jsons.jsonNode(ImmutableMap.of(key, value))));
  }

  public static AirbyteStateMessage createStreamStateMessage(final String streamName, final int stateData) {
    return new AirbyteStateMessage()
        .withType(AirbyteStateType.STREAM)
        .withStream(createStreamState(streamName).withStreamState(Jsons.jsonNode(stateData)));
  }

  public static AirbyteMessage createGlobalStateMessage(final int stateData, final String... streamNames) {
    final List<AirbyteStreamState> streamStates = new ArrayList<>();
    for (final String streamName : streamNames) {
      streamStates.add(createStreamState(streamName).withStreamState(Jsons.jsonNode(stateData)));
    }
    return new AirbyteMessage()
        .withType(Type.STATE)
        .withState(new AirbyteStateMessage().withType(AirbyteStateType.GLOBAL).withGlobal(new AirbyteGlobalState().withStreamStates(streamStates)));
  }

  public static AirbyteStreamState createStreamState(final String streamName) {
    return new AirbyteStreamState().withStreamDescriptor(new StreamDescriptor().withName(streamName));
  }

  public static AirbyteMessage createStreamEstimateMessage(final String name, final String namespace, final long byteEst, final long rowEst) {
    return createEstimateMessage(AirbyteEstimateTraceMessage.Type.STREAM, name, namespace, byteEst, rowEst);
  }

  public static AirbyteMessage createSyncEstimateMessage(final long byteEst, final long rowEst) {
    return createEstimateMessage(AirbyteEstimateTraceMessage.Type.SYNC, null, null, byteEst, rowEst);
  }

  public static AirbyteMessage createEstimateMessage(final AirbyteEstimateTraceMessage.Type type,
                                                     final String name,
                                                     final String namespace,
                                                     final long byteEst,
                                                     final long rowEst) {
    final var est = new AirbyteEstimateTraceMessage()
        .withType(type)
        .withByteEstimate(byteEst)
        .withRowEstimate(rowEst);

    if (name != null) {
      est.withName(name);
    }
    if (namespace != null) {
      est.withNamespace(namespace);
    }

    return new AirbyteMessage()
        .withType(Type.TRACE)
        .withTrace(new AirbyteTraceMessage().withType(AirbyteTraceMessage.Type.ESTIMATE)
            .withEstimate(est));
  }

  public static AirbyteMessage createErrorMessage(final String message, final Double emittedAt) {
    return new AirbyteMessage()
        .withType(AirbyteMessage.Type.TRACE)
        .withTrace(createErrorTraceMessage(message, emittedAt));
  }

  public static AirbyteTraceMessage createErrorTraceMessage(final String message, final Double emittedAt) {
    return createErrorTraceMessage(message, emittedAt, null);
  }

  public static AirbyteTraceMessage createErrorTraceMessage(final String message,
                                                            final Double emittedAt,
                                                            final AirbyteErrorTraceMessage.FailureType failureType) {
    final var msg = new AirbyteTraceMessage()
        .withType(io.airbyte.protocol.models.v0.AirbyteTraceMessage.Type.ERROR)
        .withError(new AirbyteErrorTraceMessage().withMessage(message))
        .withEmittedAt(emittedAt);

    if (failureType != null) {
      msg.getError().withFailureType(failureType);
    }

    return msg;
  }

  public static AirbyteMessage createConfigControlMessage(final Config config, final Double emittedAt) {
    return new AirbyteMessage()
        .withType(Type.CONTROL)
        .withControl(new AirbyteControlMessage()
            .withEmittedAt(emittedAt)
            .withType(AirbyteControlMessage.Type.CONNECTOR_CONFIG)
            .withConnectorConfig(new AirbyteControlConnectorConfigMessage()
                .withConfig(config)));
  }

  public static AirbyteMessage createStatusTraceMessage(final StreamDescriptor stream, final AirbyteStreamStatus status) {
    return createStatusTraceMessage(stream, status, System.currentTimeMillis());
  }

  public static AirbyteMessage createStatusTraceMessage(final StreamDescriptor stream,
                                                        final AirbyteStreamStatus status,
                                                        final Long emittedAt) {
    final AirbyteStreamStatusTraceMessage airbyteStreamStatusTraceMessage = new AirbyteStreamStatusTraceMessage()
        .withStatus(status)
        .withStreamDescriptor(stream);

    final AirbyteTraceMessage airbyteTraceMessage = new AirbyteTraceMessage()
        .withEmittedAt(emittedAt.doubleValue())
        .withType(AirbyteTraceMessage.Type.STREAM_STATUS)
        .withStreamStatus(airbyteStreamStatusTraceMessage);

    return new AirbyteMessage()
        .withType(Type.TRACE)
        .withTrace(airbyteTraceMessage);
  }

  public static AirbyteMessage createStatusTraceMessage(final StreamDescriptor stream, final AirbyteTraceMessage.Type type) {
    final AirbyteStreamStatusTraceMessage airbyteStreamStatusTraceMessage = new AirbyteStreamStatusTraceMessage()
        .withStatus(AirbyteStreamStatus.STARTED)
        .withStreamDescriptor(stream);

    final AirbyteTraceMessage airbyteTraceMessage = new AirbyteTraceMessage()
        .withEmittedAt(Long.valueOf(System.currentTimeMillis()).doubleValue())
        .withType(type)
        .withStreamStatus(airbyteStreamStatusTraceMessage);

    if (type != null) {
      switch (type) {
        case ERROR -> {
          final var error = new AirbyteErrorTraceMessage();
          airbyteTraceMessage.withError(error);
        }
        case ESTIMATE -> {
          final var estimate = new AirbyteEstimateTraceMessage();
          airbyteTraceMessage.withEstimate(estimate);
        }
        case STREAM_STATUS -> {
          final var streamStatus = new AirbyteStreamStatusTraceMessage();
          airbyteTraceMessage.withStreamStatus(streamStatus);
        }
        default -> {
          // Do nothing.
        }
      }
    }

    return new AirbyteMessage()
        .withType(Type.TRACE)
        .withTrace(airbyteTraceMessage);
  }

  public static AirbyteMessage createStreamStatusTraceMessageWithType(final StreamDescriptor stream,
                                                                      final AirbyteStreamStatusTraceMessage.AirbyteStreamStatus status) {
    final AirbyteStreamStatusTraceMessage airbyteStreamStatusTraceMessage = new AirbyteStreamStatusTraceMessage()
        .withStatus(status)
        .withStreamDescriptor(stream);

    final AirbyteTraceMessage airbyteTraceMessage = new AirbyteTraceMessage()
        .withEmittedAt(null)
        .withType(AirbyteTraceMessage.Type.STREAM_STATUS)
        .withStreamStatus(airbyteStreamStatusTraceMessage);

    return new AirbyteMessage()
        .withType(Type.TRACE)
        .withTrace(airbyteTraceMessage);
  }

  public static AirbyteMessage createAnalyticsTraceMessage(final String type, final String value) {
    final AirbyteAnalyticsTraceMessage airbyteAnalyticsTraceMessage = new AirbyteAnalyticsTraceMessage()
        .withType(type)
        .withValue(value);

    final AirbyteTraceMessage airbyteTraceMessage = new AirbyteTraceMessage()
        .withEmittedAt(Long.valueOf(System.currentTimeMillis()).doubleValue())
        .withType(AirbyteTraceMessage.Type.ANALYTICS)
        .withAnalytics(airbyteAnalyticsTraceMessage);

    return new AirbyteMessage()
        .withType(Type.TRACE)
        .withTrace(airbyteTraceMessage);
  }

}
