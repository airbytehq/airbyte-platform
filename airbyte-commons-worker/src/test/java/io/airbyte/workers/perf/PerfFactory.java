/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.perf;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.perf.PerfDestinationFactory.DestinationHelper;
import io.airbyte.workers.perf.PerfDestinationFactory.PerfAirbyteDestination;
import io.airbyte.workers.perf.PerfSourceFactory.PerfAirbyteSource;
import io.airbyte.workers.perf.PerfSourceFactory.SourceHelper;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Builder;

/**
 * Factory for creating performance focused sources and destinations.
 */
public class PerfFactory {

  /**
   * Returns a well-defined AirbyteSource for testing purposes.
   *
   * @param config Configuration that tells this source how to behave.
   * @return AirbyteSource based on the provided configuration.
   */
  public static AirbyteSource createSource(final SourceConfig config) {
    return new PerfAirbyteSource(new SourceHelper(config));
  }

  /**
   * Returns a well-defined AirbyteDestination for testing purposes.
   *
   * @param config Configuration that tells this destination how to behave.
   * @return AirbyteDestination based on the provided configuration.
   */
  public static AirbyteDestination createDestination(final DestinationConfig config) {
    return new PerfAirbyteDestination(new DestinationHelper(config));
  }

  /**
   * Returns a DestinationConfigBuilder class to configure the DestinationConfig.
   *
   * @return DestinationConfigBuilder
   */
  public static DestinationConfig.DestinationConfigBuilder createDestinationConfigBuilder() {
    return DestinationConfig.builder();
  }

  /**
   * Returns a SourceConfigBuilder class to configure the SourceConfig.
   *
   * @return SourceConfigBuilder
   */
  public static SourceConfig.SourceConfigBuilder createSourceConfigBuilder() {
    return SourceConfig.builder();
  }

  /**
   * Configuration options for creating a perf destination.
   * <p>
   * Destinations both accept (via accept) and return (via attemptRead) messages.
   *
   * @param readInitialWait How long to block on the initial attemptRead call.
   * @param echoState Should this destination echo state messages? I.e. should every state message
   *        received in the accept call also be returned via the attemptRead call.
   * @param echoLog Should this destination echo log messages? I.e. should every log message received
   *        in the accept call also be returned via the attemptRead call.
   * @param acceptBatchSize After how many messages should the accept call block? Set to 0 to disable
   *        this behavior.
   * @param acceptNumRecords How many records should this destination accept before it marks itself as
   *        finished?
   * @param readBatchSize After how many messages should the attemptRead call block? Set to 0 to
   *        disable this behavior.
   * @param readBatchWait After readBatchSize messages, block for this amount of time.
   * @param acceptInitialWait How long to block on the initial accept call.
   * @param acceptBatchWait After acceptBatchSize messages, block for this amount of time.
   * @param acceptStateWait TODO - not implemented yet as I'm not sure if it's useful.
   * @param acceptLogWait TODO - not implemented yet as I'm not sure if it's useful.
   * @param exitValue What value the exitValue method should return.
   */
  @Builder
  record DestinationConfig(
                           Duration readInitialWait,
                           boolean echoState,
                           boolean echoLog,
                           int acceptBatchSize,
                           int acceptNumRecords,
                           int readBatchSize,
                           Duration readBatchWait,
                           Duration acceptInitialWait,
                           Duration acceptBatchWait,
                           Duration acceptStateWait,
                           Duration acceptLogWait,
                           int exitValue) {

    DestinationConfig {
      if (readInitialWait == null) {
        readInitialWait = Duration.ZERO;
      }
      if (readBatchWait == null) {
        readBatchWait = Duration.ZERO;
      }
      if (acceptInitialWait == null) {
        acceptInitialWait = Duration.ZERO;
      }
      if (acceptBatchWait == null) {
        acceptBatchWait = Duration.ZERO;
      }
    }

  }

  /**
   * Configuration options for creating a perf source.
   * <p>
   * Sources only return messages via the attemptRead method.
   *
   * @param readRecords Records that will be returned from the attemptRead call. The attemptRead
   *        returns only a single record at a time, this list will be looped over until the source is
   *        finished.
   * @param readLogs Logs that will be returned from the attemptRead call. The attemptRead returns
   *        only a single record at a time, this * list will be looped over until the source is
   *        finished.
   * @param readNumRecords How many records should be returned from attemptRead before this source is
   *        marked as finished.
   * @param readBatchSize After how many messages should the attemptRead call block? Set to 0 to
   *        disable this behavior.
   * @param readInitialWait How long to block on the initial attemptRead call.
   * @param readBatchWait After readBatchSize messages, block for this amount of time.
   * @param readLogEvery Have attemptRead return a log message after this many record messages
   *        returned.
   * @param readStateEvery Have attemptRead return a state message after this many record messages
   *        returned.
   * @param exitValue What value the exitValue method should return.
   */
  @Builder
  record SourceConfig(
                      List<AirbyteMessage> readRecords,
                      List<AirbyteMessage> readLogs,
                      int readNumRecords,
                      int readBatchSize,
                      Duration readInitialWait,
                      Duration readBatchWait,
                      int readLogEvery,
                      int readStateEvery,
                      int exitValue) {

    SourceConfig {
      if (readNumRecords > 0 && (readRecords == null || readRecords.isEmpty())) {
        throw new IllegalArgumentException("cannot specify readNumRecords without also specifying readRecords");
      }
      if (readLogEvery > 0 && (readLogs == null || readLogs.isEmpty())) {
        throw new IllegalArgumentException("cannot specify readLogsEvey without also specifying readLogs");
      }
      if (readInitialWait == null) {
        readInitialWait = Duration.ZERO;
      }
      if (readBatchWait == null) {
        readBatchWait = Duration.ZERO;
      }
    }

  }

  /**
   * Helper method that exists to aid in blocking for a given duration of time.
   *
   * @param duration how long to block for.
   */
  static void sleep(Duration duration) {
    if (duration.isZero()) {
      return;
    }

    final var sleepSeconds = duration.toSeconds();
    try {
      TimeUnit.SECONDS.sleep(sleepSeconds);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
