/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler;

import io.airbyte.commons.temporal.JobMetadata;
import io.airbyte.commons.temporal.TemporalResponse;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.JobConfig.ConfigType;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/**
 * Response wrapper for synchronous temporal jobs.
 *
 * @param <T> output type of the job
 */
public class SynchronousResponse<T> {

  private final T output;
  private final SynchronousJobMetadata metadata;

  public static <T> SynchronousResponse<T> error(final SynchronousJobMetadata metadata) {
    return new SynchronousResponse<>(null, metadata);
  }

  public static <T> SynchronousResponse<T> success(final T output, final SynchronousJobMetadata metadata) {
    return new SynchronousResponse<>(output, metadata);
  }

  /**
   * Response from synchronous temporal job.
   *
   * @param temporalResponse response from temporal client
   * @param outputMapper function to retrieve output of the job
   * @param id job id
   * @param configType job type
   * @param configId id of resource for job type (i.e. if configType is discover config id is going to
   *        be a source id)
   * @param createdAt time the job was created
   * @param endedAt time the job ended
   * @param <T> output for job type
   * @param <U> output type for job type of temporal job
   * @return response
   */
  public static <T> SynchronousResponse<T> fromTemporalResponse(final TemporalResponse<ConnectorJobOutput> temporalResponse,
                                                                final Function<ConnectorJobOutput, T> outputMapper,
                                                                final UUID id,
                                                                final ConfigType configType,
                                                                final UUID configId,
                                                                final long createdAt,
                                                                final long endedAt) {

    final Optional<ConnectorJobOutput> jobOutput = temporalResponse.getOutput();
    final T responseOutput = jobOutput.map(outputMapper).orElse(null);

    final Path logPath = temporalResponse.getMetadata() == null ? null : temporalResponse.getMetadata().getLogPath();
    final JobMetadata metadataResponse = responseOutput == null
        ? new JobMetadata(false, logPath)
        : temporalResponse.getMetadata();

    final SynchronousJobMetadata metadata = SynchronousJobMetadata.fromJobMetadata(
        metadataResponse,
        jobOutput.orElse(null),
        id,
        configType,
        configId,
        createdAt,
        endedAt);
    return new SynchronousResponse<>(responseOutput, metadata);
  }

  public SynchronousResponse(final T output, final SynchronousJobMetadata metadata) {
    this.output = output;
    this.metadata = metadata;
  }

  public boolean isSuccess() {
    return metadata.isSucceeded();
  }

  public T getOutput() {
    return output;
  }

  public SynchronousJobMetadata getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SynchronousResponse<?> that = (SynchronousResponse<?>) o;
    return Objects.equals(output, that.output) && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(output, metadata);
  }

  @Override
  public String toString() {
    return "SynchronousResponse{"
        + "output=" + output
        + ", metadata=" + metadata
        + '}';
  }

}
