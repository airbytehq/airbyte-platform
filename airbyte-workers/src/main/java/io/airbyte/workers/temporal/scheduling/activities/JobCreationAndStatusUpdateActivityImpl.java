/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionJobRequestBody;
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberRequest;
import io.airbyte.api.client.model.generated.FailAttemptRequest;
import io.airbyte.api.client.model.generated.JobCreate;
import io.airbyte.api.client.model.generated.JobFailureRequest;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobSuccessWithAttemptNumberRequest;
import io.airbyte.api.client.model.generated.PersistCancelJobRequestBody;
import io.airbyte.api.client.model.generated.ReportJobStartRequest;
import io.airbyte.commons.micronaut.EnvConstants;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.State;
import io.airbyte.featureflag.AlwaysRunCheckBeforeSync;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.workers.context.AttemptContext;
import io.airbyte.workers.storage.activities.OutputStorageClient;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.UUID;
import org.openapitools.client.infrastructure.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobCreationAndStatusUpdateActivityImpl.
 */
@Singleton
@Requires(env = EnvConstants.CONTROL_PLANE)
public class JobCreationAndStatusUpdateActivityImpl implements JobCreationAndStatusUpdateActivity {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AirbyteApiClient airbyteApiClient;
  private final FeatureFlagClient featureFlagClient;
  private final OutputStorageClient<State> stateClient;
  private final OutputStorageClient<ConfiguredAirbyteCatalog> catalogClient;

  public JobCreationAndStatusUpdateActivityImpl(final AirbyteApiClient airbyteApiClient,
                                                final FeatureFlagClient featureFlagClient,
                                                @Named("outputStateClient") final OutputStorageClient<State> stateClient,
                                                @Named("outputCatalogClient") final OutputStorageClient<ConfiguredAirbyteCatalog> catalogClient) {
    this.airbyteApiClient = airbyteApiClient;
    this.featureFlagClient = featureFlagClient;
    this.stateClient = stateClient;
    this.catalogClient = catalogClient;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public JobCreationOutput createNewJob(final JobCreationInput input) {
    new AttemptContext(input.getConnectionId(), null, null).addTagsToTrace();
    try {
      final JobInfoRead jobInfoRead = airbyteApiClient.getJobsApi().createJob(new JobCreate(input.getConnectionId()));
      return new JobCreationOutput(jobInfoRead.getJob().getId());
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      log.error("Unable to create job for connection {}", input.getConnectionId(), e);
      throw new RetryableException(e);
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      log.error("Unable to create job for connection {}", input.getConnectionId(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public AttemptNumberCreationOutput createNewAttemptNumber(final AttemptCreationInput input) throws RetryableException {
    new AttemptContext(null, input.getJobId(), null).addTagsToTrace();

    try {
      final long jobId = input.getJobId();
      final var response = airbyteApiClient.getAttemptApi().createNewAttemptNumber(new CreateNewAttemptNumberRequest(jobId));
      return new AttemptNumberCreationOutput(response.getAttemptNumber());
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      log.error("createNewAttemptNumber for job {} failed with exception: {}", input.getJobId(), e.getMessage(), e);
      throw new RetryableException(e);
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      log.error("createNewAttemptNumber for job {} failed with exception: {}", input.getJobId(), e.getMessage(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void jobSuccessWithAttemptNumber(final JobSuccessInputWithAttemptNumber input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

    final var output = input.getStandardSyncOutput();

    try {
      final var request = new JobSuccessWithAttemptNumberRequest(
          input.getJobId(),
          input.getAttemptNumber(),
          input.getConnectionId(),
          output);
      airbyteApiClient.getJobsApi().jobSuccessWithAttemptNumber(request);
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      ApmTraceUtils.addExceptionToTrace(e);
      log.error("jobSuccessWithAttemptNumber for job {} failed with exception: {}", input.getJobId(), e.getMessage(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void jobFailure(final JobFailureInput input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

    try {
      final var request = new JobFailureRequest(
          input.getJobId(),
          input.getAttemptNumber(),
          input.getConnectionId(),
          input.getReason());
      airbyteApiClient.getJobsApi().jobFailure(request);
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      log.error("jobFailure for job {} attempt {} failed with exception: {}", input.getJobId(), input.getAttemptNumber(), e.getMessage(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void attemptFailureWithAttemptNumber(final AttemptNumberFailureInput input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

    final var output = input.getStandardSyncOutput();

    try {
      final var req = new FailAttemptRequest(
          input.getJobId(),
          input.getAttemptNumber(),
          input.getAttemptFailureSummary(),
          output);

      airbyteApiClient.getAttemptApi().failAttempt(req);
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      log.error("attemptFailureWithAttemptNumber for job {} failed with exception: {}", input.getJobId(), e.getMessage(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void jobCancelledWithAttemptNumber(final JobCancelledInputWithAttemptNumber input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

    try {
      final var req = new PersistCancelJobRequestBody(
          input.getAttemptFailureSummary(),
          input.getAttemptNumber(),
          input.getConnectionId(),
          input.getJobId());

      airbyteApiClient.getJobsApi().persistJobCancellation(req);
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void reportJobStart(final ReportJobStartInput input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), null).addTagsToTrace();

    try {
      airbyteApiClient.getJobsApi().reportJobStart(new ReportJobStartRequest(input.getJobId(), input.getConnectionId()));
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void ensureCleanJobState(final EnsureCleanJobStateInput input) {
    new AttemptContext(input.getConnectionId(), null, null).addTagsToTrace();
    try {
      airbyteApiClient.getJobsApi().failNonTerminalJobs(new ConnectionIdRequestBody(input.getConnectionId()));
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

  /**
   * This method is used to determine if the current job is the last job or attempt failure.
   *
   * @param input - JobCheckFailureInput.
   * @return - boolean.
   */
  @Override
  public boolean isLastJobOrAttemptFailure(final JobCheckFailureInput input) {
    // This is a hack to enforce check operation before every sync. Please be mindful of this logic.
    // This is mainly for testing and to force our canary connections to always run CHECK
    if (shouldAlwaysRunCheckBeforeSync(input.getConnectionId())) {
      return true;
    }
    // If there has been a previous attempt, that means it failed. We don't create subsequent attempts
    // on success.
    final var isNotFirstAttempt = input.getAttemptId() > 0;
    if (isNotFirstAttempt) {
      return true;
    }

    try {
      final var didSucceed = airbyteApiClient.getJobsApi().didPreviousJobSucceed(
          new ConnectionJobRequestBody(input.getConnectionId(), input.getJobId()))
          .getValue();
      // Treat anything other than an explicit success as a failure.
      return !didSucceed;
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

  private boolean shouldAlwaysRunCheckBeforeSync(UUID connectionId) {
    try {
      return featureFlagClient.boolVariation(AlwaysRunCheckBeforeSync.INSTANCE,
          new Connection(connectionId));
    } catch (final Exception e) {
      return false;
    }
  }

}
