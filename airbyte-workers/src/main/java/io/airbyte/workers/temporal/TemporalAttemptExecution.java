/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ERROR_ACTUAL_TYPE_KEY;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.SetWorkflowInAttemptRequestBody;
import io.airbyte.commons.logging.LoggingHelper;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.Worker;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This class represents a single run of a worker. It handles making sure the correct inputs and
 * outputs are passed to the selected worker. It also makes sures that the outputs of the worker are
 * persisted to the db.
 */
@SuppressWarnings({"PMD.UnusedFormalParameter", "PMD.AvoidCatchingThrowable"})
public class TemporalAttemptExecution<INPUT, OUTPUT> implements Supplier<OUTPUT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemporalAttemptExecution.class);

  private final JobRunConfig jobRunConfig;
  private final Path jobRoot;
  private final Worker<INPUT, OUTPUT> worker;
  private final INPUT input;
  private final Consumer<Path> mdcSetter;
  private final Supplier<String> workflowIdProvider;
  private final AirbyteApiClient airbyteApiClient;
  private final String airbyteVersion;
  private final Optional<String> replicationTaskQueue;

  public TemporalAttemptExecution(final Path workspaceRoot,
                                  final WorkerEnvironment workerEnvironment,
                                  final LogConfigs logConfigs,
                                  final JobRunConfig jobRunConfig,
                                  final Worker<INPUT, OUTPUT> worker,
                                  final INPUT input,
                                  final AirbyteApiClient airbyteApiClient,
                                  final String airbyteVersion,
                                  final Supplier<ActivityExecutionContext> activityContext) {
    this(
        workspaceRoot,
        jobRunConfig,
        worker,
        input,
        (path -> LogClientSingleton.getInstance().setJobMdc(workerEnvironment, logConfigs, path)),
        airbyteApiClient,
        () -> activityContext.get().getInfo().getWorkflowId(),
        airbyteVersion,
        Optional.empty());
  }

  public TemporalAttemptExecution(final Path workspaceRoot,
                                  final WorkerEnvironment workerEnvironment,
                                  final LogConfigs logConfigs,
                                  final JobRunConfig jobRunConfig,
                                  final Worker<INPUT, OUTPUT> worker,
                                  final INPUT input,
                                  final AirbyteApiClient airbyteApiClient,
                                  final String airbyteVersion,
                                  final Supplier<ActivityExecutionContext> activityContext,
                                  final Optional<String> replicationTaskQueue) {
    this(
        workspaceRoot,
        jobRunConfig,
        worker,
        input,
        (path -> LogClientSingleton.getInstance().setJobMdc(workerEnvironment, logConfigs, path)),
        airbyteApiClient,
        () -> activityContext.get().getInfo().getWorkflowId(),
        airbyteVersion,
        replicationTaskQueue);
  }

  @VisibleForTesting
  TemporalAttemptExecution(final Path workspaceRoot,
                           final JobRunConfig jobRunConfig,
                           final Worker<INPUT, OUTPUT> worker,
                           final INPUT input,
                           final Consumer<Path> mdcSetter,
                           final AirbyteApiClient airbyteApiClient,
                           final Supplier<String> workflowIdProvider,
                           final String airbyteVersion,
                           final Optional<String> replicationTaskQueue) {
    this.jobRunConfig = jobRunConfig;

    this.jobRoot = TemporalUtils.getJobRoot(workspaceRoot, jobRunConfig.getJobId(), jobRunConfig.getAttemptId());
    this.worker = worker;
    this.input = input;
    this.mdcSetter = mdcSetter;
    this.workflowIdProvider = workflowIdProvider;

    this.airbyteApiClient = airbyteApiClient;
    this.airbyteVersion = airbyteVersion;
    this.replicationTaskQueue = replicationTaskQueue;
  }

  @Override
  public OUTPUT get() {
    try {
      try (final var mdcScope = new MdcScope.Builder()
          .setLogPrefix(LoggingHelper.PLATFORM_LOGGER_PREFIX)
          .setPrefixColor(LoggingHelper.Color.CYAN_BACKGROUND)
          .build()) {

        mdcSetter.accept(jobRoot);

        if (MDC.get(LogClientSingleton.JOB_LOG_PATH_MDC_KEY) != null) {
          LOGGER.info("Docker volume job log path: " + MDC.get(LogClientSingleton.JOB_LOG_PATH_MDC_KEY));
        } else if (MDC.get(LogClientSingleton.CLOUD_JOB_LOG_PATH_MDC_KEY) != null) {
          LOGGER.info("Cloud storage job log path: " + MDC.get(LogClientSingleton.CLOUD_JOB_LOG_PATH_MDC_KEY));
        }

        LOGGER.info("Executing worker wrapper. Airbyte version: {}", airbyteVersion);
        AirbyteApiClient.retryWithJitter(() -> {
          saveWorkflowIdForCancellation(airbyteApiClient);
          return null;
        }, "save workflow id for cancellation");

        return worker.run(input, jobRoot);
      }

    } catch (final Exception e) {
      addActualRootCauseToTrace(e);
      throw Activity.wrap(e);
    } finally {
      mdcSetter.accept(null);
    }
  }

  private void addActualRootCauseToTrace(final Exception e) {
    Throwable inner = e;
    while (inner.getCause() != null) {
      inner = inner.getCause();
    }
    ApmTraceUtils.addTagsToTrace(Map.of(ERROR_ACTUAL_TYPE_KEY, e.getClass().getName()));
  }

  private void saveWorkflowIdForCancellation(final AirbyteApiClient airbyteApiClient) throws ApiException {
    // If the jobId is not a number, it means the job is a synchronous job. No attempt is created for
    // it, and it cannot be cancelled, so do not save the workflowId. See
    // SynchronousSchedulerClient.java for info.
    //
    // At this moment(Nov 2022), we decide to save workflowId for cancellation purpose only at
    // replication activity level. We know now the only async workflow is SyncWorkflow,
    // and under the same workflow, the workflowId would stay the same,
    // so it's not needed to save it for multiple times.
    if (NumberUtils.isCreatable(jobRunConfig.getJobId()) && replicationTaskQueue.isPresent()) {
      final String workflowId = workflowIdProvider.get();
      airbyteApiClient.getAttemptApi().setWorkflowInAttempt(new SetWorkflowInAttemptRequestBody()
          .jobId(Long.parseLong(jobRunConfig.getJobId()))
          .attemptNumber(jobRunConfig.getAttemptId().intValue())
          .processingTaskQueue(replicationTaskQueue.get())
          .workflowId(workflowId));
    }
  }

}
