/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ERROR_ACTUAL_TYPE_KEY;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.SetWorkflowInAttemptRequestBody;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.logging.LoggingHelper;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.Worker;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a single run of a worker. It handles making sure the correct inputs and
 * outputs are passed to the selected worker. It also makes sures that the outputs of the worker are
 * persisted to the db.
 */
@SuppressWarnings({"PMD.UnusedFormalParameter", "PMD.AvoidCatchingThrowable", "PMD.UnusedLocalVariable"})
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
  private final LogClientManager logClientManager;

  public TemporalAttemptExecution(final Path workspaceRoot,
                                  final JobRunConfig jobRunConfig,
                                  final Worker<INPUT, OUTPUT> worker,
                                  final INPUT input,
                                  final AirbyteApiClient airbyteApiClient,
                                  final String airbyteVersion,
                                  final Supplier<ActivityExecutionContext> activityContext,
                                  final LogClientManager logClientManager) {
    this(
        workspaceRoot,
        jobRunConfig,
        worker,
        input,
        (logClientManager::setJobMdc),
        airbyteApiClient,
        () -> activityContext.get().getInfo().getWorkflowId(),
        airbyteVersion,
        Optional.empty(),
        logClientManager);
  }

  public TemporalAttemptExecution(final Path workspaceRoot,
                                  final JobRunConfig jobRunConfig,
                                  final Worker<INPUT, OUTPUT> worker,
                                  final INPUT input,
                                  final AirbyteApiClient airbyteApiClient,
                                  final String airbyteVersion,
                                  final Supplier<ActivityExecutionContext> activityContext,
                                  final Optional<String> replicationTaskQueue,
                                  final LogClientManager logClientManager) {
    this(
        workspaceRoot,
        jobRunConfig,
        worker,
        input,
        (logClientManager::setJobMdc),
        airbyteApiClient,
        () -> activityContext.get().getInfo().getWorkflowId(),
        airbyteVersion,
        replicationTaskQueue,
        logClientManager);
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
                           final Optional<String> replicationTaskQueue,
                           final LogClientManager logClientManager) {
    this.jobRunConfig = jobRunConfig;

    this.jobRoot = TemporalUtils.getJobRoot(workspaceRoot, jobRunConfig.getJobId(), jobRunConfig.getAttemptId());
    this.worker = worker;
    this.input = input;
    this.mdcSetter = mdcSetter;
    this.workflowIdProvider = workflowIdProvider;

    this.airbyteApiClient = airbyteApiClient;
    this.airbyteVersion = airbyteVersion;
    this.replicationTaskQueue = replicationTaskQueue;
    this.logClientManager = logClientManager;
  }

  @Override
  public OUTPUT get() {
    try {
      try (final var mdcScope = new MdcScope.Builder()
          .setLogPrefix(LoggingHelper.PLATFORM_LOGGER_PREFIX)
          .setPrefixColor(LoggingHelper.Color.CYAN_BACKGROUND)
          .build()) {

        mdcSetter.accept(jobRoot);

        LOGGER.info("Using job log path: {}", logClientManager.fullLogPath(jobRoot));
        LOGGER.info("Executing worker wrapper. Airbyte version: {}", airbyteVersion);
        saveWorkflowIdForCancellation(airbyteApiClient);
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

  private void saveWorkflowIdForCancellation(final AirbyteApiClient airbyteApiClient) throws IOException {
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
      airbyteApiClient.getAttemptApi().setWorkflowInAttempt(new SetWorkflowInAttemptRequestBody(
          Long.parseLong(jobRunConfig.getJobId()),
          jobRunConfig.getAttemptId().intValue(),
          workflowId,
          replicationTaskQueue.get()));
    }
  }

}
