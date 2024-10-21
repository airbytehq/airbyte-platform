/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.logging.LogSource;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.general.ReplicationWorker;
import io.temporal.activity.Activity;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a single run of a worker. It handles making sure the correct inputs and
 * outputs are passed to the selected worker. It also makes sures that the outputs of the worker are
 * persisted to the db.
 */
@SuppressWarnings({"PMD.UnusedFormalParameter", "PMD.AvoidCatchingThrowable", "PMD.UnusedLocalVariable"})
public class TemporalAttemptExecution implements Supplier<ReplicationOutput> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemporalAttemptExecution.class);

  private final Path jobRoot;
  private final ReplicationWorker worker;
  private final ReplicationInput input;
  private final Consumer<Path> mdcSetter;
  private final String airbyteVersion;
  private final LogClientManager logClientManager;

  public TemporalAttemptExecution(final Path workspaceRoot,
                                  final JobRunConfig jobRunConfig,
                                  final ReplicationWorker worker,
                                  final ReplicationInput input,
                                  final String airbyteVersion,
                                  final LogClientManager logClientManager) {
    this(
        workspaceRoot,
        jobRunConfig,
        worker,
        input,
        (logClientManager::setJobMdc),
        airbyteVersion,
        logClientManager);
  }

  @VisibleForTesting
  TemporalAttemptExecution(final Path workspaceRoot,
                           final JobRunConfig jobRunConfig,
                           final ReplicationWorker worker,
                           final ReplicationInput input,
                           final Consumer<Path> mdcSetter,
                           final String airbyteVersion,
                           final LogClientManager logClientManager) {
    this.jobRoot = TemporalUtils.getJobRoot(workspaceRoot, jobRunConfig.getJobId(), jobRunConfig.getAttemptId());
    this.worker = worker;
    this.input = input;
    this.mdcSetter = mdcSetter;

    this.airbyteVersion = airbyteVersion;
    this.logClientManager = logClientManager;
  }

  @Override
  public ReplicationOutput get() {
    try {
      try (final var mdcScope = new MdcScope.Builder().setExtraMdcEntries(LogSource.PLATFORM.toMdc()).build()) {

        mdcSetter.accept(jobRoot);

        LOGGER.info("Using job log path: {}", logClientManager.fullLogPath(jobRoot));
        LOGGER.info("Executing worker wrapper. Airbyte version: {}", airbyteVersion);
        return worker.run(input, jobRoot);
      }

    } catch (final Exception e) {
      ApmTraceUtils.addActualRootCauseToTrace(e);
      throw Activity.wrap(e);
    } finally {
      mdcSetter.accept(null);
    }
  }

}
