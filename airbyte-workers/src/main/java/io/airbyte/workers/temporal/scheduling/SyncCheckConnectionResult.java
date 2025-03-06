/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling;

import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.FailureReason;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.helper.FailureHelper;
import io.airbyte.workers.temporal.sync.SyncOutputProvider;
import java.lang.invoke.MethodHandles;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SyncCheckConnectionFailure.
 */
public class SyncCheckConnectionResult {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Long jobId;
  private final Integer attemptId;
  private ConnectorJobOutput failureOutput;
  private FailureReason.FailureOrigin origin = null;

  public SyncCheckConnectionResult(final JobRunConfig jobRunConfig) {
    Long jobId = 0L;
    Integer attemptId = 0;

    try {
      jobId = Long.valueOf(jobRunConfig.getJobId());
      attemptId = Math.toIntExact(jobRunConfig.getAttemptId());
    } catch (final Exception e) {
      // In tests, the jobId and attemptId may not be available
      log.warn("Cannot determine jobId or attemptId: " + e.getMessage());
    }

    this.jobId = jobId;
    this.attemptId = attemptId;
  }

  public boolean isFailed() {
    return this.origin != null && this.failureOutput != null;
  }

  public void setFailureOrigin(final FailureReason.FailureOrigin origin) {
    this.origin = origin;
  }

  public void setFailureOutput(final ConnectorJobOutput failureOutput) {
    this.failureOutput = failureOutput;
  }

  /**
   * Build failure out.
   *
   * @return sync output
   */
  public StandardSyncOutput buildFailureOutput() {
    if (!this.isFailed()) {
      throw new RuntimeException("Cannot build failure output without a failure origin and output");
    }

    final StandardSyncOutput syncOutput = new StandardSyncOutput()
        .withStandardSyncSummary(SyncOutputProvider.EMPTY_FAILED_SYNC);

    if (failureOutput.getFailureReason() != null) {
      syncOutput.setFailures(List.of(failureOutput.getFailureReason().withFailureOrigin(origin)));
    } else {
      final StandardCheckConnectionOutput checkOutput = failureOutput.getCheckConnection();
      final Exception ex = new IllegalArgumentException(checkOutput.getMessage());
      final FailureReason checkFailureReason = FailureHelper.checkFailure(ex, jobId, attemptId, origin);
      syncOutput.setFailures(List.of(checkFailureReason));
    }

    return syncOutput;
  }

  /**
   * Test if output failed.
   *
   * @param output output
   * @return true, if failed. otherwise, false.
   */
  public static boolean isOutputFailed(final ConnectorJobOutput output) {
    if (output.getOutputType() != OutputType.CHECK_CONNECTION) {
      throw new IllegalArgumentException("Output type must be CHECK_CONNECTION");
    }

    return output.getFailureReason() != null || output.getCheckConnection().getStatus() == StandardCheckConnectionOutput.Status.FAILED;
  }

}
