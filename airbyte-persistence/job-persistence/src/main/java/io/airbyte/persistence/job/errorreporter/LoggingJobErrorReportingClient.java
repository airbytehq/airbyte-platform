/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter;

import edu.umd.cs.findbugs.annotations.Nullable;
import io.airbyte.config.FailureReason;
import io.airbyte.config.StandardWorkspace;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log job error reports.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class LoggingJobErrorReportingClient implements JobErrorReportingClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingJobErrorReportingClient.class);

  @Override
  public void reportJobFailureReason(@Nullable final StandardWorkspace workspace,
                                     final FailureReason reason,
                                     final String dockerImage,
                                     final Map<String, String> metadata,
                                     @Nullable final AttemptConfigReportingContext attemptConfig) {
    LOGGER.info(
        "Report Job Error -> workspaceId: {}, dockerImage: {}, failureReason: {}, metadata: {}, state: {}, sourceConfig: {}, destinationConfig: {}",
        workspace != null ? workspace.getWorkspaceId() : "null",
        dockerImage,
        reason,
        metadata,
        attemptConfig != null ? attemptConfig.state() : "null",
        attemptConfig != null ? attemptConfig.sourceConfig() : "null",
        attemptConfig != null ? attemptConfig.destinationConfig() : "null");
  }

}
