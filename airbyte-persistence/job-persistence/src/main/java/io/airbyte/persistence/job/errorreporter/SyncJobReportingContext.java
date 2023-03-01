/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter;

/**
 * Sync Job Reporting Context.
 *
 * @param jobId job id
 * @param sourceDockerImage source docker image
 * @param destinationDockerImage destination docker image
 */
public record SyncJobReportingContext(long jobId, String sourceDockerImage, String destinationDockerImage) {

}
