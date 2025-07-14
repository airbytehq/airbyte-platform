/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.UUID;

/**
 * Sync Job Reporting Context.
 *
 * @param jobId job id.
 * @param sourceVersionId source definition version id. Can be null in the case of resets.
 * @param destinationVersionId destination definition version id.
 */
public record SyncJobReportingContext(long jobId, @Nullable UUID sourceVersionId, UUID destinationVersionId) {

}
