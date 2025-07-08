/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared;

import io.airbyte.config.JobStatus;
import io.airbyte.config.SourceConnection;
import java.time.OffsetDateTime;
import java.util.Map;

/**
 * A pair of a source connection and its associated connection count.
 *
 * @param source Source connection.
 * @param connectionCount Number of non-deprecated connections using this source.
 * @param lastSync Timestamp of the most recent sync for any connection using this source.
 * @param connectionJobStatuses Map of most recent job status to count of connections with that job
 *        status.
 */
public record SourceConnectionWithCount(
                                        SourceConnection source,
                                        int connectionCount,
                                        OffsetDateTime lastSync,
                                        Map<JobStatus, Integer> connectionJobStatuses) {

}
