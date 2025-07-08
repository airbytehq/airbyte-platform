/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared;

import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobStatus;
import java.time.OffsetDateTime;
import java.util.Map;

/**
 * A pair of a destination connection and its associated connection count.
 *
 * @param destination Destination connection.
 * @param connectionCount Number of non-deprecated connections using this destination.
 * @param lastSync Timestamp of the most recent sync for any connection using this destination.
 * @param connectionJobStatuses Map of most recent job status to count of connections with that job
 *        status.
 */
public record DestinationConnectionWithCount(
                                             DestinationConnection destination,
                                             int connectionCount,
                                             OffsetDateTime lastSync,
                                             Map<JobStatus, Integer> connectionJobStatuses) {

}
