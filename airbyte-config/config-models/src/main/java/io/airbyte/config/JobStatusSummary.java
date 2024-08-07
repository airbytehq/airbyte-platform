/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import java.util.UUID;

public record JobStatusSummary(UUID connectionId, Long createdAt, JobStatus status) {

}
