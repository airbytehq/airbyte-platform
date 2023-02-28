/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter;

import java.util.UUID;

/**
 * Connector Job Reporting context.
 *
 * @param jobId job id
 * @param dockerImage docker image
 */
public record ConnectorJobReportingContext(UUID jobId, String dockerImage) {}
