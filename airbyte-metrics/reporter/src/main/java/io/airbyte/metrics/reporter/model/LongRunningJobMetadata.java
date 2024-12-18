/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter.model;

public record LongRunningJobMetadata(
                                     String sourceDockerImage,
                                     String destinationDockerImage,
                                     String connectionId) {}
