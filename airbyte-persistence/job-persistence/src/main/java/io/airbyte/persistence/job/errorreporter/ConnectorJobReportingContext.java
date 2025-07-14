/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter;

import io.airbyte.config.ReleaseStage;
import jakarta.annotation.Nullable;
import java.util.UUID;

/**
 * Connector Job Reporting context.
 *
 * @param jobId job id
 * @param dockerImage docker image
 * @param releaseStage connector release stage - can be null in the case of Spec jobs, since they
 *        can run on arbitrary images.
 */
public record ConnectorJobReportingContext(UUID jobId,
                                           String dockerImage,
                                           @Nullable ReleaseStage releaseStage,
                                           @Nullable Long internalSupportLevel) {}
