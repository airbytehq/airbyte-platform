/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter;

import io.airbyte.config.ReleaseStage;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Connector Job Reporting context.
 *
 * @param jobId job id
 * @param dockerImage docker image
 * @param releaseStage connector release stage - can be null in the case of Spec jobs, since they
 *        can run on arbitrary images.
 */
public record ConnectorJobReportingContext(UUID jobId, String dockerImage, @Nullable ReleaseStage releaseStage) {}
