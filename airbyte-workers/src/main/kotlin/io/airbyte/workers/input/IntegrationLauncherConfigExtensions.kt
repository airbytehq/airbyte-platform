/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.input

import io.airbyte.commons.constants.WorkerConstants.RESET_JOB_SOURCE_DOCKER_IMAGE_STUB
import io.airbyte.persistence.job.models.IntegrationLauncherConfig

fun IntegrationLauncherConfig.isReset(): Boolean = this.dockerImage == RESET_JOB_SOURCE_DOCKER_IMAGE_STUB
