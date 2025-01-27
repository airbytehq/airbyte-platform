/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.model

import io.airbyte.workers.models.SpecInput

fun SpecInput.getJobId(): String = this.jobRunConfig.jobId

fun SpecInput.getAttemptId(): Long = this.jobRunConfig.attemptId

fun SpecInput.usesCustomConnector(): Boolean = this.launcherConfig.isCustomConnector
