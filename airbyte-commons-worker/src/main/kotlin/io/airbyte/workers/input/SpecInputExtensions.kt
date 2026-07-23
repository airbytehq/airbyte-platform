/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.input

import io.airbyte.workers.models.SpecInput

fun SpecInput.getJobId(): String = this.jobRunConfig.jobId

fun SpecInput.getAttemptId(): Long = this.jobRunConfig.attemptId

fun SpecInput.usesCustomConnector(): Boolean = this.launcherConfig.isCustomConnector
