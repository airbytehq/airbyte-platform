/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.input

import io.airbyte.config.ResourceRequirements
import io.airbyte.persistence.job.models.ReplicationInput

fun ReplicationInput.getJobId(): String = this.jobRunConfig.jobId

fun ReplicationInput.getAttemptId(): Long = this.jobRunConfig.attemptId

fun ReplicationInput.getOrchestratorResourceReqs(): ResourceRequirements? = this.syncResourceRequirements?.orchestrator

fun ReplicationInput.getSourceResourceReqs(): ResourceRequirements? = this.syncResourceRequirements?.source

fun ReplicationInput.getDestinationResourceReqs(): ResourceRequirements? = this.syncResourceRequirements?.destination

fun ReplicationInput.usesCustomConnector(): Boolean = this.sourceLauncherConfig.isCustomConnector || this.destinationLauncherConfig.isCustomConnector
