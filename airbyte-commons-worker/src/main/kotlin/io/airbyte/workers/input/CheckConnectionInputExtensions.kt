/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.input

import io.airbyte.config.ActorType
import io.airbyte.workers.models.CheckConnectionInput
import java.util.UUID

fun CheckConnectionInput.getJobId(): String = this.jobRunConfig.jobId

fun CheckConnectionInput.getAttemptId(): Long = this.jobRunConfig.attemptId

fun CheckConnectionInput.getActorType(): ActorType = this.checkConnectionInput.actorType

fun CheckConnectionInput.getOrganizationId(): UUID = this.checkConnectionInput.actorContext.organizationId
