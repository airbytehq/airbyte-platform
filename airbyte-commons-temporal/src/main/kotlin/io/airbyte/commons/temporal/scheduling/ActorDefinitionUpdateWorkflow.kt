/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling

import io.airbyte.config.ActorType
import io.airbyte.config.FailureReason
import io.airbyte.config.ScopedResourceRequirements
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import java.util.UUID

data class SpecRequest(
  val shouldFetchSpec: Boolean,
  val dockerImage: String,
  val dockerImageTag: String,
  val workspaceId: UUID,
)

data class SpecMetadata(
  val name: String?,
  val icon: String?,
  val documentationUrl: String?,
  val resourceRequirements: ScopedResourceRequirements?,
)

data class ActorDefinitionUpdateInput(
  val requestId: String,
  val actorType: ActorType,
  val actorDefinitionId: UUID?,
  val specRequest: SpecRequest,
  val specMetadata: SpecMetadata,
)

data class ActorDefinitionUpdateOutput(
  val actorDefinitionId: UUID?,
  val commandId: String?,
  val failureReason: FailureReason?,
)

@WorkflowInterface
interface ActorDefinitionUpdateWorkflow {
  @WorkflowMethod
  fun run(input: ActorDefinitionUpdateInput): ActorDefinitionUpdateOutput
}
