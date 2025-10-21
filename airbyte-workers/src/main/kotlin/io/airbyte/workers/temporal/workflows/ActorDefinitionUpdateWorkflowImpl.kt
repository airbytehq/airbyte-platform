/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.workflows

import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.commons.temporal.scheduling.ActorDefinitionUpdateInput
import io.airbyte.commons.temporal.scheduling.ActorDefinitionUpdateOutput
import io.airbyte.commons.temporal.scheduling.ActorDefinitionUpdateWorkflow
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.SpecCommandApiInput
import io.airbyte.workers.temporal.activities.ActorDefinitionUpdateActivity
import io.airbyte.workers.temporal.activities.FinishInput
import io.temporal.api.enums.v1.ParentClosePolicy
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.workflow.Workflow

open class ActorDefinitionUpdateWorkflowImpl : ActorDefinitionUpdateWorkflow {
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private lateinit var actorDefinitionUpdateActivity: ActorDefinitionUpdateActivity

  override fun run(input: ActorDefinitionUpdateInput): ActorDefinitionUpdateOutput {
    var commandId: String? = null
    if (input.specRequest.shouldFetchSpec) {
      commandId = "spec_${input.requestId}"
      val specInput =
        SpecCommandApiInput.SpecApiInput(
          requestId = input.requestId,
          commandId = commandId,
          actorDefinitionId = null,
          dockerImage = input.specRequest.dockerImage,
          dockerImageTag = input.specRequest.dockerImageTag,
          workspaceId = input.specRequest.workspaceId,
        )
      val specWorkflow =
        Workflow.newChildWorkflowStub(
          ConnectorCommandWorkflow::class.java,
          ChildWorkflowOptions
            .newBuilder()
            .setWorkflowId(commandId)
            .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
            .build(),
        )
      specWorkflow.run(SpecCommandApiInput(specInput))
    }

    val output =
      actorDefinitionUpdateActivity.finish(
        FinishInput(
          requestId = input.requestId,
          commandId = commandId,
          actorType = input.actorType,
          actorDefinitionId = input.actorDefinitionId,
          specMetadata = input.specMetadata,
          specRequest = input.specRequest,
        ),
      )

    return ActorDefinitionUpdateOutput(
      actorDefinitionId = output.actorDefinitionId,
      commandId = commandId,
      failureReason = output.failureReason,
    )
  }
}
