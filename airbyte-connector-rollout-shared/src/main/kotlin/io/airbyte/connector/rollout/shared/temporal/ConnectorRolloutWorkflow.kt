/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPause
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputStart
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutWorkflowInput
import io.temporal.workflow.UpdateMethod
import io.temporal.workflow.UpdateValidatorMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

@WorkflowInterface
interface ConnectorRolloutWorkflow {
  @WorkflowMethod
  fun run(input: ConnectorRolloutWorkflowInput): ConnectorEnumRolloutState

  @UpdateMethod
  fun startRollout(input: ConnectorRolloutActivityInputStart): ConnectorRolloutOutput

  @UpdateValidatorMethod(updateName = "startRollout")
  fun startRolloutValidator(input: ConnectorRolloutActivityInputStart)

  @UpdateMethod
  fun findRollout(input: ConnectorRolloutActivityInputFind): List<ConnectorRolloutOutput>

  @UpdateValidatorMethod(updateName = "findRollout")
  fun findRolloutValidator(input: ConnectorRolloutActivityInputFind)

  @UpdateMethod
  fun getRollout(input: ConnectorRolloutActivityInputGet): ConnectorRolloutOutput

  @UpdateValidatorMethod(updateName = "getRollout")
  fun getRolloutValidator(input: ConnectorRolloutActivityInputGet)

  @UpdateMethod
  fun pauseRollout(input: ConnectorRolloutActivityInputPause): ConnectorRolloutOutput

  @UpdateValidatorMethod(updateName = "pauseRollout")
  fun pauseRolloutValidator(input: ConnectorRolloutActivityInputPause)

  @UpdateMethod
  fun progressRollout(input: ConnectorRolloutActivityInputRollout): ConnectorRolloutOutput

  @UpdateValidatorMethod(updateName = "progressRollout")
  fun progressRolloutValidator(input: ConnectorRolloutActivityInputRollout)

  @UpdateMethod
  fun finalizeRollout(input: ConnectorRolloutActivityInputFinalize): ConnectorRolloutOutput

  @UpdateValidatorMethod(updateName = "finalizeRollout")
  fun finalizeRolloutValidator(input: ConnectorRolloutActivityInputFinalize)
}
