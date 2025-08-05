/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testsyncworkflow

import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2Input
import io.airbyte.config.StandardSyncOutput

class EmptySyncWorkflow : SyncWorkflowV2 {
  override fun run(input: SyncWorkflowV2Input): StandardSyncOutput = StandardSyncOutput()
}
