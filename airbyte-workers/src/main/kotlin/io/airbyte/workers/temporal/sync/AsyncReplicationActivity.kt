/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.config.StandardSyncOutput
import io.airbyte.workers.models.ReplicationActivityInput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

/**
 * ReplicationActivity.
 */
@ActivityInterface
interface AsyncReplicationActivity {
  @ActivityMethod
  fun startReplication(replicationInput: ReplicationActivityInput): String

  @ActivityMethod
  fun getReplicationOutput(
    replicationInput: ReplicationActivityInput,
    workloadId: String,
  ): StandardSyncOutput

  fun cancel(
    replicationInput: ReplicationActivityInput,
    workloadId: String,
  )
}
