/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.buffered.worker

import io.airbyte.workers.general.BufferConfiguration
import io.airbyte.workers.helper.StreamStatusCompletionTracker

data class ReplicationWorkerContext(
  val jobId: String,
  val attempt: Int,
  val bufferConfiguration: BufferConfiguration,
  val replicationWorkerHelper: ReplicationWorkerHelperK,
  val replicationWorkerState: ReplicationWorkerState,
  val streamStatusCompletionTracker: StreamStatusCompletionTracker,
)
