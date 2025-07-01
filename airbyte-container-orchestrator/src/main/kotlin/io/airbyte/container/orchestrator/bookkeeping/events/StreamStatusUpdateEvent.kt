/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.events

import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.client.model.generated.StreamStatusRead
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusKey
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.api.client.model.generated.StreamStatusRunState as ApiEnum

data class StreamStatusUpdateEvent(
  // TODO: move cache to client proper when Docker uses Orchestrator
  val cache: MutableMap<StreamStatusKey, StreamStatusRead>,
  val key: StreamStatusKey,
  val runState: ApiEnum,
  val metadata: StreamStatusRateLimitedMetadata? = null,
  val ctx: ReplicationContext,
)
