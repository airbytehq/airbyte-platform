/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.hydration

import java.util.UUID

data class SecretHydrationContext(
  val organizationId: UUID,
  val workspaceId: UUID,
)
