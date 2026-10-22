/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.messages

import io.airbyte.api.server.generated.models.CatalogDiff

data class SchemaUpdateNotification(
  val workspace: WorkspaceInfo,
  val connectionInfo: ConnectionInfo,
  val sourceInfo: SourceInfo,
  val isBreakingChange: Boolean,
  val catalogDiff: CatalogDiff,
)
