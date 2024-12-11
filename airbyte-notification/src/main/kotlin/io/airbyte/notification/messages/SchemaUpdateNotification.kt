package io.airbyte.notification.messages

import io.airbyte.api.model.generated.CatalogDiff

data class SchemaUpdateNotification(
  val workspace: WorkspaceInfo,
  val connectionInfo: ConnectionInfo,
  val sourceInfo: SourceInfo,
  val isBreakingChange: Boolean,
  val catalogDiff: CatalogDiff,
)
