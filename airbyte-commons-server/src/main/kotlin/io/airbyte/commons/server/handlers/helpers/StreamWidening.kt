/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.server.generated.models.AirbyteStream
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.SyncMode
import io.airbyte.commons.protocol.CatalogDiffHelpers
import io.airbyte.commons.protocol.ColumnTypeWidening
import io.airbyte.config.DestinationSyncMode
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.SourceDefinition
import java.util.UUID

/**
 * The context [io.airbyte.featureflag.WidenColumnTypes] is read with, so the rollout can target organizations,
 * source connectors, or both.
 */
fun widenColumnTypesContext(
  organizationId: UUID,
  sourceDefinitionId: UUID,
): Context = Multi(listOf(Organization(organizationId), SourceDefinition(sourceDefinitionId)))

/**
 * The schema to save when [discovered] replaces [saved]: the discovered schema, with each changed column kept at a
 * supertype by [ColumnTypeWidening].
 */
internal fun widenStreamSchema(
  saved: AirbyteStream,
  discovered: AirbyteStream,
  config: AirbyteStreamConfiguration,
): ColumnTypeWidening.MergeResult = ColumnTypeWidening.mergeStreamSchema(saved.jsonSchema!!, discovered.jsonSchema!!, keyColumns(config))

// The same columns CatalogDiffHelpers.fieldTransformBreaksConnection treats as breaking. Only top-level columns are
// widened, so a nested key path counts as its top-level column.
private fun keyColumns(config: AirbyteStreamConfiguration): Set<String> {
  val cursor = if (config.syncMode == SyncMode.INCREMENTAL) config.cursorField.orEmpty().take(1) else emptyList()
  val primaryKey =
    if (CatalogDiffHelpers.isDedup(DestinationSyncMode.valueOf(config.destinationSyncMode.name))) {
      config.primaryKey.orEmpty().mapNotNull { it.firstOrNull() }
    } else {
      emptyList()
    }
  return (cursor + primaryKey).toSet()
}
