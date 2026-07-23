/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.DiffCatalogsRequest
import io.airbyte.api.model.generated.DiffCatalogsResponse
import io.airbyte.api.model.generated.SchemaChange
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.protocol.CatalogDiffHelpers.getCatalogDiff
import io.airbyte.commons.server.converters.CatalogDiffConverters.streamTransformToApi
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.handlers.helpers.CatalogMergeHelper
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.StandardSync
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.protocol.models.v0.AirbyteCatalog as ProtocolAirbyteCatalog

@Singleton
class CatalogDiffService(
  private val catalogService: CatalogService,
  private val connectionService: ConnectionService,
  private val applySchemaChangeHelper: ApplySchemaChangeHelper,
  private val catalogConverter: CatalogConverter,
  private val catalogMergeHelper: CatalogMergeHelper,
  private val metricClient: MetricClient,
) {
  fun diffCatalogs(request: DiffCatalogsRequest): DiffCatalogsResponse {
    val currentCatalogId = request.currentCatalogId
    val newCatalogId = request.newCatalogId
    val connectionId = request.connectionId

    // If connection ID is provided, use it to get configured catalog
    // Otherwise, retrieve catalogs without connection context
    val (currentCatalog, newCatalog, configuredCatalog) =
      if (connectionId != null) {
        // Get connection to retrieve configured catalog
        val connection = connectionService.getStandardSync(connectionId)

        // Retrieve and convert catalogs
        Triple(
          retrieveDiscoveredCatalog(currentCatalogId),
          retrieveDiscoveredCatalog(newCatalogId),
          connection.catalog,
        )
      } else {
        // Retrieve catalogs without connection context
        Triple(
          retrieveDiscoveredCatalog(currentCatalogId),
          retrieveDiscoveredCatalog(newCatalogId),
          null,
        )
      }

    // Compute the diff
    val catalogDiff = computeDiff(currentCatalog, newCatalog, configuredCatalog)

    // Determine schema change classification
    val schemaChange: SchemaChange =
      if (catalogDiff.transforms.isEmpty()) {
        SchemaChange.NO_CHANGE
      } else if (configuredCatalog != null && applySchemaChangeHelper.containsBreakingChange(catalogDiff)) {
        SchemaChange.BREAKING
      } else {
        SchemaChange.NON_BREAKING
      }

    // Compute merged catalog if connection ID is provided
    val mergedCatalog: AirbyteCatalog? =
      if (connectionId != null && configuredCatalog != null) {
        // Convert catalogs to API format
        // originalConfigured: the configured catalog with user selections (converted from internal model)
        val originalConfiguredApiCatalog = catalogConverter.toApi(configuredCatalog, null)
        // originalDiscovered: the current/old discovered catalog (from protocol)
        val originalDiscoveredApiCatalog = catalogConverter.toApi(currentCatalog, null)
        // discovered: the new discovered catalog (from protocol)
        val newDiscoveredApiCatalog = catalogConverter.toApi(newCatalog, null)

        // Merge the new catalog with the connection's configured catalog
        catalogMergeHelper.mergeCatalogWithConfiguration(originalConfiguredApiCatalog, originalDiscoveredApiCatalog, newDiscoveredApiCatalog)
      } else {
        null
      }

    // Persist changes to connection if requested
    if (request.persistChanges == true && connectionId != null) {
      persistCatalogChanges(connectionId, newCatalogId, schemaChange, catalogDiff, configuredCatalog)
    }

    return DiffCatalogsResponse()
      .catalogDiff(catalogDiff)
      .schemaChange(schemaChange)
      .mergedCatalog(mergedCatalog)
  }

  /**
   * Computes the diff between two catalogs using a configured catalog for context.
   * This is the core diff logic that can be reused by different callers.
   * Uses protocol models to avoid unnecessary API conversions.
   */
  fun computeDiff(
    currentCatalog: ProtocolAirbyteCatalog,
    newCatalog: ProtocolAirbyteCatalog,
    configuredCatalog: ConfiguredAirbyteCatalog?,
  ): CatalogDiff {
    val streamTransforms =
      getCatalogDiff(
        currentCatalog,
        newCatalog,
        configuredCatalog ?: ConfiguredAirbyteCatalog().withStreams(emptyList()),
      )

    return CatalogDiff().transforms(
      streamTransforms
        .stream()
        .map { streamTransformToApi(it) }
        .toList(),
    )
  }

  private fun retrieveDiscoveredCatalog(catalogId: UUID): ProtocolAirbyteCatalog {
    val catalog = catalogService.getActorCatalogById(catalogId)
    return Jsons.`object`(
      catalog.catalog,
      ProtocolAirbyteCatalog::class.java,
    )
  }

  /**
   * Persists catalog changes to the connection database record.
   * Updates:
   * - breakingChange: boolean flag indicating if there are breaking changes
   * - status: set to INACTIVE if breaking change detected OR if nonBreakingChangesPreference is DISABLE and there are any changes
   *
   * Emits metrics for schema change detection.
   *
   * Note: Does NOT update sourceCatalogId - that should be updated separately in the caller's workflow.
   * This aligns with disableConnectionIfNeeded behavior.
   */
  private fun persistCatalogChanges(
    connectionId: UUID,
    newCatalogId: UUID,
    schemaChange: SchemaChange,
    catalogDiff: CatalogDiff,
    originalConfiguredCatalog: ConfiguredAirbyteCatalog?,
  ) {
    // Get the current connection
    val connection =
      connectionService.getStandardSync(connectionId)
        ?: throw IllegalArgumentException("Connection not found: $connectionId")

    // Determine if there's a breaking change
    val hasBreakingChange = schemaChange == SchemaChange.BREAKING

    // Emit metrics for schema change detection
    if (hasBreakingChange) {
      metricClient.count(
        OssMetricsRegistry.BREAKING_SCHEMA_CHANGE_DETECTED,
        1L,
        MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
      )
    } else if (schemaChange == SchemaChange.NON_BREAKING) {
      metricClient.count(
        OssMetricsRegistry.NON_BREAKING_SCHEMA_CHANGE_DETECTED,
        1L,
        MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
      )
    }

    // Update connection fields
    connection.breakingChange = hasBreakingChange

    // Determine if connection should be disabled
    // 1. Disable if there are breaking changes
    // 2. Disable if there are non-breaking changes but the connection is configured to disable for any schema changes
    val shouldDisable =
      if (hasBreakingChange) {
        true
      } else if (connection.nonBreakingChangesPreference == StandardSync.NonBreakingChangesPreference.DISABLE &&
        applySchemaChangeHelper.containsChanges(catalogDiff)
      ) {
        true
      } else {
        false
      }

    if (shouldDisable) {
      connection.status = StandardSync.Status.INACTIVE
    }

    // Persist to database
    connectionService.writeStandardSync(connection)
  }
}
