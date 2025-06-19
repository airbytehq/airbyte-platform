/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.ConnectionScheduleData
import io.airbyte.api.model.generated.ConnectionScheduleDataCron
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.publicApi.server.generated.models.ConnectionPatchRequest
import io.airbyte.publicApi.server.generated.models.ScheduleTypeEnum
import io.airbyte.server.apis.publicapi.helpers.ConnectionHelper
import jakarta.validation.constraints.NotBlank
import java.util.UUID

/**
 * Mappers that help convert models from the public api to models from the config api.
 */
object ConnectionUpdateMapper {
  /**
   * Converts a ConnectionPatchRequest object from the public api to a ConnectionUpdate from the
   * config api. Assumes validation has been done for all the connection configurations including but
   * not limited to cron expressions, streams, and their sync modes.
   *
   * @param connectionId connection Id
   * @param connectionPatchRequest Input of a connection put from public api
   * @return ConnectionCreate Response object to be sent to config api
   */
  fun from(
    connectionId: @NotBlank UUID?,
    connectionPatchRequest: ConnectionPatchRequest,
    catalogId: UUID?,
    configuredCatalog: AirbyteCatalog?,
  ): ConnectionUpdate {
    val connectionUpdateOss = ConnectionUpdate()
    connectionUpdateOss.connectionId(connectionId)
    connectionUpdateOss.name = connectionPatchRequest.name
    if (connectionPatchRequest.nonBreakingSchemaUpdatesBehavior != null) {
      connectionUpdateOss.nonBreakingChangesPreference =
        ConnectionHelper.convertNonBreakingSchemaUpdatesBehaviorEnum(
          connectionPatchRequest.nonBreakingSchemaUpdatesBehavior,
        )
    }
    if (connectionPatchRequest.namespaceDefinition != null) {
      connectionUpdateOss.namespaceDefinition = ConnectionHelper.convertNamespaceDefinitionEnum(connectionPatchRequest.namespaceDefinition)
    }
    if (connectionPatchRequest.namespaceFormat != null) {
      connectionUpdateOss.namespaceFormat = connectionPatchRequest.namespaceFormat
    }
    if (connectionPatchRequest.prefix != null) {
      connectionUpdateOss.prefix = connectionPatchRequest.prefix
    }

    // set schedule
    if (connectionPatchRequest.schedule != null) {
      connectionUpdateOss.scheduleType = ConnectionScheduleType.fromValue(connectionPatchRequest.schedule!!.scheduleType.toString())
      if (connectionPatchRequest.schedule!!.scheduleType !== ScheduleTypeEnum.MANUAL) {
        // This should only be set if we're not manual
        val connectionScheduleDataCron = ConnectionScheduleDataCron()
        connectionScheduleDataCron.cronExpression = connectionPatchRequest.schedule!!.cronExpression
        connectionScheduleDataCron.cronTimeZone = "UTC"
        val connectionScheduleData = ConnectionScheduleData()
        connectionScheduleData.cron = connectionScheduleDataCron
        connectionUpdateOss.scheduleData = connectionScheduleData
      } else {
        connectionUpdateOss.scheduleType = ConnectionScheduleType.MANUAL
      }
    }

    // set streams
    if (catalogId != null) {
      connectionUpdateOss.sourceCatalogId = catalogId
    }
    if (configuredCatalog != null) {
      connectionUpdateOss.syncCatalog = configuredCatalog
    }
    if (connectionPatchRequest.status != null) {
      connectionUpdateOss.status = ConnectionStatus.fromValue(connectionPatchRequest.status.toString())
    }
    if (connectionPatchRequest.tags != null) {
      connectionUpdateOss.tags = ConnectionHelper.convertTags(connectionPatchRequest.tags ?: emptyList())
    }
    return connectionUpdateOss
  }
}
