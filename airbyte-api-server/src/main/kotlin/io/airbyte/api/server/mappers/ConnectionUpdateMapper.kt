/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.mappers

import io.airbyte.airbyte_api.model.generated.ConnectionPatchRequest
import io.airbyte.airbyte_api.model.generated.ScheduleTypeEnum
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.ConnectionScheduleData
import io.airbyte.api.client.model.generated.ConnectionScheduleDataCron
import io.airbyte.api.client.model.generated.ConnectionScheduleType
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.ConnectionUpdate
import io.airbyte.api.client.model.generated.Geography
import io.airbyte.api.server.helpers.ConnectionHelper
import java.util.UUID
import javax.validation.constraints.NotBlank

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
    connectionUpdateOss.setName(connectionPatchRequest.getName())
    if (connectionPatchRequest.getNonBreakingSchemaUpdatesBehavior() != null) {
      connectionUpdateOss.setNonBreakingChangesPreference(
        ConnectionHelper.convertNonBreakingSchemaUpdatesBehaviorEnum(connectionPatchRequest.getNonBreakingSchemaUpdatesBehavior()),
      )
    }
    if (connectionPatchRequest.getNamespaceDefinition() != null) {
      connectionUpdateOss.setNamespaceDefinition(
        ConnectionHelper.convertNamespaceDefinitionEnum(connectionPatchRequest.getNamespaceDefinition()),
      )
    }
    if (connectionPatchRequest.getNamespaceFormat() != null) {
      connectionUpdateOss.setNamespaceFormat(connectionPatchRequest.getNamespaceFormat())
    }
    if (connectionPatchRequest.getPrefix() != null) {
      connectionUpdateOss.setPrefix(connectionPatchRequest.getPrefix())
    }

    // set geography
    if (connectionPatchRequest.getDataResidency() != null) {
      connectionUpdateOss.setGeography(Geography.fromValue(connectionPatchRequest.getDataResidency().toString()))
    }

    // set schedule
    if (connectionPatchRequest.getSchedule() != null) {
      connectionUpdateOss.setScheduleType(ConnectionScheduleType.fromValue(connectionPatchRequest.getSchedule().getScheduleType().toString()))
      if (connectionPatchRequest.getSchedule().getScheduleType() !== ScheduleTypeEnum.MANUAL) {
        // This should only be set if we're not manual
        val connectionScheduleDataCron = ConnectionScheduleDataCron()
        connectionScheduleDataCron.setCronExpression(connectionPatchRequest.getSchedule().getCronExpression())
        connectionScheduleDataCron.setCronTimeZone("UTC")
        val connectionScheduleData = ConnectionScheduleData()
        connectionScheduleData.setCron(connectionScheduleDataCron)
        connectionUpdateOss.setScheduleData(connectionScheduleData)
      } else {
        connectionUpdateOss.setScheduleType(ConnectionScheduleType.MANUAL)
      }
    }

    // set streams
    if (catalogId != null) {
      connectionUpdateOss.setSourceCatalogId(catalogId)
    }
    if (configuredCatalog != null) {
      connectionUpdateOss.setSyncCatalog(configuredCatalog)
    }
    if (connectionPatchRequest.getStatus() != null) {
      connectionUpdateOss.setStatus(ConnectionStatus.fromValue(connectionPatchRequest.getStatus().toString()))
    }
    return connectionUpdateOss
  }
}
