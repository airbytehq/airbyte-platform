/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.mappers

import io.airbyte.airbyte_api.model.generated.ConnectionCreateRequest
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.ConnectionCreate
import io.airbyte.api.client.model.generated.ConnectionScheduleData
import io.airbyte.api.client.model.generated.ConnectionScheduleDataCron
import io.airbyte.api.client.model.generated.ConnectionScheduleType
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.Geography
import io.airbyte.api.server.helpers.ConnectionHelper
import java.util.UUID

/**
 * Mappers that help convert models from the public api to models from the config api.
 */
object ConnectionCreateMapper {
  /**
   * Converts a ConnectionCreateRequest object from the public api to a ConnectionCreate from the
   * config api. Assumes validation has been done for all the connectino create request configurations
   * including but not limited to cron expressions, streams, and their sync modes.
   *
   * @param connectionCreateRequest Input of a connection create from public api
   * @return ConnectionCreate Response object to be sent to config api
   */
  fun from(
    connectionCreateRequest: ConnectionCreateRequest,
    catalogId: UUID?,
    configuredCatalog: AirbyteCatalog?,
  ): ConnectionCreate {
    val connectionCreateOss = ConnectionCreate()
    connectionCreateOss.sourceId = connectionCreateRequest.sourceId
    connectionCreateOss.destinationId = connectionCreateRequest.destinationId
    connectionCreateOss.name = connectionCreateRequest.name
    connectionCreateOss.nonBreakingChangesPreference =
      ConnectionHelper.convertNonBreakingSchemaUpdatesBehaviorEnum(
        connectionCreateRequest.nonBreakingSchemaUpdatesBehavior,
      )
    connectionCreateOss.namespaceDefinition = ConnectionHelper.convertNamespaceDefinitionEnum(connectionCreateRequest.namespaceDefinition)
    if (connectionCreateRequest.namespaceFormat != null) {
      connectionCreateOss.namespaceFormat = connectionCreateRequest.namespaceFormat
    }
    if (connectionCreateRequest.prefix != null) {
      connectionCreateOss.setPrefix(connectionCreateRequest.prefix)
    }

    // set geography
    connectionCreateOss.setGeography(Geography.fromValue(connectionCreateRequest.dataResidency.toString()))

    // set schedule
    if (connectionCreateRequest.schedule != null) {
      connectionCreateOss.scheduleType = ConnectionScheduleType.fromValue(connectionCreateRequest.schedule.scheduleType.toString())
      val connectionScheduleDataCron = ConnectionScheduleDataCron()
      connectionScheduleDataCron.cronExpression = connectionCreateRequest.schedule.cronExpression
      connectionScheduleDataCron.setCronTimeZone("UTC")
      val connectionScheduleData = ConnectionScheduleData()
      connectionScheduleData.setCron(connectionScheduleDataCron)
      connectionCreateOss.setScheduleData(connectionScheduleData)
    } else {
      connectionCreateOss.setScheduleType(ConnectionScheduleType.MANUAL)
    }

    // set streams
    if (catalogId != null) {
      connectionCreateOss.setSourceCatalogId(catalogId)
    }
    if (configuredCatalog != null) {
      connectionCreateOss.setSyncCatalog(configuredCatalog)
    }
    if (connectionCreateRequest.status != null) {
      connectionCreateOss.setStatus(ConnectionStatus.fromValue(connectionCreateRequest.status.toString()))
    } else {
      connectionCreateOss.setStatus(ConnectionStatus.ACTIVE)
    }
    return connectionCreateOss
  }
}
