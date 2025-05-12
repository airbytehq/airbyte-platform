/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionScheduleData
import io.airbyte.api.model.generated.ConnectionScheduleDataCron
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest
import io.airbyte.server.apis.publicapi.helpers.ConnectionHelper
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
    dataplaneGroupId: UUID?,
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
      connectionCreateOss.prefix = connectionCreateRequest.prefix
    }

    connectionCreateOss.dataplaneGroupId = dataplaneGroupId

    // set schedule
    if (connectionCreateRequest.schedule != null) {
      connectionCreateOss.scheduleType = ConnectionScheduleType.fromValue(connectionCreateRequest.schedule!!.scheduleType.toString())
      val connectionScheduleDataCron = ConnectionScheduleDataCron()
      connectionScheduleDataCron.cronExpression = connectionCreateRequest.schedule!!.cronExpression
      connectionScheduleDataCron.cronTimeZone = "UTC"
      val connectionScheduleData = ConnectionScheduleData()
      connectionScheduleData.cron = connectionScheduleDataCron
      connectionCreateOss.scheduleData = connectionScheduleData
    } else {
      connectionCreateOss.scheduleType = ConnectionScheduleType.MANUAL
    }

    // set streams
    if (catalogId != null) {
      connectionCreateOss.sourceCatalogId = catalogId
    }
    if (configuredCatalog != null) {
      connectionCreateOss.syncCatalog = configuredCatalog
    }
    if (connectionCreateRequest.status != null) {
      connectionCreateOss.status = ConnectionStatus.fromValue(connectionCreateRequest.status.toString())
    } else {
      connectionCreateOss.status = ConnectionStatus.ACTIVE
    }

    if (connectionCreateRequest.tags != null) {
      connectionCreateOss.tags = ConnectionHelper.convertTags(connectionCreateRequest.tags ?: emptyList())
    }

    return connectionCreateOss
  }
}
