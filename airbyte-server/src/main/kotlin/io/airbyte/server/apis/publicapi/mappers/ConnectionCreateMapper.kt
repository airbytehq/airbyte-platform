/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.ConnectionCreate
import io.airbyte.api.server.generated.models.ConnectionScheduleData
import io.airbyte.api.server.generated.models.ConnectionScheduleDataCron
import io.airbyte.api.server.generated.models.ConnectionScheduleType
import io.airbyte.api.server.generated.models.ConnectionStatus
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
    destinationCatalogId: UUID?,
    configuredCatalog: AirbyteCatalog?,
  ): ConnectionCreate {
    val schedule = connectionCreateRequest.schedule
    // a schedule without a cron expression carries no schedule data; the schedule helper rejects it for cron schedules
    val scheduleData =
      schedule?.cronExpression?.let { cronExpression ->
        ConnectionScheduleData(cron = ConnectionScheduleDataCron(cronExpression = cronExpression, cronTimeZone = schedule.cronTimeZone ?: "UTC"))
      }
    val scheduleType =
      if (schedule != null) {
        ConnectionScheduleType.decode(schedule.scheduleType.toString())
          ?: throw IllegalArgumentException("Unexpected schedule type: ${schedule.scheduleType}")
      } else {
        ConnectionScheduleType.MANUAL
      }
    val status =
      connectionCreateRequest.status?.let {
        ConnectionStatus.decode(it.toString()) ?: throw IllegalArgumentException("Unexpected connection status: $it")
      } ?: ConnectionStatus.ACTIVE

    return ConnectionCreate(
      sourceId = connectionCreateRequest.sourceId,
      destinationId = connectionCreateRequest.destinationId,
      name = connectionCreateRequest.name,
      nonBreakingChangesPreference =
        ConnectionHelper.convertNonBreakingSchemaUpdatesBehaviorEnum(
          connectionCreateRequest.nonBreakingSchemaUpdatesBehavior,
        ),
      namespaceDefinition = ConnectionHelper.convertNamespaceDefinitionEnum(connectionCreateRequest.namespaceDefinition),
      namespaceFormat = connectionCreateRequest.namespaceFormat,
      prefix = connectionCreateRequest.prefix,
      scheduleType = scheduleType,
      scheduleData = scheduleData,
      sourceCatalogId = catalogId,
      syncCatalog = configuredCatalog,
      destinationCatalogId = destinationCatalogId,
      status = status,
      tags = connectionCreateRequest.tags?.let { ConnectionHelper.convertTags(it) } ?: emptyList(),
    )
  }
}
