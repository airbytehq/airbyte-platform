/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
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
      geography = Geography.decode(connectionCreateRequest.dataResidency.toString()) ?: Geography.AUTO,
      scheduleType =
        connectionCreateRequest.schedule?.scheduleType?.let {
            scheduleType ->
          ConnectionScheduleType.decode(scheduleType.toString())
        } ?: ConnectionScheduleType.MANUAL,
      scheduleData =
        connectionCreateRequest.schedule?.cronExpression?.let { cronExpression ->
          ConnectionScheduleData(
            basicSchedule = null,
            cron = ConnectionScheduleDataCron(cronExpression = cronExpression, cronTimeZone = "UTC"),
          )
        },
      sourceCatalogId = catalogId,
      syncCatalog = configuredCatalog,
      status = connectionCreateRequest.status?.let { status -> ConnectionStatus.decode(status.toString()) } ?: ConnectionStatus.ACTIVE,
    )
  }
}
