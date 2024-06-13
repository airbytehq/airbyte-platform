/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
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
    connectionId: @NotBlank UUID,
    connectionPatchRequest: ConnectionPatchRequest,
    catalogId: UUID?,
    configuredCatalog: AirbyteCatalog?,
  ): ConnectionUpdate {
    return ConnectionUpdate(
      connectionId = connectionId,
      name = connectionPatchRequest.name,
      nonBreakingChangesPreference =
        connectionPatchRequest.nonBreakingSchemaUpdatesBehavior?.let {
          ConnectionHelper.convertNonBreakingSchemaUpdatesBehaviorEnum(connectionPatchRequest.nonBreakingSchemaUpdatesBehavior)
        } ?: null,
      namespaceDefinition =
        connectionPatchRequest.namespaceDefinition?.let {
          ConnectionHelper.convertNamespaceDefinitionEnum(connectionPatchRequest.namespaceDefinition)
        } ?: null,
      namespaceFormat = connectionPatchRequest.namespaceFormat,
      prefix = connectionPatchRequest.prefix,
      geography =
        connectionPatchRequest.dataResidency?.let {
          Geography.valueOf(connectionPatchRequest.dataResidency.toString())
        } ?: null,
      scheduleType =
        connectionPatchRequest.schedule?.let { schedule ->
          if (schedule.scheduleType !== ScheduleTypeEnum.MANUAL) {
            ConnectionScheduleType.valueOf(schedule.scheduleType.toString())
          } else {
            ConnectionScheduleType.MANUAL
          }
        } ?: null,
      scheduleData =
        connectionPatchRequest.schedule?.let { schedule ->
          if (schedule.scheduleType !== ScheduleTypeEnum.MANUAL) {
            ConnectionScheduleData(
              basicSchedule = null,
              cron =
                ConnectionScheduleDataCron(
                  cronExpression = schedule.cronExpression,
                  cronTimeZone = "UTC",
                ),
            )
          } else {
            null
          }
        } ?: null,
      sourceCatalogId = catalogId,
      syncCatalog = configuredCatalog,
      status =
        connectionPatchRequest.status?.let {
          ConnectionStatus.valueOf(connectionPatchRequest.status.toString())
        } ?: null,
    )
  }
}
