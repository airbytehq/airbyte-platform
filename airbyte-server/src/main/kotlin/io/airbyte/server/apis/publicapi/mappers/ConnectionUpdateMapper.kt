/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.ConnectionScheduleData
import io.airbyte.api.server.generated.models.ConnectionScheduleDataCron
import io.airbyte.api.server.generated.models.ConnectionScheduleType
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.ConnectionUpdate
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
    val schedule = connectionPatchRequest.schedule
    val scheduleType =
      schedule?.let {
        if (it.scheduleType === ScheduleTypeEnum.MANUAL) {
          ConnectionScheduleType.MANUAL
        } else {
          ConnectionScheduleType.decode(it.scheduleType.toString())
            ?: throw IllegalArgumentException("Unexpected schedule type: ${it.scheduleType}")
        }
      }
    // Schedule data is only set if we're not manual. A schedule without a cron expression carries no schedule
    // data; the schedule helper rejects it for cron schedules.
    val scheduleData =
      schedule?.takeIf { it.scheduleType !== ScheduleTypeEnum.MANUAL }?.cronExpression?.let { cronExpression ->
        ConnectionScheduleData(cron = ConnectionScheduleDataCron(cronExpression = cronExpression, cronTimeZone = schedule.cronTimeZone ?: "UTC"))
      }

    return ConnectionUpdate(
      connectionId = connectionId!!,
      name = connectionPatchRequest.name,
      nonBreakingChangesPreference =
        connectionPatchRequest.nonBreakingSchemaUpdatesBehavior?.let { ConnectionHelper.convertNonBreakingSchemaUpdatesBehaviorEnum(it) },
      namespaceDefinition = connectionPatchRequest.namespaceDefinition?.let { ConnectionHelper.convertNamespaceDefinitionEnum(it) },
      namespaceFormat = connectionPatchRequest.namespaceFormat,
      prefix = connectionPatchRequest.prefix,
      scheduleType = scheduleType,
      scheduleData = scheduleData,
      sourceCatalogId = catalogId,
      syncCatalog = configuredCatalog,
      status =
        connectionPatchRequest.status?.let {
          ConnectionStatus.decode(it.toString()) ?: throw IllegalArgumentException("Unexpected connection status: $it")
        },
      tags = connectionPatchRequest.tags?.let { ConnectionHelper.convertTags(it) },
    )
  }
}
