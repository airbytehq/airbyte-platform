/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.ConnectionScheduleType
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.ConnectionUpdate
import io.airbyte.api.server.generated.models.NamespaceDefinitionType
import io.airbyte.api.server.generated.models.NonBreakingChangesPreference
import io.airbyte.commons.US_DATAPLANE_GROUP
import io.airbyte.publicApi.server.generated.models.AirbyteApiConnectionSchedule
import io.airbyte.publicApi.server.generated.models.ConnectionPatchRequest
import io.airbyte.publicApi.server.generated.models.ConnectionStatusEnum
import io.airbyte.publicApi.server.generated.models.NamespaceDefinitionEnumNoDefault
import io.airbyte.publicApi.server.generated.models.NonBreakingSchemaUpdatesBehaviorEnumNoDefault
import io.airbyte.publicApi.server.generated.models.ScheduleTypeEnum
import io.airbyte.server.apis.publicapi.helpers.AirbyteCatalogHelper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectionUpdateMapperTest {
  @Test
  fun testConnectionUpdateMapper() {
    val connectionId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()

    val catalog =
      AirbyteCatalog(streams = emptyList())

    val connectionPatchRequest =
      ConnectionPatchRequest(
        name = "test",
        nonBreakingSchemaUpdatesBehavior = NonBreakingSchemaUpdatesBehaviorEnumNoDefault.DISABLE_CONNECTION,
        namespaceDefinition = NamespaceDefinitionEnumNoDefault.DESTINATION,
        namespaceFormat = "test",
        prefix = "test",
        dataResidency = US_DATAPLANE_GROUP,
        schedule =
          AirbyteApiConnectionSchedule(
            scheduleType = ScheduleTypeEnum.CRON,
            cronExpression = "0 0 0 0 0 0",
            cronTimeZone = "America/New_York",
          ),
        status = ConnectionStatusEnum.INACTIVE,
      )

    val expectedOssConnectionUpdateRequest =
      ConnectionUpdate(
        name = connectionPatchRequest.name,
        nonBreakingChangesPreference = NonBreakingChangesPreference.DISABLE,
        namespaceDefinition = NamespaceDefinitionType.DESTINATION,
        namespaceFormat = "test",
        prefix = "test",
        scheduleType = ConnectionScheduleType.CRON,
        sourceCatalogId = catalogId,
        syncCatalog = catalog,
        status = ConnectionStatus.INACTIVE,
        scheduleData =
          io.airbyte.api.server.generated.models.ConnectionScheduleData(
            cron =
              io.airbyte.api.server.generated.models.ConnectionScheduleDataCron(
                cronExpression = "0 0 0 0 0 0",
                cronTimeZone = "America/New_York",
              ),
          ),
        connectionId = connectionId,
      )
    Assertions.assertEquals(
      expectedOssConnectionUpdateRequest,
      ConnectionUpdateMapper.from(connectionId, connectionPatchRequest, catalogId, catalog),
    )
  }

  @Test
  fun `maps a non-UTC timezone on patch`() {
    val request =
      ConnectionPatchRequest(
        name = "test",
        nonBreakingSchemaUpdatesBehavior = NonBreakingSchemaUpdatesBehaviorEnumNoDefault.DISABLE_CONNECTION,
        namespaceDefinition = NamespaceDefinitionEnumNoDefault.DESTINATION,
        namespaceFormat = "test",
        prefix = "test",
        dataResidency = US_DATAPLANE_GROUP,
        schedule =
          AirbyteCatalogHelper.normalizeCronExpression(
            AirbyteApiConnectionSchedule(
              scheduleType = ScheduleTypeEnum.CRON,
              cronExpression = "0 0 0 * * ? Europe/Paris",
            ),
          ),
        status = ConnectionStatusEnum.INACTIVE,
      )

    val result =
      ConnectionUpdateMapper.from(
        UUID.randomUUID(),
        request,
        UUID.randomUUID(),
        AirbyteCatalog(streams = emptyList()),
      )

    assertEquals("0 0 0 * * ?", result.scheduleData!!.cron!!.cronExpression)
    assertEquals("Europe/Paris", result.scheduleData!!.cron!!.cronTimeZone)
  }

  @Test
  fun `defaults timezone to UTC when omitted`() {
    val request =
      ConnectionPatchRequest(
        schedule =
          AirbyteApiConnectionSchedule(
            scheduleType = ScheduleTypeEnum.CRON,
            cronExpression = "0 0 0 0 0 0",
          ),
      )

    val result =
      ConnectionUpdateMapper.from(
        UUID.randomUUID(),
        request,
        UUID.randomUUID(),
        AirbyteCatalog(streams = emptyList()),
      )

    assertEquals("UTC", result.scheduleData!!.cron!!.cronTimeZone)
  }
}
