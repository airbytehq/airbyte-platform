/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.commons.US_DATAPLANE_GROUP
import io.airbyte.publicApi.server.generated.models.AirbyteApiConnectionSchedule
import io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest
import io.airbyte.publicApi.server.generated.models.ConnectionStatusEnum
import io.airbyte.publicApi.server.generated.models.NamespaceDefinitionEnum
import io.airbyte.publicApi.server.generated.models.ScheduleTypeEnum
import io.airbyte.server.apis.publicapi.helpers.AirbyteCatalogHelper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectionCreateMapperTest {
  @Test
  fun testConnectionCreateMapper() {
    val catalogId = UUID.randomUUID()
    val destinationCatalogId = UUID.randomUUID()

    val catalog =
      AirbyteCatalog().apply {
        this.streams = emptyList()
      }

    val connectionCreateRequest =
      ConnectionCreateRequest(
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        name = "test",
        nonBreakingSchemaUpdatesBehavior = io.airbyte.publicApi.server.generated.models.NonBreakingSchemaUpdatesBehaviorEnum.DISABLE_CONNECTION,
        namespaceDefinition = NamespaceDefinitionEnum.DESTINATION,
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

    val expectedOssConnectionCreateRequest =
      ConnectionCreate().apply {
        this.sourceId = connectionCreateRequest.sourceId
        this.destinationId = connectionCreateRequest.destinationId
        this.name = connectionCreateRequest.name
        this.nonBreakingChangesPreference = NonBreakingChangesPreference.DISABLE
        this.namespaceDefinition = NamespaceDefinitionType.DESTINATION
        this.namespaceFormat = "test"
        this.prefix = "test"
        this.scheduleType = ConnectionScheduleType.CRON
        this.sourceCatalogId = catalogId
        this.destinationCatalogId = destinationCatalogId
        this.syncCatalog = catalog
        this.status = ConnectionStatus.INACTIVE
        val connectionScheduleDataCron =
          io.airbyte.api.model.generated.ConnectionScheduleDataCron().apply {
            this.cronExpression = "0 0 0 0 0 0"
            this.cronTimeZone = "America/New_York"
          }
        val connectionScheduleData =
          io.airbyte.api.model.generated.ConnectionScheduleData().apply {
            this.cron = connectionScheduleDataCron
          }
        this.scheduleData = connectionScheduleData
      }
    assertEquals(expectedOssConnectionCreateRequest, ConnectionCreateMapper.from(connectionCreateRequest, catalogId, destinationCatalogId, catalog))
  }

  @Test
  fun `maps a suffixed timezone after normalization`() {
    val request =
      ConnectionCreateRequest(
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        name = "test",
        nonBreakingSchemaUpdatesBehavior =
          io.airbyte.publicApi.server.generated.models.NonBreakingSchemaUpdatesBehaviorEnum.DISABLE_CONNECTION,
        namespaceDefinition = NamespaceDefinitionEnum.DESTINATION,
        namespaceFormat = "test",
        prefix = "test",
        dataResidency = US_DATAPLANE_GROUP,
        schedule =
          AirbyteCatalogHelper.normalizeCronExpression(
            AirbyteApiConnectionSchedule(
              scheduleType = ScheduleTypeEnum.CRON,
              cronExpression = "0 0 0 * * ? America/Los_Angeles",
            ),
          ),
        status = ConnectionStatusEnum.INACTIVE,
      )

    val result =
      ConnectionCreateMapper.from(
        request,
        UUID.randomUUID(),
        UUID.randomUUID(),
        AirbyteCatalog().apply { streams = emptyList() },
      )

    assertEquals("0 0 0 * * ?", result.scheduleData.cron.cronExpression)
    assertEquals("America/Los_Angeles", result.scheduleData.cron.cronTimeZone)
  }

  @Test
  fun `defaults timezone to UTC when omitted`() {
    val request =
      ConnectionCreateRequest(
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        name = "test",
        nonBreakingSchemaUpdatesBehavior =
          io.airbyte.publicApi.server.generated.models.NonBreakingSchemaUpdatesBehaviorEnum.DISABLE_CONNECTION,
        namespaceDefinition = NamespaceDefinitionEnum.DESTINATION,
        namespaceFormat = "test",
        prefix = "test",
        dataResidency = US_DATAPLANE_GROUP,
        schedule =
          AirbyteApiConnectionSchedule(
            scheduleType = ScheduleTypeEnum.CRON,
            cronExpression = "0 0 0 0 0 0",
          ),
        status = ConnectionStatusEnum.INACTIVE,
      )

    val result =
      ConnectionCreateMapper.from(
        request,
        UUID.randomUUID(),
        UUID.randomUUID(),
        AirbyteCatalog().apply { streams = emptyList() },
      )

    assertEquals("UTC", result.scheduleData.cron.cronTimeZone)
  }
}
