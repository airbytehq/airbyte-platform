/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.commons.US_DATAPLANE_GROUP
import io.airbyte.publicApi.server.generated.models.AirbyteApiConnectionSchedule
import io.airbyte.publicApi.server.generated.models.ConnectionPatchRequest
import io.airbyte.publicApi.server.generated.models.ConnectionStatusEnum
import io.airbyte.publicApi.server.generated.models.NamespaceDefinitionEnumNoDefault
import io.airbyte.publicApi.server.generated.models.NonBreakingSchemaUpdatesBehaviorEnumNoDefault
import io.airbyte.publicApi.server.generated.models.ScheduleTypeEnum
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectionUpdateMapperTest {
  @Test
  fun testConnectionUpdateMapper() {
    val connectionId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()

    val catalog =
      AirbyteCatalog().apply {
        this.streams = emptyList()
      }

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
          ),
        status = ConnectionStatusEnum.INACTIVE,
      )

    val expectedOssConnectionUpdateRequest =
      ConnectionUpdate().apply {
        this.name = connectionPatchRequest.name
        this.nonBreakingChangesPreference = NonBreakingChangesPreference.DISABLE
        this.namespaceDefinition = NamespaceDefinitionType.DESTINATION
        this.namespaceFormat = "test"
        this.prefix = "test"
        this.scheduleType = ConnectionScheduleType.CRON
        this.sourceCatalogId = catalogId
        this.syncCatalog = catalog
        this.status = ConnectionStatus.INACTIVE
        val connectionScheduleDataCron =
          io.airbyte.api.model.generated.ConnectionScheduleDataCron().apply {
            this.cronExpression = "0 0 0 0 0 0"
            this.cronTimeZone = "UTC"
          }
        val connectionScheduleData =
          io.airbyte.api.model.generated.ConnectionScheduleData().apply {
            this.cron = connectionScheduleDataCron
          }
        this.scheduleData = connectionScheduleData
        this.connectionId = connectionId
      }
    Assertions.assertEquals(
      expectedOssConnectionUpdateRequest,
      ConnectionUpdateMapper.from(connectionId, connectionPatchRequest, catalogId, catalog),
    )
  }
}
