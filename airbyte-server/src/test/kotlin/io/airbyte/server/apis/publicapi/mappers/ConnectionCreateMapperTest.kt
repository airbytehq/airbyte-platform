package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.Geography
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.public_api.model.generated.AirbyteApiConnectionSchedule
import io.airbyte.public_api.model.generated.ConnectionCreateRequest
import io.airbyte.public_api.model.generated.ConnectionStatusEnum
import io.airbyte.public_api.model.generated.GeographyEnum
import io.airbyte.public_api.model.generated.NamespaceDefinitionEnum
import io.airbyte.public_api.model.generated.ScheduleTypeEnum
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectionCreateMapperTest {
  @Test
  fun testConnectionCreateMapper() {
    val catalogId = UUID.randomUUID()

    val catalog =
      AirbyteCatalog().apply {
        this.streams = emptyList()
      }

    val connectionCreateRequest =
      ConnectionCreateRequest().apply {
        this.sourceId = UUID.randomUUID()
        this.destinationId = UUID.randomUUID()
        this.name = "test"
        this.nonBreakingSchemaUpdatesBehavior = io.airbyte.public_api.model.generated.NonBreakingSchemaUpdatesBehaviorEnum.DISABLE_CONNECTION
        this.namespaceDefinition = NamespaceDefinitionEnum.DESTINATION
        this.namespaceFormat = "test"
        this.prefix = "test"
        this.dataResidency = GeographyEnum.US
        this.schedule =
          AirbyteApiConnectionSchedule().apply {
            this.scheduleType = ScheduleTypeEnum.CRON
            this.cronExpression = "0 0 0 0 0 0"
          }
        this.status = ConnectionStatusEnum.INACTIVE
      }

    val expectedOssConnectionCreateRequest =
      ConnectionCreate().apply {
        this.sourceId = connectionCreateRequest.sourceId
        this.destinationId = connectionCreateRequest.destinationId
        this.name = connectionCreateRequest.name
        this.nonBreakingChangesPreference = NonBreakingChangesPreference.DISABLE
        this.namespaceDefinition = NamespaceDefinitionType.DESTINATION
        this.namespaceFormat = "test"
        this.prefix = "test"
        this.geography = Geography.US
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
      }
    assertEquals(expectedOssConnectionCreateRequest, ConnectionCreateMapper.from(connectionCreateRequest, catalogId, catalog))
  }
}
