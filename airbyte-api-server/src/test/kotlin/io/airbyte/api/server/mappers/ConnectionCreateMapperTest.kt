package io.airbyte.api.server.mappers

import io.airbyte.airbyte_api.model.generated.ConnectionCreateRequest
import io.airbyte.airbyte_api.model.generated.ConnectionSchedule
import io.airbyte.airbyte_api.model.generated.ConnectionStatusEnum
import io.airbyte.airbyte_api.model.generated.GeographyEnum
import io.airbyte.airbyte_api.model.generated.NamespaceDefinitionEnum
import io.airbyte.airbyte_api.model.generated.NonBreakingSchemaUpdatesBehaviorEnum
import io.airbyte.airbyte_api.model.generated.ScheduleTypeEnum
import io.airbyte.api.client.model.generated.ConnectionCreate
import io.airbyte.api.client.model.generated.ConnectionScheduleData
import io.airbyte.api.client.model.generated.ConnectionScheduleDataCron
import io.airbyte.api.client.model.generated.ConnectionScheduleType
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.Geography
import io.airbyte.api.client.model.generated.NamespaceDefinitionType
import io.airbyte.api.client.model.generated.NonBreakingChangesPreference
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectionCreateMapperTest {
  @Test
  fun testFrom() {
    val request =
      ConnectionCreateRequest().apply {
        this.name = "test"
        this.sourceId = UUID.randomUUID()
        this.destinationId = UUID.randomUUID()
        this.schedule =
          ConnectionSchedule().apply {
            this.scheduleType = ScheduleTypeEnum.CRON
            this.cronExpression = "0 0 * * *"
          }
        this.status = ConnectionStatusEnum.ACTIVE
        this.dataResidency = GeographyEnum.US
        this.nonBreakingSchemaUpdatesBehavior = NonBreakingSchemaUpdatesBehaviorEnum.DISABLE_CONNECTION
        this.namespaceDefinition = NamespaceDefinitionEnum.SOURCE
      }

    val catalogId = UUID.randomUUID()
    val catalog = null

    val expected =
      ConnectionCreate(
        sourceId = request.sourceId,
        destinationId = request.destinationId,
        name = "test",
        scheduleType = ConnectionScheduleType.CRON,
        scheduleData =
          ConnectionScheduleData(
            cron =
              ConnectionScheduleDataCron(
                cronExpression = "0 0 * * *",
                cronTimeZone = "UTC",
              ),
          ),
        status = ConnectionStatus.ACTIVE,
        geography = Geography.US,
        nonBreakingChangesPreference = NonBreakingChangesPreference.DISABLE,
        namespaceDefinition = NamespaceDefinitionType.SOURCE,
        sourceCatalogId = catalogId,
        syncCatalog = catalog,
      )

    assert(expected == ConnectionCreateMapper.from(request, catalogId, catalog))
  }
}
