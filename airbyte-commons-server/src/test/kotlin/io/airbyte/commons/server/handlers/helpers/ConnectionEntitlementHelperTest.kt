/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.commons.entitlements.EntitlementHelper
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.ConnectorEntitlement
import io.airbyte.commons.entitlements.models.DestinationSalesforceEnterpriseConnector
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.entitlements.models.FasterSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.MappersEntitlement
import io.airbyte.commons.entitlements.models.SourceWorkdayEnterpriseConnector
import io.airbyte.commons.server.helpers.ConnectionHelpers
import io.airbyte.config.Cron
import io.airbyte.config.MapperConfig
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync
import io.airbyte.config.helpers.CronExpressionHelper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.models.OrganizationId
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.util.UUID

/**
 * Unit tests for ConnectionEntitlementHelper.
 *
 * Note: These tests treat OrganizationId as a UUID for simplicity.
 * The wrapper conversion is handled in the actual implementation.
 */
class ConnectionEntitlementHelperTest {
  private lateinit var entitlementService: EntitlementService
  private lateinit var connectionService: ConnectionService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var cronExpressionHelper: CronExpressionHelper
  private lateinit var entitlementHelper: EntitlementHelper
  private lateinit var connectionEntitlementHelper: ConnectionEntitlementHelper

  private val connectionId = UUID.randomUUID()
  private val sourceDefinitionId = SourceWorkdayEnterpriseConnector.actorDefinitionId
  private val destinationDefinitionId = DestinationSalesforceEnterpriseConnector.actorDefinitionId
  private val organizationId = OrganizationId(UUID.randomUUID())
  private val statusReason = "Connection locked due to subscription downgrade"

  @BeforeEach
  fun setUp() {
    entitlementService = mock()
    connectionService = mock()
    sourceService = mock()
    destinationService = mock()
    cronExpressionHelper = CronExpressionHelper()
    entitlementHelper = mock()
    connectionEntitlementHelper =
      ConnectionEntitlementHelper(connectionService, cronExpressionHelper, entitlementService, sourceService, destinationService, entitlementHelper)
  }

  private fun createMockConnection(
    connectionId: UUID = this.connectionId,
    cronExpression: String = "0 0 * * * ?", // Default: hourly
    withMappers: Boolean = false,
  ): StandardSync {
    val connection =
      StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(UUID.randomUUID())
        .withDestinationId(UUID.randomUUID())
        .withCatalog(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog())
        .withScheduleData(
          ScheduleData()
            .withCron(
              Cron()
                .withCronExpression(cronExpression)
                .withCronTimeZone("UTC"),
            ),
        )

    if (withMappers) {
      // Add a mapper to the first stream
      val streamWithMapper =
        connection.catalog.streams.first().also { stream ->
          stream.mappers =
            listOf(
              object : MapperConfig {
                override fun name(): String = "hashing"

                override fun id(): UUID = UUID.randomUUID()

                override fun documentationUrl(): String? = null

                override fun config(): Any = mapOf<String, Any>()
              },
            )
        }
      connection.catalog.streams = listOf(streamWithMapper)
    }

    return connection
  }

  @Test
  fun `isEntitledToConnection returns false when not entitled to source`() {
    val connection = createMockConnection()
    val sourceEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + sourceDefinitionId)!!
    val destEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + destinationDefinitionId)!!

    whenever(entitlementService.checkEntitlement(organizationId, sourceEntitlement)).thenReturn(
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
      ),
    )
    whenever(entitlementService.checkEntitlement(organizationId, destEntitlement)).thenReturn(
      EntitlementResult(
        featureId = destEntitlement.featureId,
        isEntitled = true,
        reason = null,
      ),
    )

    val result =
      connectionEntitlementHelper.isEntitledToConnection(
        connection = connection,
        subHourSyncIds = emptyList(),
        sourceDefinitionId = sourceDefinitionId,
        destinationDefinitionId = destinationDefinitionId,
        organizationId = organizationId,
      )

    assertFalse(result)
  }

  @Test
  fun `isEntitledToConnection returns false when not entitled to destination`() {
    val connection = createMockConnection()
    val sourceEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + sourceDefinitionId)!!
    val destEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + destinationDefinitionId)!!

    whenever(entitlementService.checkEntitlement(organizationId, sourceEntitlement)).thenReturn(
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = true,
        reason = null,
      ),
    )
    whenever(entitlementService.checkEntitlement(organizationId, destEntitlement)).thenReturn(
      EntitlementResult(
        featureId = destEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
      ),
    )

    val result =
      connectionEntitlementHelper.isEntitledToConnection(
        connection = connection,
        subHourSyncIds = emptyList(),
        sourceDefinitionId = sourceDefinitionId,
        destinationDefinitionId = destinationDefinitionId,
        organizationId = organizationId,
      )

    assertFalse(result)
  }

  @Test
  fun `isEntitledToConnection returns true when entitled to both`() {
    val connection = createMockConnection()
    val sourceEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + sourceDefinitionId)!!
    val destEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + destinationDefinitionId)!!

    whenever(entitlementService.checkEntitlement(organizationId, sourceEntitlement)).thenReturn(
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = true,
        reason = null,
      ),
    )
    whenever(entitlementService.checkEntitlement(organizationId, destEntitlement)).thenReturn(
      EntitlementResult(
        featureId = destEntitlement.featureId,
        isEntitled = true,
        reason = null,
      ),
    )

    val result =
      connectionEntitlementHelper.isEntitledToConnection(
        connection = connection,
        subHourSyncIds = emptyList(),
        sourceDefinitionId = sourceDefinitionId,
        destinationDefinitionId = destinationDefinitionId,
        organizationId = organizationId,
      )

    assertTrue(result)
  }

  @Test
  fun `isEntitledToConnection returns false when both are enterprise connectors and only entitled to source`() {
    val connection = createMockConnection()
    val sourceEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + sourceDefinitionId)!!
    val destEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + destinationDefinitionId)!!

    whenever(entitlementService.checkEntitlement(organizationId, sourceEntitlement)).thenReturn(
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = true,
        reason = null,
      ),
    )
    whenever(entitlementService.checkEntitlement(organizationId, destEntitlement)).thenReturn(
      EntitlementResult(
        featureId = destEntitlement.featureId,
        isEntitled = false,
        reason = null,
      ),
    )

    val result =
      connectionEntitlementHelper.isEntitledToConnection(
        connection = connection,
        subHourSyncIds = emptyList(),
        sourceDefinitionId = sourceDefinitionId,
        destinationDefinitionId = destinationDefinitionId,
        organizationId = organizationId,
      )

    assertFalse(result)
  }

  @Test
  fun `isEntitledToConnection returns false when both are enterprise connectors and only entitled to destination`() {
    val connection = createMockConnection()
    val sourceEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + sourceDefinitionId)!!
    val destEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + destinationDefinitionId)!!

    whenever(entitlementService.checkEntitlement(organizationId, sourceEntitlement)).thenReturn(
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = false,
        reason = null,
      ),
    )
    whenever(entitlementService.checkEntitlement(organizationId, destEntitlement)).thenReturn(
      EntitlementResult(
        featureId = destEntitlement.featureId,
        isEntitled = true,
        reason = null,
      ),
    )

    val result =
      connectionEntitlementHelper.isEntitledToConnection(
        connection = connection,
        subHourSyncIds = emptyList(),
        sourceDefinitionId = sourceDefinitionId,
        destinationDefinitionId = destinationDefinitionId,
        organizationId = organizationId,
      )

    assertFalse(result)
  }

  @Test
  fun `isEntitledToConnection returns true when connector not in Entitlements registry`() {
    // Use random UUIDs that won't be in the Entitlements registry
    val connection = createMockConnection()
    val unknownSourceId = UUID.randomUUID()
    val unknownDestId = UUID.randomUUID()

    val result =
      connectionEntitlementHelper.isEntitledToConnection(
        connection = connection,
        subHourSyncIds = emptyList(),
        sourceDefinitionId = unknownSourceId,
        destinationDefinitionId = unknownDestId,
        organizationId = organizationId,
      )

    assertTrue(result)
  }

  @Test
  fun `isEntitledToConnection returns false when has mappers but not entitled`() {
    val connection = createMockConnection(withMappers = true)

    // Entitled to connectors but not mappers
    whenever(entitlementService.checkEntitlement(organizationId, MappersEntitlement)).thenReturn(
      EntitlementResult(
        featureId = MappersEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled to mappers",
      ),
    )

    val result =
      connectionEntitlementHelper.isEntitledToConnection(
        connection = connection,
        subHourSyncIds = emptyList(),
        sourceDefinitionId = UUID.randomUUID(),
        destinationDefinitionId = UUID.randomUUID(),
        organizationId = organizationId,
      )

    assertFalse(result)
  }

  @Test
  fun `isEntitledToConnection returns false when sub-hourly sync but not entitled`() {
    val connection = createMockConnection(cronExpression = "0 */30 * * * ?")

    // Entitled to connectors but not faster sync
    whenever(entitlementService.checkEntitlement(organizationId, FasterSyncFrequencyEntitlement)).thenReturn(
      EntitlementResult(
        featureId = FasterSyncFrequencyEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled to faster sync frequency",
      ),
    )

    val result =
      connectionEntitlementHelper.isEntitledToConnection(
        connection = connection,
        subHourSyncIds = listOf(connectionId),
        sourceDefinitionId = UUID.randomUUID(),
        destinationDefinitionId = UUID.randomUUID(),
        organizationId = organizationId,
      )

    assertFalse(result)
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization unlocks entitled locked connection`() {
    val lockedConnectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destId = UUID.randomUUID()
    val unknownSourceDefId = UUID.randomUUID()
    val unknownDestDefId = UUID.randomUUID()

    val lockedConnection =
      createMockConnection(connectionId = lockedConnectionId)
        .withSourceId(sourceId)
        .withDestinationId(destId)
        .withStatus(StandardSync.Status.LOCKED)

    val sourceDefinition = mock<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mock<io.airbyte.config.StandardDestinationDefinition>()

    whenever(connectionService.listConnectionIdsForOrganization(organizationId.value))
      .thenReturn(listOf(lockedConnectionId))
    whenever(entitlementHelper.findSubHourSyncIds(organizationId)).thenReturn(emptyList())
    whenever(connectionService.getStandardSync(lockedConnectionId)).thenReturn(lockedConnection)
    whenever(sourceService.getSourceDefinitionFromSource(sourceId)).thenReturn(sourceDefinition)
    whenever(sourceDefinition.sourceDefinitionId).thenReturn(unknownSourceDefId)
    whenever(destinationService.getDestinationDefinitionFromDestination(destId)).thenReturn(destDefinition)
    whenever(destDefinition.destinationDefinitionId).thenReturn(unknownDestDefId)

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    org.mockito.kotlin.verify(connectionService).updateConnectionStatus(
      lockedConnectionId,
      StandardSync.Status.INACTIVE,
      null,
    )
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization does not unlock non-entitled locked connection`() {
    val lockedConnectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destId = UUID.randomUUID()

    val lockedConnection =
      createMockConnection(connectionId = lockedConnectionId)
        .withSourceId(sourceId)
        .withDestinationId(destId)
        .withStatus(StandardSync.Status.LOCKED)

    val sourceDefinition = mock<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mock<io.airbyte.config.StandardDestinationDefinition>()
    val sourceEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + sourceDefinitionId)!!

    whenever(connectionService.listConnectionIdsForOrganization(organizationId.value))
      .thenReturn(listOf(lockedConnectionId))
    whenever(entitlementHelper.findSubHourSyncIds(organizationId)).thenReturn(emptyList())
    whenever(connectionService.getStandardSync(lockedConnectionId)).thenReturn(lockedConnection)
    whenever(sourceService.getSourceDefinitionFromSource(sourceId)).thenReturn(sourceDefinition)
    whenever(sourceDefinition.sourceDefinitionId).thenReturn(sourceDefinitionId)
    whenever(destinationService.getDestinationDefinitionFromDestination(destId)).thenReturn(destDefinition)
    whenever(destDefinition.destinationDefinitionId).thenReturn(destinationDefinitionId)
    whenever(entitlementService.checkEntitlement(eq(organizationId), any<Entitlement>())).thenReturn(
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
      ),
    )

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    org.mockito.kotlin.verify(connectionService, org.mockito.kotlin.never()).updateConnectionStatus(
      any(),
      any(),
      any(),
    )
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization skips non-locked connections`() {
    val activeConnectionId = UUID.randomUUID()
    val activeConnection =
      createMockConnection(connectionId = activeConnectionId)
        .withStatus(StandardSync.Status.ACTIVE)

    whenever(connectionService.listConnectionIdsForOrganization(organizationId.value))
      .thenReturn(listOf(activeConnectionId))
    whenever(entitlementHelper.findSubHourSyncIds(organizationId)).thenReturn(emptyList())
    whenever(connectionService.getStandardSync(activeConnectionId)).thenReturn(activeConnection)

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    org.mockito.kotlin.verify(connectionService, org.mockito.kotlin.never()).updateConnectionStatus(
      any(),
      any(),
      any(),
    )
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization handles multiple connections correctly`() {
    val lockedEntitledId = UUID.randomUUID()
    val lockedNotEntitledId = UUID.randomUUID()
    val activeId = UUID.randomUUID()

    val sourceId1 = UUID.randomUUID()
    val destId1 = UUID.randomUUID()
    val sourceId2 = UUID.randomUUID()
    val destId2 = UUID.randomUUID()
    val sourceId3 = UUID.randomUUID()
    val destId3 = UUID.randomUUID()

    val unknownSourceDefId1 = UUID.randomUUID()
    val unknownDestDefId1 = UUID.randomUUID()

    // Locked and entitled
    val lockedEntitled =
      createMockConnection(connectionId = lockedEntitledId)
        .withSourceId(sourceId1)
        .withDestinationId(destId1)
        .withStatus(StandardSync.Status.LOCKED)

    // Locked but not entitled
    val lockedNotEntitled =
      createMockConnection(connectionId = lockedNotEntitledId)
        .withSourceId(sourceId2)
        .withDestinationId(destId2)
        .withStatus(StandardSync.Status.LOCKED)

    // Active connection
    val active =
      createMockConnection(connectionId = activeId)
        .withSourceId(sourceId3)
        .withDestinationId(destId3)
        .withStatus(StandardSync.Status.ACTIVE)

    val sourceDefinition1 = mock<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition1 = mock<io.airbyte.config.StandardDestinationDefinition>()
    val sourceDefinition2 = mock<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition2 = mock<io.airbyte.config.StandardDestinationDefinition>()

    val sourceEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + sourceDefinitionId)!!

    whenever(connectionService.listConnectionIdsForOrganization(organizationId.value))
      .thenReturn(listOf(lockedEntitledId, lockedNotEntitledId, activeId))
    whenever(entitlementHelper.findSubHourSyncIds(organizationId)).thenReturn(emptyList())

    whenever(connectionService.getStandardSync(lockedEntitledId)).thenReturn(lockedEntitled)
    whenever(sourceService.getSourceDefinitionFromSource(sourceId1)).thenReturn(sourceDefinition1)
    whenever(sourceDefinition1.sourceDefinitionId).thenReturn(unknownSourceDefId1)
    whenever(destinationService.getDestinationDefinitionFromDestination(destId1)).thenReturn(destDefinition1)
    whenever(destDefinition1.destinationDefinitionId).thenReturn(unknownDestDefId1)

    whenever(connectionService.getStandardSync(lockedNotEntitledId)).thenReturn(lockedNotEntitled)
    whenever(sourceService.getSourceDefinitionFromSource(sourceId2)).thenReturn(sourceDefinition2)
    whenever(sourceDefinition2.sourceDefinitionId).thenReturn(sourceDefinitionId)
    whenever(destinationService.getDestinationDefinitionFromDestination(destId2)).thenReturn(destDefinition2)
    whenever(destDefinition2.destinationDefinitionId).thenReturn(destinationDefinitionId)
    whenever(entitlementService.checkEntitlement(eq(organizationId), any<Entitlement>())).thenReturn(
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
      ),
    )

    whenever(connectionService.getStandardSync(activeId)).thenReturn(active)

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    // Should only unlock the entitled locked connection
    org.mockito.kotlin.verify(connectionService).updateConnectionStatus(
      lockedEntitledId,
      StandardSync.Status.INACTIVE,
      null,
    )
    org.mockito.kotlin.verify(connectionService, org.mockito.kotlin.never()).updateConnectionStatus(
      eq(lockedNotEntitledId),
      any(),
      any(),
    )
    org.mockito.kotlin.verify(connectionService, org.mockito.kotlin.never()).updateConnectionStatus(
      eq(activeId),
      any(),
      any(),
    )
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization handles exceptions gracefully`() {
    val connection1Id = UUID.randomUUID()
    val connection2Id = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destId = UUID.randomUUID()

    val connection2 =
      createMockConnection(connectionId = connection2Id)
        .withSourceId(sourceId)
        .withDestinationId(destId)
        .withStatus(StandardSync.Status.LOCKED)

    val sourceDefinition = mock<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mock<io.airbyte.config.StandardDestinationDefinition>()
    val unknownSourceDefId = UUID.randomUUID()
    val unknownDestDefId = UUID.randomUUID()

    whenever(connectionService.listConnectionIdsForOrganization(organizationId.value))
      .thenReturn(listOf(connection1Id, connection2Id))
    whenever(entitlementHelper.findSubHourSyncIds(organizationId)).thenReturn(emptyList())

    // First connection throws exception
    whenever(connectionService.getStandardSync(connection1Id))
      .thenThrow(RuntimeException("Database error"))

    // Second connection processes successfully
    whenever(connectionService.getStandardSync(connection2Id)).thenReturn(connection2)
    whenever(sourceService.getSourceDefinitionFromSource(sourceId)).thenReturn(sourceDefinition)
    whenever(sourceDefinition.sourceDefinitionId).thenReturn(unknownSourceDefId)
    whenever(destinationService.getDestinationDefinitionFromDestination(destId)).thenReturn(destDefinition)
    whenever(destDefinition.destinationDefinitionId).thenReturn(unknownDestDefId)

    // Should not throw and should process connection2
    assertDoesNotThrow {
      connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)
    }

    org.mockito.kotlin.verify(connectionService).updateConnectionStatus(
      connection2Id,
      StandardSync.Status.INACTIVE,
      null,
    )
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization checks sub-hour sync entitlement`() {
    val lockedConnectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destId = UUID.randomUUID()
    val unknownSourceDefId = UUID.randomUUID()
    val unknownDestDefId = UUID.randomUUID()

    val lockedConnection =
      createMockConnection(connectionId = lockedConnectionId, cronExpression = "0 */30 * * * ?")
        .withSourceId(sourceId)
        .withDestinationId(destId)
        .withStatus(StandardSync.Status.LOCKED)

    val sourceDefinition = mock<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mock<io.airbyte.config.StandardDestinationDefinition>()

    whenever(connectionService.listConnectionIdsForOrganization(organizationId.value))
      .thenReturn(listOf(lockedConnectionId))
    whenever(entitlementHelper.findSubHourSyncIds(organizationId)).thenReturn(listOf(lockedConnectionId))
    whenever(connectionService.getStandardSync(lockedConnectionId)).thenReturn(lockedConnection)
    whenever(sourceService.getSourceDefinitionFromSource(sourceId)).thenReturn(sourceDefinition)
    whenever(sourceDefinition.sourceDefinitionId).thenReturn(unknownSourceDefId)
    whenever(destinationService.getDestinationDefinitionFromDestination(destId)).thenReturn(destDefinition)
    whenever(destDefinition.destinationDefinitionId).thenReturn(unknownDestDefId)

    // Not entitled to faster sync frequency
    whenever(entitlementService.checkEntitlement(organizationId, FasterSyncFrequencyEntitlement)).thenReturn(
      EntitlementResult(
        featureId = FasterSyncFrequencyEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
      ),
    )

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    // Should not unlock because not entitled to faster sync
    org.mockito.kotlin.verify(connectionService, org.mockito.kotlin.never()).updateConnectionStatus(
      any(),
      any(),
      any(),
    )
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization checks mapper entitlement`() {
    val lockedConnectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destId = UUID.randomUUID()
    val unknownSourceDefId = UUID.randomUUID()
    val unknownDestDefId = UUID.randomUUID()

    val lockedConnection =
      createMockConnection(connectionId = lockedConnectionId, withMappers = true)
        .withSourceId(sourceId)
        .withDestinationId(destId)
        .withStatus(StandardSync.Status.LOCKED)

    val sourceDefinition = mock<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mock<io.airbyte.config.StandardDestinationDefinition>()

    whenever(connectionService.listConnectionIdsForOrganization(organizationId.value))
      .thenReturn(listOf(lockedConnectionId))
    whenever(entitlementHelper.findSubHourSyncIds(organizationId)).thenReturn(emptyList())
    whenever(connectionService.getStandardSync(lockedConnectionId)).thenReturn(lockedConnection)
    whenever(sourceService.getSourceDefinitionFromSource(sourceId)).thenReturn(sourceDefinition)
    whenever(sourceDefinition.sourceDefinitionId).thenReturn(unknownSourceDefId)
    whenever(destinationService.getDestinationDefinitionFromDestination(destId)).thenReturn(destDefinition)
    whenever(destDefinition.destinationDefinitionId).thenReturn(unknownDestDefId)

    // Not entitled to mappers
    whenever(entitlementService.checkEntitlement(organizationId, MappersEntitlement)).thenReturn(
      EntitlementResult(
        featureId = MappersEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
      ),
    )

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    // Should not unlock because not entitled to mappers
    org.mockito.kotlin.verify(connectionService, org.mockito.kotlin.never()).updateConnectionStatus(
      any(),
      any(),
      any(),
    )
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization unlocks connection with mappers when entitled`() {
    val lockedConnectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destId = UUID.randomUUID()
    val unknownSourceDefId = UUID.randomUUID()
    val unknownDestDefId = UUID.randomUUID()

    val lockedConnection =
      createMockConnection(connectionId = lockedConnectionId, withMappers = true)
        .withSourceId(sourceId)
        .withDestinationId(destId)
        .withStatus(StandardSync.Status.LOCKED)

    val sourceDefinition = mock<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mock<io.airbyte.config.StandardDestinationDefinition>()

    whenever(connectionService.listConnectionIdsForOrganization(organizationId.value))
      .thenReturn(listOf(lockedConnectionId))
    whenever(entitlementHelper.findSubHourSyncIds(organizationId)).thenReturn(emptyList())
    whenever(connectionService.getStandardSync(lockedConnectionId)).thenReturn(lockedConnection)
    whenever(sourceService.getSourceDefinitionFromSource(sourceId)).thenReturn(sourceDefinition)
    whenever(sourceDefinition.sourceDefinitionId).thenReturn(unknownSourceDefId)
    whenever(destinationService.getDestinationDefinitionFromDestination(destId)).thenReturn(destDefinition)
    whenever(destDefinition.destinationDefinitionId).thenReturn(unknownDestDefId)

    // Entitled to mappers
    whenever(entitlementService.checkEntitlement(organizationId, MappersEntitlement)).thenReturn(
      EntitlementResult(
        featureId = MappersEntitlement.featureId,
        isEntitled = true,
        reason = null,
      ),
    )

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    // Should unlock because entitled to mappers
    org.mockito.kotlin.verify(connectionService).updateConnectionStatus(
      lockedConnectionId,
      StandardSync.Status.INACTIVE,
      null,
    )
  }
}
