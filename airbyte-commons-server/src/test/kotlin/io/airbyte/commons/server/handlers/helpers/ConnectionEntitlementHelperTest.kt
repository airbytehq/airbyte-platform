/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.commons.entitlements.EntitlementHelper
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.AdvancedMappersEntitlement
import io.airbyte.commons.entitlements.models.ConnectorEntitlement
import io.airbyte.commons.entitlements.models.DestinationSalesforceEnterpriseConnector
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.entitlements.models.FasterSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.FifteenMinuteSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.MappersEntitlement
import io.airbyte.commons.entitlements.models.SourceWorkdayEnterpriseConnector
import io.airbyte.commons.server.helpers.ConnectionHelpers
import io.airbyte.config.BasicSchedule
import io.airbyte.config.Cron
import io.airbyte.config.MapperConfig
import io.airbyte.config.MapperOperationName
import io.airbyte.config.Schedule
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync
import io.airbyte.config.helpers.CronExpressionHelper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.models.OrganizationId
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
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
    entitlementService = mockk()
    connectionService = mockk()
    sourceService = mockk()
    destinationService = mockk()
    cronExpressionHelper = CronExpressionHelper()
    entitlementHelper = mockk()
    every { connectionService.writeStandardSync(any()) } just Runs
    every { connectionService.updateConnectionStatus(any(), any(), any()) } just Runs
    every { entitlementService.checkEntitlement(any(), any()) } returns
      EntitlementResult(
        featureId = "unknown-feature",
        isEntitled = false,
        reason = null,
      )
    connectionEntitlementHelper =
      ConnectionEntitlementHelper(connectionService, cronExpressionHelper, entitlementService, sourceService, destinationService, entitlementHelper)
  }

  private fun createMockConnection(
    connectionId: UUID = this.connectionId,
    cronExpression: String = "0 0 * * * ?", // Default: hourly
    withMappers: Boolean = false,
    mapperName: String = "hashing",
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
                override fun name(): String = mapperName

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

    every { entitlementService.checkEntitlement(organizationId, sourceEntitlement) } returns
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
      )
    every { entitlementService.checkEntitlement(organizationId, destEntitlement) } returns
      EntitlementResult(
        featureId = destEntitlement.featureId,
        isEntitled = true,
        reason = null,
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

    every { entitlementService.checkEntitlement(organizationId, sourceEntitlement) } returns
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = true,
        reason = null,
      )
    every { entitlementService.checkEntitlement(organizationId, destEntitlement) } returns
      EntitlementResult(
        featureId = destEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
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

    every { entitlementService.checkEntitlement(organizationId, sourceEntitlement) } returns
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = true,
        reason = null,
      )
    every { entitlementService.checkEntitlement(organizationId, destEntitlement) } returns
      EntitlementResult(
        featureId = destEntitlement.featureId,
        isEntitled = true,
        reason = null,
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

    every { entitlementService.checkEntitlement(organizationId, sourceEntitlement) } returns
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = true,
        reason = null,
      )
    every { entitlementService.checkEntitlement(organizationId, destEntitlement) } returns
      EntitlementResult(
        featureId = destEntitlement.featureId,
        isEntitled = false,
        reason = null,
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

    every { entitlementService.checkEntitlement(organizationId, sourceEntitlement) } returns
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = false,
        reason = null,
      )
    every { entitlementService.checkEntitlement(organizationId, destEntitlement) } returns
      EntitlementResult(
        featureId = destEntitlement.featureId,
        isEntitled = true,
        reason = null,
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
    every { entitlementService.checkEntitlement(organizationId, MappersEntitlement) } returns
      EntitlementResult(
        featureId = MappersEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled to mappers",
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
  fun `isEntitledToConnection returns true when field renaming mapper and entitled to mappers but not advanced mappers`() {
    val connection = createMockConnection(withMappers = true, mapperName = MapperOperationName.FIELD_RENAMING)

    every { entitlementService.checkEntitlement(organizationId, MappersEntitlement) } returns
      EntitlementResult(
        featureId = MappersEntitlement.featureId,
        isEntitled = true,
        reason = null,
      )

    val result =
      connectionEntitlementHelper.isEntitledToConnection(
        connection = connection,
        subHourSyncIds = emptyList(),
        sourceDefinitionId = UUID.randomUUID(),
        destinationDefinitionId = UUID.randomUUID(),
        organizationId = organizationId,
      )

    assertTrue(result)
  }

  @Test
  fun `isEntitledToConnection returns false when field renaming mapper but not entitled to mappers`() {
    val connection = createMockConnection(withMappers = true, mapperName = MapperOperationName.FIELD_RENAMING)

    every { entitlementService.checkEntitlement(organizationId, MappersEntitlement) } returns
      EntitlementResult(
        featureId = MappersEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled to mappers",
      )
    every { entitlementService.checkEntitlement(organizationId, AdvancedMappersEntitlement) } returns
      EntitlementResult(
        featureId = AdvancedMappersEntitlement.featureId,
        isEntitled = true,
        reason = null,
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
  fun `isEntitledToConnection returns false when advanced mapper and not entitled to advanced mappers`() {
    val connection = createMockConnection(withMappers = true, mapperName = MapperOperationName.HASHING)

    every { entitlementService.checkEntitlement(organizationId, MappersEntitlement) } returns
      EntitlementResult(
        featureId = MappersEntitlement.featureId,
        isEntitled = true,
        reason = null,
      )
    every { entitlementService.checkEntitlement(organizationId, AdvancedMappersEntitlement) } returns
      EntitlementResult(
        featureId = AdvancedMappersEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled to advanced mappers",
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
  fun `isEntitledToConnection returns true when advanced mapper and entitled to both mappers entitlements`() {
    val connection = createMockConnection(withMappers = true, mapperName = MapperOperationName.HASHING)

    every { entitlementService.checkEntitlement(organizationId, MappersEntitlement) } returns
      EntitlementResult(
        featureId = MappersEntitlement.featureId,
        isEntitled = true,
        reason = null,
      )
    every { entitlementService.checkEntitlement(organizationId, AdvancedMappersEntitlement) } returns
      EntitlementResult(
        featureId = AdvancedMappersEntitlement.featureId,
        isEntitled = true,
        reason = null,
      )

    val result =
      connectionEntitlementHelper.isEntitledToConnection(
        connection = connection,
        subHourSyncIds = emptyList(),
        sourceDefinitionId = UUID.randomUUID(),
        destinationDefinitionId = UUID.randomUUID(),
        organizationId = organizationId,
      )

    assertTrue(result)
  }

  @Test
  fun `isEntitledToConnection returns false when sub-hourly sync but not entitled`() {
    val connection = createMockConnection(cronExpression = "0 */30 * * * ?")

    // Entitled to connectors but not faster sync
    every { entitlementService.checkEntitlement(organizationId, FasterSyncFrequencyEntitlement) } returns
      EntitlementResult(
        featureId = FasterSyncFrequencyEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled to faster sync frequency",
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
  fun `isEntitledToConnection returns true when fifteen-minute entitlement covers sub-hour sync`() {
    val connection = createMockConnection(cronExpression = "0 */30 * * * ?")

    every { entitlementService.checkEntitlement(organizationId, FifteenMinuteSyncFrequencyEntitlement) } returns
      EntitlementResult(
        featureId = FifteenMinuteSyncFrequencyEntitlement.featureId,
        isEntitled = true,
        reason = null,
      )

    val result =
      connectionEntitlementHelper.isEntitledToConnection(
        connection = connection,
        subHourSyncIds = listOf(connectionId),
        sourceDefinitionId = UUID.randomUUID(),
        destinationDefinitionId = UUID.randomUUID(),
        organizationId = organizationId,
      )

    assertTrue(result)
  }

  @Test
  fun `isEntitledToConnection returns false when sync is faster than fifteen minutes and only plus entitlement exists`() {
    val connection = createMockConnection(cronExpression = "0 */5 * * * ?")

    every { entitlementService.checkEntitlement(organizationId, FifteenMinuteSyncFrequencyEntitlement) } returns
      EntitlementResult(
        featureId = FifteenMinuteSyncFrequencyEntitlement.featureId,
        isEntitled = true,
        reason = null,
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

    val sourceDefinition = mockk<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mockk<io.airbyte.config.StandardDestinationDefinition>()

    every { connectionService.listConnectionIdsForOrganization(organizationId.value) } returns listOf(lockedConnectionId)
    every { entitlementHelper.findSubHourSyncIds(organizationId) } returns emptyList()
    every { connectionService.getStandardSync(lockedConnectionId) } returns lockedConnection
    every { sourceService.getSourceDefinitionFromSource(sourceId) } returns sourceDefinition
    every { sourceDefinition.sourceDefinitionId } returns unknownSourceDefId
    every { destinationService.getDestinationDefinitionFromDestination(destId) } returns destDefinition
    every { destDefinition.destinationDefinitionId } returns unknownDestDefId

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    verify {
      connectionService.updateConnectionStatus(
        lockedConnectionId,
        StandardSync.Status.INACTIVE,
        null,
      )
    }
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization skips sync frequency entitlement checks when no sub-hour syncs exist`() {
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

    val sourceDefinition = mockk<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mockk<io.airbyte.config.StandardDestinationDefinition>()

    every { connectionService.listConnectionIdsForOrganization(organizationId.value) } returns listOf(lockedConnectionId)
    every { entitlementHelper.findSubHourSyncIds(organizationId) } returns emptyList()
    every { connectionService.getStandardSync(lockedConnectionId) } returns lockedConnection
    every { sourceService.getSourceDefinitionFromSource(sourceId) } returns sourceDefinition
    every { sourceDefinition.sourceDefinitionId } returns unknownSourceDefId
    every { destinationService.getDestinationDefinitionFromDestination(destId) } returns destDefinition
    every { destDefinition.destinationDefinitionId } returns unknownDestDefId

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    verify(exactly = 0) { entitlementService.checkEntitlement(organizationId, FasterSyncFrequencyEntitlement) }
    verify(exactly = 0) { entitlementService.checkEntitlement(organizationId, FifteenMinuteSyncFrequencyEntitlement) }
    verify {
      connectionService.updateConnectionStatus(
        lockedConnectionId,
        StandardSync.Status.INACTIVE,
        null,
      )
    }
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

    val sourceDefinition = mockk<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mockk<io.airbyte.config.StandardDestinationDefinition>()
    val sourceEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + sourceDefinitionId)!!

    every { connectionService.listConnectionIdsForOrganization(organizationId.value) } returns listOf(lockedConnectionId)
    every { entitlementHelper.findSubHourSyncIds(organizationId) } returns emptyList()
    every { connectionService.getStandardSync(lockedConnectionId) } returns lockedConnection
    every { sourceService.getSourceDefinitionFromSource(sourceId) } returns sourceDefinition
    every { sourceDefinition.sourceDefinitionId } returns sourceDefinitionId
    every { destinationService.getDestinationDefinitionFromDestination(destId) } returns destDefinition
    every { destDefinition.destinationDefinitionId } returns destinationDefinitionId
    every { entitlementService.checkEntitlement(organizationId, any<Entitlement>()) } returns
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
      )

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    verify(exactly = 0) {
      connectionService.updateConnectionStatus(
        any(),
        any(),
        any(),
      )
    }
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization skips non-locked connections`() {
    val activeConnectionId = UUID.randomUUID()
    val activeConnection =
      createMockConnection(connectionId = activeConnectionId)
        .withStatus(StandardSync.Status.ACTIVE)

    every { connectionService.listConnectionIdsForOrganization(organizationId.value) } returns listOf(activeConnectionId)
    every { entitlementHelper.findSubHourSyncIds(organizationId) } returns emptyList()
    every { connectionService.getStandardSync(activeConnectionId) } returns activeConnection

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    verify(exactly = 0) {
      connectionService.updateConnectionStatus(
        any(),
        any(),
        any(),
      )
    }
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

    val sourceDefinition1 = mockk<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition1 = mockk<io.airbyte.config.StandardDestinationDefinition>()
    val sourceDefinition2 = mockk<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition2 = mockk<io.airbyte.config.StandardDestinationDefinition>()

    val sourceEntitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + sourceDefinitionId)!!

    every { connectionService.listConnectionIdsForOrganization(organizationId.value) } returns listOf(lockedEntitledId, lockedNotEntitledId, activeId)
    every { entitlementHelper.findSubHourSyncIds(organizationId) } returns emptyList()

    every { connectionService.getStandardSync(lockedEntitledId) } returns lockedEntitled
    every { sourceService.getSourceDefinitionFromSource(sourceId1) } returns sourceDefinition1
    every { sourceDefinition1.sourceDefinitionId } returns unknownSourceDefId1
    every { destinationService.getDestinationDefinitionFromDestination(destId1) } returns destDefinition1
    every { destDefinition1.destinationDefinitionId } returns unknownDestDefId1

    every { connectionService.getStandardSync(lockedNotEntitledId) } returns lockedNotEntitled
    every { sourceService.getSourceDefinitionFromSource(sourceId2) } returns sourceDefinition2
    every { sourceDefinition2.sourceDefinitionId } returns sourceDefinitionId
    every { destinationService.getDestinationDefinitionFromDestination(destId2) } returns destDefinition2
    every { destDefinition2.destinationDefinitionId } returns destinationDefinitionId
    every { entitlementService.checkEntitlement(organizationId, any<Entitlement>()) } returns
      EntitlementResult(
        featureId = sourceEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
      )

    every { connectionService.getStandardSync(activeId) } returns active

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    // Should only unlock the entitled locked connection
    verify {
      connectionService.updateConnectionStatus(
        lockedEntitledId,
        StandardSync.Status.INACTIVE,
        null,
      )
    }
    verify(exactly = 0) {
      connectionService.updateConnectionStatus(
        lockedNotEntitledId,
        any(),
        any(),
      )
    }
    verify(exactly = 0) {
      connectionService.updateConnectionStatus(
        activeId,
        any(),
        any(),
      )
    }
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

    val sourceDefinition = mockk<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mockk<io.airbyte.config.StandardDestinationDefinition>()
    val unknownSourceDefId = UUID.randomUUID()
    val unknownDestDefId = UUID.randomUUID()

    every { connectionService.listConnectionIdsForOrganization(organizationId.value) } returns listOf(connection1Id, connection2Id)
    every { entitlementHelper.findSubHourSyncIds(organizationId) } returns emptyList()

    // First connection throws exception
    every { connectionService.getStandardSync(connection1Id) } throws RuntimeException("Database error")

    // Second connection processes successfully
    every { connectionService.getStandardSync(connection2Id) } returns connection2
    every { sourceService.getSourceDefinitionFromSource(sourceId) } returns sourceDefinition
    every { sourceDefinition.sourceDefinitionId } returns unknownSourceDefId
    every { destinationService.getDestinationDefinitionFromDestination(destId) } returns destDefinition
    every { destDefinition.destinationDefinitionId } returns unknownDestDefId

    // Should not throw and should process connection2
    assertDoesNotThrow {
      connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)
    }

    verify {
      connectionService.updateConnectionStatus(
        connection2Id,
        StandardSync.Status.INACTIVE,
        null,
      )
    }
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization does not unlock faster-than-fifteen locked connection with only fifteen-minute entitlement`() {
    val lockedConnectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destId = UUID.randomUUID()
    val unknownSourceDefId = UUID.randomUUID()
    val unknownDestDefId = UUID.randomUUID()

    val lockedConnection =
      createMockConnection(connectionId = lockedConnectionId, cronExpression = "0 */5 * * * ?")
        .withSourceId(sourceId)
        .withDestinationId(destId)
        .withStatus(StandardSync.Status.LOCKED)

    val sourceDefinition = mockk<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mockk<io.airbyte.config.StandardDestinationDefinition>()

    every { connectionService.listConnectionIdsForOrganization(organizationId.value) } returns listOf(lockedConnectionId)
    every { entitlementHelper.findSubHourSyncIds(organizationId) } returns listOf(lockedConnectionId)
    every { connectionService.getStandardSync(lockedConnectionId) } returns lockedConnection
    every { sourceService.getSourceDefinitionFromSource(sourceId) } returns sourceDefinition
    every { sourceDefinition.sourceDefinitionId } returns unknownSourceDefId
    every { destinationService.getDestinationDefinitionFromDestination(destId) } returns destDefinition
    every { destDefinition.destinationDefinitionId } returns unknownDestDefId

    every { entitlementService.checkEntitlement(organizationId, FasterSyncFrequencyEntitlement) } returns
      EntitlementResult(
        featureId = FasterSyncFrequencyEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
      )
    every { entitlementService.checkEntitlement(organizationId, FifteenMinuteSyncFrequencyEntitlement) } returns
      EntitlementResult(
        featureId = FifteenMinuteSyncFrequencyEntitlement.featureId,
        isEntitled = true,
        reason = null,
      )

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    verify(exactly = 0) { connectionService.writeStandardSync(any()) }
    verify(exactly = 0) {
      connectionService.updateConnectionStatus(
        any(),
        any(),
        any(),
      )
    }
  }

  @Test
  fun `unlockEntitledConnectionsForOrganization downgrades frequency-only locked connection to hourly when no longer entitled`() {
    val lockedConnectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destId = UUID.randomUUID()
    val unknownSourceDefId = UUID.randomUUID()
    val unknownDestDefId = UUID.randomUUID()

    val lockedConnection =
      createBasicScheduleConnection(connectionId = lockedConnectionId, minutes = 30L)
        .withSourceId(sourceId)
        .withDestinationId(destId)
        .withStatus(StandardSync.Status.LOCKED)

    val sourceDefinition = mockk<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mockk<io.airbyte.config.StandardDestinationDefinition>()

    every { connectionService.listConnectionIdsForOrganization(organizationId.value) } returns listOf(lockedConnectionId)
    every { entitlementHelper.findSubHourSyncIds(organizationId) } returns listOf(lockedConnectionId)
    every { connectionService.getStandardSync(lockedConnectionId) } returns lockedConnection
    every { sourceService.getSourceDefinitionFromSource(sourceId) } returns sourceDefinition
    every { sourceDefinition.sourceDefinitionId } returns unknownSourceDefId
    every { destinationService.getDestinationDefinitionFromDestination(destId) } returns destDefinition
    every { destDefinition.destinationDefinitionId } returns unknownDestDefId

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    val writtenConnection = slot<StandardSync>()
    verify { connectionService.writeStandardSync(capture(writtenConnection)) }
    assertHourlySchedule(writtenConnection.captured, lockedConnectionId)
    verify {
      connectionService.updateConnectionStatus(
        lockedConnectionId,
        StandardSync.Status.INACTIVE,
        null,
      )
    }
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

    val sourceDefinition = mockk<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mockk<io.airbyte.config.StandardDestinationDefinition>()

    every { connectionService.listConnectionIdsForOrganization(organizationId.value) } returns listOf(lockedConnectionId)
    every { entitlementHelper.findSubHourSyncIds(organizationId) } returns emptyList()
    every { connectionService.getStandardSync(lockedConnectionId) } returns lockedConnection
    every { sourceService.getSourceDefinitionFromSource(sourceId) } returns sourceDefinition
    every { sourceDefinition.sourceDefinitionId } returns unknownSourceDefId
    every { destinationService.getDestinationDefinitionFromDestination(destId) } returns destDefinition
    every { destDefinition.destinationDefinitionId } returns unknownDestDefId

    // Not entitled to mappers
    every { entitlementService.checkEntitlement(organizationId, MappersEntitlement) } returns
      EntitlementResult(
        featureId = MappersEntitlement.featureId,
        isEntitled = false,
        reason = "Not entitled",
      )

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    // Should not unlock because not entitled to mappers
    verify(exactly = 0) {
      connectionService.updateConnectionStatus(
        any(),
        any(),
        any(),
      )
    }
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

    val sourceDefinition = mockk<io.airbyte.config.StandardSourceDefinition>()
    val destDefinition = mockk<io.airbyte.config.StandardDestinationDefinition>()

    every { connectionService.listConnectionIdsForOrganization(organizationId.value) } returns listOf(lockedConnectionId)
    every { entitlementHelper.findSubHourSyncIds(organizationId) } returns emptyList()
    every { connectionService.getStandardSync(lockedConnectionId) } returns lockedConnection
    every { sourceService.getSourceDefinitionFromSource(sourceId) } returns sourceDefinition
    every { sourceDefinition.sourceDefinitionId } returns unknownSourceDefId
    every { destinationService.getDestinationDefinitionFromDestination(destId) } returns destDefinition
    every { destDefinition.destinationDefinitionId } returns unknownDestDefId

    // Entitled to mappers
    every { entitlementService.checkEntitlement(organizationId, MappersEntitlement) } returns
      EntitlementResult(
        featureId = MappersEntitlement.featureId,
        isEntitled = true,
        reason = null,
      )
    every { entitlementService.checkEntitlement(organizationId, AdvancedMappersEntitlement) } returns
      EntitlementResult(
        featureId = AdvancedMappersEntitlement.featureId,
        isEntitled = true,
        reason = null,
      )

    connectionEntitlementHelper.unlockEntitledConnectionsForOrganization(organizationId)

    // Should unlock because entitled to mappers
    verify {
      connectionService.updateConnectionStatus(
        lockedConnectionId,
        StandardSync.Status.INACTIVE,
        null,
      )
    }
  }

  private fun createBasicScheduleConnection(
    connectionId: UUID,
    minutes: Long,
  ): StandardSync =
    createMockConnection(connectionId = connectionId)
      .withScheduleType(StandardSync.ScheduleType.BASIC_SCHEDULE)
      .withScheduleData(ScheduleData().withBasicSchedule(BasicSchedule().withTimeUnit(BasicSchedule.TimeUnit.MINUTES).withUnits(minutes)))
      .withSchedule(Schedule().withTimeUnit(Schedule.TimeUnit.MINUTES).withUnits(minutes))
      .withManual(false)

  private fun assertHourlySchedule(
    connection: StandardSync,
    connectionId: UUID,
  ) {
    assertTrue(connection.connectionId == connectionId)
    assertTrue(connection.scheduleType == StandardSync.ScheduleType.BASIC_SCHEDULE)
    assertTrue(connection.scheduleData.basicSchedule.timeUnit == BasicSchedule.TimeUnit.HOURS)
    assertTrue(connection.scheduleData.basicSchedule.units == 1L)
    assertTrue(connection.schedule.timeUnit == Schedule.TimeUnit.HOURS)
    assertTrue(connection.schedule.units == 1L)
    assertFalse(connection.manual)
  }
}
