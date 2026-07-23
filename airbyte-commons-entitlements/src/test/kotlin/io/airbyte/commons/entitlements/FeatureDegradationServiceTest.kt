/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.DestinationSalesforceEnterpriseConnector
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.FasterSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.FifteenMinuteSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.MappersEntitlement
import io.airbyte.commons.entitlements.models.RbacRolesEntitlement
import io.airbyte.commons.entitlements.models.SourceNetsuiteEnterpriseConnector
import io.airbyte.commons.entitlements.models.SourceOracleEnterpriseConnector
import io.airbyte.config.ActorType
import io.airbyte.config.BasicSchedule
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.Cron
import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_ADMIN
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_EDITOR
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_MEMBER
import io.airbyte.config.Permission.PermissionType.WORKSPACE_ADMIN
import io.airbyte.config.Permission.PermissionType.WORKSPACE_EDITOR
import io.airbyte.config.Permission.PermissionType.WORKSPACE_READER
import io.airbyte.config.Schedule
import io.airbyte.config.ScheduleData
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.StatusReason
import io.airbyte.config.UserInvitation
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.UserInvitationService
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.mockk.Called
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.UUID

class FeatureDegradationServiceTest {
  private val permissionService = mockk<PermissionService>()
  private val entitlementClient = mockk<EntitlementClient>()
  private val connectionService = mockk<ConnectionService>(relaxed = true)
  private val workspacePersistence = mockk<WorkspacePersistence>()
  private val connectionEntitlementHelper = mockk<EntitlementHelper>()
  private val userInvitationService = mockk<UserInvitationService>()
  private val featureDegradationService =
    FeatureDegradationService(
      permissionService,
      entitlementClient,
      connectionService,
      workspacePersistence,
      userInvitationService,
      connectionEntitlementHelper,
    )
  private val featureDegradationServiceStubbed = spyk(featureDegradationService)

  private val cronTimezoneUtc = "UTC"

  @Test
  fun `downgradeFeaturesIfRequired executes non-schedule downgrades when going from UNIFIED_TRIAL to STANDARD`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val subHourConnectionId = UUID.randomUUID()
    val mapperConnectionId = UUID.randomUUID()
    val subHourConnection = basicMinutesConnection(subHourConnectionId, 30L)

    // Even for upgrades, the function is called (it just checks if it needs to do anything)
    every { entitlementClient.getEntitlements(orgId) } returns
      listOf(
        EntitlementResult(RbacRolesEntitlement.featureId, true),
        EntitlementResult(MappersEntitlement.featureId, true),
        EntitlementResult(FasterSyncFrequencyEntitlement.featureId, true),
        EntitlementResult(SourceOracleEnterpriseConnector.featureId, true),
        EntitlementResult(DestinationSalesforceEnterpriseConnector.featureId, true),
      )
    every { entitlementClient.getEntitlementsForPlan(EntitlementPlan.STANDARD) } returns emptyList()
    every { entitlementClient.updateOrganization(orgId, EntitlementPlan.STANDARD) } just Runs
    every { featureDegradationServiceStubbed.downgradeRBAC(orgId) } just Runs
    every { connectionService.listConnectionIdsForOrganizationWithMappers(orgId.value) } returns listOf(mapperConnectionId)
    every { connectionEntitlementHelper.findSubHourSyncIds(orgId) } returns listOf(subHourConnectionId)
    every { connectionService.getStandardSync(subHourConnectionId) } returns subHourConnection
    every { connectionService.writeStandardSync(any()) } just Runs
    every {
      connectionService.listConnectionIdsForOrganizationAndActorDefinitions(
        orgId.value,
        any(),
        ActorType.SOURCE,
      )
    } returns emptyList()
    every {
      connectionService.listConnectionIdsForOrganizationAndActorDefinitions(
        orgId.value,
        any(),
        ActorType.DESTINATION,
      )
    } returns emptyList()
    every { connectionService.lockConnectionsById(any(), any()) } returns setOf(mapperConnectionId)

    featureDegradationServiceStubbed.downgradeFeaturesIfRequired(orgId, EntitlementPlan.UNIFIED_TRIAL, EntitlementPlan.STANDARD)

    // Verify downgrade side effects are applied for UNIFIED_TRIAL to STANDARD.
    verify { featureDegradationServiceStubbed.downgradeRBAC(orgId) }
    verify { connectionService.listConnectionIdsForOrganizationWithMappers(orgId.value) }
    verify {
      connectionService.listConnectionIdsForOrganizationAndActorDefinitions(
        orgId.value,
        any(),
        ActorType.SOURCE,
      )
    }
    verify {
      connectionService.listConnectionIdsForOrganizationAndActorDefinitions(
        orgId.value,
        any(),
        ActorType.DESTINATION,
      )
    }
    verify { connectionEntitlementHelper.findSubHourSyncIds(orgId) }
    verifyHourlyScheduleWrite(subHourConnectionId)
    verify {
      connectionService.lockConnectionsById(
        match { it.contains(mapperConnectionId) && !it.contains(subHourConnectionId) },
        StatusReason.SUBSCRIPTION_DOWNGRADED_ACCESS_REVOKED.value,
      )
    }
  }

  @Test
  fun `downgradeFeaturesIfRequired does not execute any downgrades when going from PRO to STANDARD`() {
    val orgId = OrganizationId(UUID.randomUUID())

    // Even for upgrades, the function is called (it just checks if it needs to do anything)
    every { entitlementClient.getEntitlements(orgId) } returns listOf(EntitlementResult("feature-rbac-roles", true))
    every { entitlementClient.getEntitlementsForPlan(EntitlementPlan.STANDARD) } returns emptyList()
    every { entitlementClient.updateOrganization(orgId, EntitlementPlan.STANDARD) } just runs
    every { featureDegradationServiceStubbed.downgradeRBAC(orgId) } just runs

    featureDegradationServiceStubbed.downgradeFeaturesIfRequired(orgId, EntitlementPlan.PRO, EntitlementPlan.STANDARD)

    // Verify downgrade RBAC function is not called if degradation is not supported.
    verify(exactly = 0) { featureDegradationServiceStubbed.downgradeRBAC(orgId) }
    verify { connectionService wasNot Called }
  }

  @Test
  fun `downgradeFeaturesIfRequired finds all connections to downgrade and passes them to the connection service`() {
    val orgId = OrganizationId(UUID.randomUUID())

    // Even for upgrades, the function is called (it just checks if it needs to do anything)
    every { entitlementClient.getEntitlements(orgId) } returns
      listOf(
        EntitlementResult(RbacRolesEntitlement.featureId, true),
        EntitlementResult(MappersEntitlement.featureId, true),
        EntitlementResult(FasterSyncFrequencyEntitlement.featureId, true),
        EntitlementResult(DestinationSalesforceEnterpriseConnector.featureId, true),
        EntitlementResult(SourceNetsuiteEnterpriseConnector.featureId, true),
        EntitlementResult(SourceOracleEnterpriseConnector.featureId, true),
      )
    every { entitlementClient.getEntitlementsForPlan(EntitlementPlan.STANDARD) } returns emptyList()
    every { entitlementClient.updateOrganization(orgId, EntitlementPlan.STANDARD) } just runs
    every { featureDegradationServiceStubbed.downgradeRBAC(orgId) } just runs

    val connectionIds = listOf(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
    every { connectionService.listConnectionIdsForOrganizationWithMappers(orgId.value) } returns listOf(connectionIds[0], connectionIds[1])
    every { connectionEntitlementHelper.findSubHourSyncIds(orgId) } returns listOf(connectionIds[1], connectionIds[2])
    every { connectionService.getStandardSync(connectionIds[1]) } returns basicMinutesConnection(connectionIds[1], 30L)
    every { connectionService.getStandardSync(connectionIds[2]) } returns basicMinutesConnection(connectionIds[2], 30L)
    every { connectionService.writeStandardSync(any()) } just Runs
    every {
      connectionService.listConnectionIdsForOrganizationAndActorDefinitions(
        orgId.value,
        match {
          it.containsAll(listOf(SourceNetsuiteEnterpriseConnector.actorDefinitionId, SourceOracleEnterpriseConnector.actorDefinitionId))
        },
        ActorType.SOURCE,
      )
    } returns listOf(connectionIds[3], connectionIds[4])
    every {
      connectionService.listConnectionIdsForOrganizationAndActorDefinitions(
        orgId.value,
        match {
          it.containsAll(listOf(DestinationSalesforceEnterpriseConnector.actorDefinitionId))
        },
        ActorType.DESTINATION,
      )
    } returns listOf(connectionIds[0], connectionIds[4])

    featureDegradationServiceStubbed.downgradeFeaturesIfRequired(orgId, EntitlementPlan.UNIFIED_TRIAL, EntitlementPlan.STANDARD)

    // Verify downgrade RBAC function is not called if not going from UNIFIED_TRIAL to STANDARD
    verify {
      connectionService.lockConnectionsById(
        match {
          it.containsAll(listOf(connectionIds[0], connectionIds[1], connectionIds[3], connectionIds[4])) &&
            !it.contains(connectionIds[2])
        },
        StatusReason.SUBSCRIPTION_DOWNGRADED_ACCESS_REVOKED.value,
      )
    }
  }

  @Test
  fun `downgradeFeaturesIfRequired downgrades sub-hour connections to hourly for fifteen-minute entitlement downgrade`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val connectionId = UUID.randomUUID()
    val subHourConnection = basicMinutesConnection(connectionId, 30L)

    every { entitlementClient.getEntitlements(orgId) } returns
      listOf(
        EntitlementResult(FifteenMinuteSyncFrequencyEntitlement.featureId, true),
      )
    every { entitlementClient.getEntitlementsForPlan(EntitlementPlan.STANDARD) } returns emptyList()
    every { connectionEntitlementHelper.findSubHourSyncIds(orgId) } returns listOf(connectionId)
    every { connectionService.getStandardSync(connectionId) } returns subHourConnection
    every { connectionService.writeStandardSync(any()) } just Runs

    featureDegradationServiceStubbed.downgradeFeaturesIfRequired(orgId, EntitlementPlan.UNIFIED_TRIAL, EntitlementPlan.STANDARD)

    verify { connectionEntitlementHelper.findSubHourSyncIds(orgId) }
    verifyHourlyScheduleWrite(connectionId)
    verify(exactly = 0) { connectionService.lockConnectionsById(any(), any()) }
  }

  @Test
  fun `downgradeFeaturesIfRequired downgrades sub-hour cron connections to basic hourly schedules`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val connectionId = UUID.randomUUID()
    val subHourConnection = cronConnection(connectionId, "0 */30 * * * ?")

    every { entitlementClient.getEntitlements(orgId) } returns
      listOf(
        EntitlementResult(FifteenMinuteSyncFrequencyEntitlement.featureId, true),
      )
    every { entitlementClient.getEntitlementsForPlan(EntitlementPlan.STANDARD) } returns emptyList()
    every { connectionEntitlementHelper.findSubHourSyncIds(orgId) } returns listOf(connectionId)
    every { connectionService.getStandardSync(connectionId) } returns subHourConnection
    every { connectionService.writeStandardSync(any()) } just Runs

    featureDegradationServiceStubbed.downgradeFeaturesIfRequired(orgId, EntitlementPlan.UNIFIED_TRIAL, EntitlementPlan.STANDARD)

    verify { connectionEntitlementHelper.findSubHourSyncIds(orgId) }
    verifyHourlyScheduleWrite(connectionId)
    verify(exactly = 0) { connectionService.lockConnectionsById(any(), any()) }
  }

  @Test
  fun `downgradeFeaturesIfRequired locks sub-hour connection when schedule downgrade write fails`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val downgradedConnectionId = UUID.randomUUID()
    val failedConnectionId = UUID.randomUUID()
    val downgradedConnection = basicMinutesConnection(downgradedConnectionId, 30L)
    val failedConnection = basicMinutesConnection(failedConnectionId, 30L)

    every { entitlementClient.getEntitlements(orgId) } returns
      listOf(
        EntitlementResult(FifteenMinuteSyncFrequencyEntitlement.featureId, true),
      )
    every { entitlementClient.getEntitlementsForPlan(EntitlementPlan.STANDARD) } returns emptyList()
    every { connectionEntitlementHelper.findSubHourSyncIds(orgId) } returns listOf(downgradedConnectionId, failedConnectionId)
    every { connectionService.getStandardSync(downgradedConnectionId) } returns downgradedConnection
    every { connectionService.getStandardSync(failedConnectionId) } returns failedConnection
    every { connectionService.writeStandardSync(match { it.connectionId == downgradedConnectionId }) } just Runs
    every { connectionService.writeStandardSync(match { it.connectionId == failedConnectionId }) } throws RuntimeException("write failed")
    every { connectionService.lockConnectionsById(any(), any()) } returns setOf(failedConnectionId)

    featureDegradationServiceStubbed.downgradeFeaturesIfRequired(orgId, EntitlementPlan.UNIFIED_TRIAL, EntitlementPlan.STANDARD)

    verifyHourlyScheduleWrite(downgradedConnectionId)
    verifyHourlyScheduleWrite(failedConnectionId)
    verify {
      connectionService.lockConnectionsById(
        match { it.contains(failedConnectionId) && !it.contains(downgradedConnectionId) },
        StatusReason.SUBSCRIPTION_DOWNGRADED_ACCESS_REVOKED.value,
      )
    }
  }

  @Test
  fun `downgradeFeaturesIfRequired locks non-schedule Plus-only connections and downgrades sub-hour schedules when going from PLUS to STANDARD`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val mapperConnectionId = UUID.randomUUID()
    val subHourConnectionId = UUID.randomUUID()
    val subHourConnection = basicMinutesConnection(subHourConnectionId, 30L)

    every { entitlementClient.getEntitlements(orgId) } returns
      listOf(
        EntitlementResult(MappersEntitlement.featureId, true),
        EntitlementResult(FifteenMinuteSyncFrequencyEntitlement.featureId, true),
      )
    every { entitlementClient.getEntitlementsForPlan(EntitlementPlan.STANDARD) } returns emptyList()
    every { connectionService.listConnectionIdsForOrganizationWithMappers(orgId.value) } returns listOf(mapperConnectionId)
    every { connectionEntitlementHelper.findSubHourSyncIds(orgId) } returns listOf(subHourConnectionId)
    every { connectionService.getStandardSync(subHourConnectionId) } returns subHourConnection
    every { connectionService.writeStandardSync(any()) } just Runs
    every { connectionService.lockConnectionsById(any(), any()) } returns setOf(mapperConnectionId)

    featureDegradationServiceStubbed.downgradeFeaturesIfRequired(orgId, EntitlementPlan.PLUS, EntitlementPlan.STANDARD)

    verify { connectionService.listConnectionIdsForOrganizationWithMappers(orgId.value) }
    verify { connectionEntitlementHelper.findSubHourSyncIds(orgId) }
    verifyHourlyScheduleWrite(subHourConnectionId)
    verify {
      connectionService.lockConnectionsById(
        match { it.contains(mapperConnectionId) && !it.contains(subHourConnectionId) },
        StatusReason.SUBSCRIPTION_DOWNGRADED_ACCESS_REVOKED.value,
      )
    }
  }

  private fun basicMinutesConnection(
    connectionId: UUID,
    minutes: Long,
  ): StandardSync =
    StandardSync()
      .withConnectionId(connectionId)
      .withCatalog(ConfiguredAirbyteCatalog().withStreams(emptyList()))
      .withStatus(StandardSync.Status.ACTIVE)
      .withScheduleType(StandardSync.ScheduleType.BASIC_SCHEDULE)
      .withScheduleData(ScheduleData().withBasicSchedule(BasicSchedule().withTimeUnit(BasicSchedule.TimeUnit.MINUTES).withUnits(minutes)))
      .withSchedule(Schedule().withTimeUnit(Schedule.TimeUnit.MINUTES).withUnits(minutes))
      .withManual(false)

  private fun cronConnection(
    connectionId: UUID,
    cronExpression: String,
  ): StandardSync =
    StandardSync()
      .withConnectionId(connectionId)
      .withCatalog(ConfiguredAirbyteCatalog().withStreams(emptyList()))
      .withStatus(StandardSync.Status.ACTIVE)
      .withScheduleType(StandardSync.ScheduleType.CRON)
      .withScheduleData(ScheduleData().withCron(Cron().withCronExpression(cronExpression).withCronTimeZone(cronTimezoneUtc)))
      .withManual(false)

  private fun verifyHourlyScheduleWrite(connectionId: UUID) {
    verify {
      connectionService.writeStandardSync(
        match {
          it.connectionId == connectionId &&
            it.scheduleType == StandardSync.ScheduleType.BASIC_SCHEDULE &&
            it.scheduleData.basicSchedule.timeUnit == BasicSchedule.TimeUnit.HOURS &&
            it.scheduleData.basicSchedule.units == 1L &&
            it.schedule.timeUnit == Schedule.TimeUnit.HOURS &&
            it.schedule.units == 1L &&
            !it.manual
        },
      )
    }
  }

  @Test
  fun downgradeRBAC() {
    val orgId = OrganizationId(UUID.randomUUID())
    val workspace = StandardWorkspace().withWorkspaceId(UUID.randomUUID()).withOrganizationId(orgId.value)

    every { workspacePersistence.getDefaultWorkspaceForOrganization(orgId.value) } returns workspace
    every { featureDegradationServiceStubbed.downgradePermissions(orgId, workspace, WORKSPACE_ADMIN) } just runs
    every { featureDegradationServiceStubbed.downgradeUserInvites(orgId, workspace, WORKSPACE_ADMIN) } just runs

    featureDegradationServiceStubbed.downgradeRBAC(orgId)
    verify { featureDegradationServiceStubbed.downgradePermissions(orgId, workspace, WORKSPACE_ADMIN) }
    verify { featureDegradationServiceStubbed.downgradeUserInvites(orgId, workspace, WORKSPACE_ADMIN) }
  }

  @Test
  fun downgradePermissions() {
    val orgId = OrganizationId(UUID.randomUUID())
    val workspace = StandardWorkspace().withWorkspaceId(UUID.randomUUID()).withOrganizationId(orgId.value)

    every { permissionService.getPermissionsByWorkspaceId(workspace.workspaceId) } returns
      listOf(
        Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
        Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_EDITOR),
        Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_READER),
      )
    every { permissionService.getPermissionsByOrganizationId(orgId.value) } returns
      listOf(
        Permission().withOrganizationId(orgId.value).withPermissionType(ORGANIZATION_ADMIN),
        Permission().withOrganizationId(orgId.value).withPermissionType(ORGANIZATION_EDITOR),
        Permission().withOrganizationId(orgId.value).withPermissionType(ORGANIZATION_MEMBER),
      )
    every { permissionService.updatePermissions(any()) } just Runs

    featureDegradationService.downgradePermissions(orgId, workspace)
    verify {
      permissionService.updatePermissions(
        listOf(
          Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
          Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
          Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
        ),
      )
    }
    verify {
      permissionService.updatePermissions(
        listOf(
          Permission().withOrganizationId(orgId.value).withPermissionType(ORGANIZATION_ADMIN),
          Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
          Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
        ),
      )
    }
  }

  @Test
  fun downgradeUserInvites() {
    val orgId = OrganizationId(UUID.randomUUID())
    val workspace = StandardWorkspace().withWorkspaceId(UUID.randomUUID()).withOrganizationId(orgId.value)

    every { userInvitationService.getPendingInvitations(ScopeType.WORKSPACE, workspace.workspaceId) } returns
      listOf(
        UserInvitation().withScopeType(ScopeType.WORKSPACE).withScopeId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
        UserInvitation().withScopeType(ScopeType.WORKSPACE).withScopeId(workspace.workspaceId).withPermissionType(WORKSPACE_EDITOR),
        UserInvitation().withScopeType(ScopeType.WORKSPACE).withScopeId(workspace.workspaceId).withPermissionType(WORKSPACE_READER),
      )
    every { userInvitationService.getPendingInvitations(ScopeType.ORGANIZATION, orgId.value) } returns
      listOf(
        UserInvitation().withScopeType(ScopeType.ORGANIZATION).withScopeId(orgId.value).withPermissionType(ORGANIZATION_ADMIN),
        UserInvitation().withScopeType(ScopeType.ORGANIZATION).withScopeId(orgId.value).withPermissionType(ORGANIZATION_EDITOR),
        UserInvitation().withScopeType(ScopeType.ORGANIZATION).withScopeId(orgId.value).withPermissionType(ORGANIZATION_MEMBER),
      )
    every { userInvitationService.updateUserInvitations(any()) } just Runs

    featureDegradationService.downgradeUserInvites(orgId, workspace)
    verify {
      userInvitationService.updateUserInvitations(
        listOf(
          UserInvitation().withScopeType(ScopeType.WORKSPACE).withScopeId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
          UserInvitation().withScopeType(ScopeType.WORKSPACE).withScopeId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
          UserInvitation().withScopeType(ScopeType.WORKSPACE).withScopeId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
        ),
      )
    }
    verify {
      userInvitationService.updateUserInvitations(
        listOf(
          UserInvitation().withScopeType(ScopeType.ORGANIZATION).withScopeId(orgId.value).withPermissionType(ORGANIZATION_ADMIN),
          UserInvitation().withScopeType(ScopeType.WORKSPACE).withScopeId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
          UserInvitation().withScopeType(ScopeType.WORKSPACE).withScopeId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
        ),
      )
    }
  }
}
