/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.DestinationSalesforceEnterpriseConnector
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.FasterSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.MappersEntitlement
import io.airbyte.commons.entitlements.models.RbacRolesEntitlement
import io.airbyte.commons.entitlements.models.SourceNetsuiteEnterpriseConnector
import io.airbyte.commons.entitlements.models.SourceOracleEnterpriseConnector
import io.airbyte.config.ActorType
import io.airbyte.config.Cron
import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_ADMIN
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_EDITOR
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_MEMBER
import io.airbyte.config.Permission.PermissionType.WORKSPACE_ADMIN
import io.airbyte.config.Permission.PermissionType.WORKSPACE_EDITOR
import io.airbyte.config.Permission.PermissionType.WORKSPACE_READER
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.StatusReason
import io.airbyte.config.helpers.CronExpressionHelper
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.shared.ConnectionCronSchedule
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
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

class FeatureDegradationServiceTest {
  private val permissionService = mockk<PermissionService>()
  private val entitlementClient = mockk<EntitlementClient>()
  private val connectionService = mockk<ConnectionService>(relaxed = true)
  private val workspacePersistence = mockk<WorkspacePersistence>()
  private val cronExpressionHelper = CronExpressionHelper()
  private val featureDegradationService =
    FeatureDegradationService(permissionService, entitlementClient, connectionService, workspacePersistence, cronExpressionHelper)
  private val featureDegradationServiceStubbed = spyk(featureDegradationService)

  private val cronTimezoneUtc = "UTC"

  @Test
  fun `downgradeFeaturesIfRequired only executes downgrades when going from UNIFIED_TRIAL to STANDARD`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val connectionId = UUID.randomUUID()

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
    every { entitlementClient.updateOrganization(orgId, EntitlementPlan.STANDARD) } just runs
    every { featureDegradationServiceStubbed.downgradeRBACRoles(orgId) } just runs
    every { connectionService.listSubHourConnectionIdsForOrganization(orgId.value) } returns listOf(connectionId)

    featureDegradationServiceStubbed.downgradeFeaturesIfRequired(orgId, EntitlementPlan.UNIFIED_TRIAL, EntitlementPlan.STANDARD)

    // Verify downgrade RBAC function is called only when going from UNIFIED_TRIAL to STANDARD
    verify { featureDegradationServiceStubbed.downgradeRBACRoles(orgId) }
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
    verify { connectionService.listSubHourConnectionIdsForOrganization(orgId.value) }
    verify { connectionService.listConnectionCronSchedulesForOrganization(orgId.value) }
    verify {
      connectionService.lockConnectionsById(
        match { it.contains(connectionId) },
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
    every { featureDegradationServiceStubbed.downgradeRBACRoles(orgId) } just runs

    featureDegradationServiceStubbed.downgradeFeaturesIfRequired(orgId, EntitlementPlan.PRO, EntitlementPlan.STANDARD)

    // Verify downgrade RBAC function is not called if not going from UNIFIED_TRIAL to STANDARD
    verify(exactly = 0) { featureDegradationServiceStubbed.downgradeRBACRoles(orgId) }
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
    every { featureDegradationServiceStubbed.downgradeRBACRoles(orgId) } just runs

    val connectionIds = listOf(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
    every { connectionService.listConnectionIdsForOrganizationWithMappers(orgId.value) } returns listOf(connectionIds[0], connectionIds[1])
    every { featureDegradationServiceStubbed.findSubHourSyncIds(orgId) } returns listOf(connectionIds[1], connectionIds[2])
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
        match { it.containsAll(connectionIds.slice(0..4)) },
        StatusReason.SUBSCRIPTION_DOWNGRADED_ACCESS_REVOKED.value,
      )
    }
  }

  @Test
  fun downgradeRBACRoles() {
    val orgId = OrganizationId(UUID.randomUUID())
    val workspace = StandardWorkspace().withWorkspaceId(UUID.randomUUID()).withOrganizationId(orgId.value)

    every { workspacePersistence.getDefaultWorkspaceForOrganization(orgId.value) } returns workspace
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

    featureDegradationService.downgradeRBACRoles(orgId)
    verify { workspacePersistence.getDefaultWorkspaceForOrganization(orgId.value) }
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
  fun findSubHourSyncIds() {
    val orgId = OrganizationId(UUID.randomUUID())

    val cronSlow = "0 0 */4 * * ?"
    val cronSlow2 = "0 0 */2 * * ?"
    val cronFast = "0 */30 * * * ?"
    val cronFast2 = "0 */14 * * * ?"

    val connectionIds = listOf(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
    every { connectionService.listSubHourConnectionIdsForOrganization(orgId.value) } returns listOf(connectionIds[0], connectionIds[1])
    every { connectionService.listConnectionCronSchedulesForOrganization(orgId.value) } returns
      listOf(
        ConnectionCronSchedule(connectionIds[2], ScheduleData().withCron(Cron().withCronExpression(cronSlow).withCronTimeZone(cronTimezoneUtc))),
        ConnectionCronSchedule(connectionIds[3], ScheduleData().withCron(Cron().withCronExpression(cronFast).withCronTimeZone(cronTimezoneUtc))),
        ConnectionCronSchedule(connectionIds[4], ScheduleData().withCron(Cron().withCronExpression(cronSlow2).withCronTimeZone(cronTimezoneUtc))),
        ConnectionCronSchedule(connectionIds[5], ScheduleData().withCron(Cron().withCronExpression(cronFast2).withCronTimeZone(cronTimezoneUtc))),
      )

    val result = featureDegradationService.findSubHourSyncIds(orgId)

    Assertions.assertTrue(result.containsAll(listOf(connectionIds[0], connectionIds[1], connectionIds[3], connectionIds[5])))
  }
}
