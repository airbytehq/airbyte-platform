/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectionEntitlementsValidatorTest {
  private val destinationService = mockk<DestinationService>()
  private val sourceService = mockk<SourceService>()
  private val connectionService = mockk<ConnectionService>(relaxed = true)
  private val workspaceHelper = mockk<WorkspaceHelper>()
  private val licenseEntitlementChecker = mockk<LicenseEntitlementChecker>()

  private var standardSourceDef = StandardSourceDefinition().withSourceDefinitionId(UUID.randomUUID()).withEnterprise(false)
  private var enterpriseSourceDef = StandardSourceDefinition().withSourceDefinitionId(UUID.randomUUID()).withEnterprise(true)
  private var standardDestinationDef = StandardDestinationDefinition().withDestinationDefinitionId(UUID.randomUUID()).withEnterprise(false)
  private var enterpriseDestinationDef = StandardDestinationDefinition().withDestinationDefinitionId(UUID.randomUUID()).withEnterprise(true)

  private lateinit var connectionEntitlementsValidator: ConnectionEntitlementsValidator

  @BeforeEach
  fun setup() {
    connectionEntitlementsValidator =
      ConnectionEntitlementsValidator(destinationService, sourceService, connectionService, workspaceHelper, licenseEntitlementChecker)
  }

  @Test
  fun testValidateEntitlements() {
    every { sourceService.listPublicSourceDefinitions(false) } returns listOf(standardSourceDef, enterpriseSourceDef)
    every { destinationService.listPublicDestinationDefinitions(false) } returns listOf(standardDestinationDef, enterpriseDestinationDef)

    val entitledOrgId = UUID.randomUUID()
    val entitledWorkspaceId = UUID.randomUUID()
    val entitledConnectionId = UUID.randomUUID()
    val entitledSourceId = UUID.randomUUID()
    val entitledDestinationId = UUID.randomUUID()
    val notEntitledOrgId = UUID.randomUUID()
    val notEntitledWorkspaceId = UUID.randomUUID()
    val notEntitledConnectionId = UUID.randomUUID()
    val notEntitledConnectionId2 = UUID.randomUUID()
    val notEntitledSourceId = UUID.randomUUID()
    val notEntitledDestinationId = UUID.randomUUID()

    val entitledSource = SourceConnection().withSourceId(entitledSourceId).withWorkspaceId(entitledWorkspaceId)
    val entitledSourceConnection = StandardSync().withConnectionId(entitledConnectionId).withSourceId(entitledSourceId)

    val notEntitledSource = SourceConnection().withSourceId(notEntitledSourceId).withWorkspaceId(notEntitledWorkspaceId)
    val notEntitledSourceConnection = StandardSync().withConnectionId(notEntitledConnectionId).withSourceId(notEntitledSourceId)

    val entitledDestinationConnection =
      StandardSync()
        .withConnectionId(
          entitledConnectionId,
        ).withSourceId(entitledSourceId)
        .withDestinationId(entitledDestinationId)
    val notEntitledDestinationConnection =
      StandardSync()
        .withConnectionId(
          notEntitledConnectionId2,
        ).withSourceId(notEntitledSourceId)
        .withDestinationId(notEntitledDestinationId)

    every {
      connectionService.listConnectionsByActorDefinitionIdAndType(enterpriseSourceDef.sourceDefinitionId, "source", false, false)
    } returns listOf(entitledSourceConnection, notEntitledSourceConnection)
    every {
      connectionService.listConnectionsByActorDefinitionIdAndType(enterpriseDestinationDef.destinationDefinitionId, "destination", false, false)
    } returns listOf(entitledDestinationConnection, notEntitledDestinationConnection)

    every { sourceService.listSourcesWithIds(listOf(entitledSourceId, notEntitledSourceId)) } returns listOf(entitledSource, notEntitledSource)

    every { workspaceHelper.getOrganizationForWorkspace(entitledWorkspaceId) } returns entitledOrgId
    every { workspaceHelper.getOrganizationForWorkspace(notEntitledWorkspaceId) } returns notEntitledOrgId

    every {
      licenseEntitlementChecker.checkEntitlement(entitledOrgId, Entitlement.SOURCE_CONNECTOR, enterpriseSourceDef.sourceDefinitionId)
    } returns true

    every {
      licenseEntitlementChecker.checkEntitlement(notEntitledOrgId, Entitlement.SOURCE_CONNECTOR, enterpriseSourceDef.sourceDefinitionId)
    } returns false

    every {
      licenseEntitlementChecker.checkEntitlement(entitledOrgId, Entitlement.DESTINATION_CONNECTOR, enterpriseDestinationDef.destinationDefinitionId)
    } returns true

    every {
      licenseEntitlementChecker.checkEntitlement(
        notEntitledOrgId,
        Entitlement.DESTINATION_CONNECTOR,
        enterpriseDestinationDef.destinationDefinitionId,
      )
    } returns false

    connectionEntitlementsValidator.validateEntitlements()

    verify(exactly = 1) {
      connectionService.disableConnectionsById(listOf(notEntitledConnectionId))
    }

    verify(exactly = 1) {
      connectionService.disableConnectionsById(listOf(notEntitledConnectionId2))
    }
  }
}
