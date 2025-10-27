/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ListOrganizationSummariesRequestBody
import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.api.model.generated.OrganizationCreateRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationReadList
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.api.model.generated.WorkspaceReadList
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.config.Organization
import io.airbyte.data.repositories.OrgMemberCount
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.impls.data.OrganizationPaymentConfigServiceDataImpl
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.FeatureFlagClient
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

class OrganizationsHandlerTest {
  private val organizationId1 = UUID.randomUUID()
  private val organizationName = "org_name"
  private val organizationEmail = "email@email.com"
  private val organizationSsoRealm = "realm"
  private val organization =
    Organization()
      .withOrganizationId(organizationId1)
      .withEmail(organizationEmail)
      .withName(organizationName)

  private lateinit var permissionHandler: PermissionHandler
  private lateinit var organizationService: OrganizationService
  private lateinit var uuidSupplier: Supplier<UUID>
  private lateinit var organizationsHandler: OrganizationsHandler
  private lateinit var organizationPaymentConfigService: OrganizationPaymentConfigServiceDataImpl
  private lateinit var workspacesHandler: WorkspacesHandler
  private lateinit var permissionService: PermissionService
  private lateinit var entitlementService: EntitlementService
  private lateinit var featureFlagClient: FeatureFlagClient

  @BeforeEach
  fun setup() {
    permissionHandler = mockk(relaxed = true)
    uuidSupplier = mockk()
    organizationService = mockk()
    organizationPaymentConfigService = mockk(relaxed = true)
    workspacesHandler = mockk()
    permissionService = mockk()
    entitlementService = mockk(relaxed = true)
    featureFlagClient = mockk(relaxed = true)

    organizationsHandler =
      spyk(
        OrganizationsHandler(
          organizationService,
          permissionHandler,
          uuidSupplier,
          organizationPaymentConfigService,
          workspacesHandler,
          permissionService,
          entitlementService,
          featureFlagClient,
        ),
        recordPrivateCalls = true,
      )
  }

  @Test
  fun testCreateOrganization() {
    val newOrganization =
      Organization()
        .withOrganizationId(organizationId1)
        .withEmail(organizationEmail)
        .withName(organizationName)

    every { uuidSupplier.get() } returns organizationId1
    every { organizationService.writeOrganization(any()) } just runs

    val result =
      organizationsHandler.createOrganization(
        OrganizationCreateRequestBody()
          .organizationName(organizationName)
          .email(organizationEmail),
      )

    assertEquals(organizationId1, result.organizationId)
    assertEquals(organizationName, result.organizationName)
    assertEquals(organizationEmail, result.email)

    verify { organizationPaymentConfigService.saveDefaultPaymentConfig(organizationId1) }
  }

  @Test
  fun testGetOrganization() {
    every {
      organizationService.getOrganization(organizationId1)
    } returns
      Optional.of(
        Organization()
          .withOrganizationId(organizationId1)
          .withEmail(organizationEmail)
          .withName(organizationName)
          .withSsoRealm(
            organizationSsoRealm,
          ),
      )

    val result =
      organizationsHandler.getOrganization(
        OrganizationIdRequestBody().organizationId(organizationId1),
      )

    assertEquals(organizationId1, result.organizationId)
    assertEquals(organizationName, result.organizationName)
    assertEquals(organizationEmail, result.email)
    assertEquals(organizationSsoRealm, result.ssoRealm)
  }

  @Test
  fun testUpdateOrganization() {
    val newName = "new name"
    val newEmail = "new email"
    val updatedOrg =
      Organization()
        .withOrganizationId(organizationId1)
        .withName(newName)
        .withEmail(newEmail)

    every {
      organizationService.getOrganization(organizationId1)
    } returns Optional.of(organization.withEmail(organizationEmail).withName(organizationName))

    every {
      organizationService.writeOrganization(any())
    } just runs

    val result =
      organizationsHandler.updateOrganization(
        OrganizationUpdateRequestBody()
          .organizationId(organizationId1)
          .organizationName(newName)
          .email(newEmail),
      )

    assertEquals(organizationId1, result.organizationId)
    assertEquals(newName, result.organizationName)
    assertEquals(newEmail, result.email)
  }

  @Test
  fun testListOrganizationsByUserWithoutPagination() {
    val userId = UUID.randomUUID()
    val orgId = UUID.randomUUID()
    val request =
      ListOrganizationsByUserRequestBody()
        .userId(userId)
        .nameContains("keyword")

    every {
      organizationService.listOrganizationsByUserId(userId, any())
    } returns
      listOf(
        Organization()
          .withOrganizationId(orgId)
          .withUserId(userId)
          .withName("org name")
          .withEmail(organizationEmail)
          .withSsoRealm(
            organizationSsoRealm,
          ),
      )

    val expectedList =
      OrganizationReadList().organizations(
        listOf(
          OrganizationRead()
            .organizationName("org name")
            .organizationId(orgId)
            .email(organizationEmail)
            .ssoRealm(
              organizationSsoRealm,
            ),
        ),
      )

    val result = organizationsHandler.listOrganizationsByUser(request)
    assertEquals(expectedList, result)
  }

  @Test
  fun testListOrganizationsByUserWithPagination() {
    val userId = UUID.randomUUID()
    val orgId = UUID.randomUUID()
    val request =
      ListOrganizationsByUserRequestBody()
        .userId(userId)
        .nameContains("keyword")
        .pagination(Pagination().pageSize(10).rowOffset(1))

    every {
      organizationService.listOrganizationsByUserIdPaginated(any(), any())
    } returns
      listOf(
        Organization()
          .withOrganizationId(orgId)
          .withUserId(userId)
          .withName(organizationName)
          .withEmail(organizationEmail)
          .withSsoRealm(
            organizationSsoRealm,
          ),
      )

    val expectedList =
      OrganizationReadList().organizations(
        listOf(
          OrganizationRead()
            .organizationName(organizationName)
            .organizationId(orgId)
            .email(organizationEmail)
            .ssoRealm(
              organizationSsoRealm,
            ),
        ),
      )

    val result = organizationsHandler.listOrganizationsByUser(request)
    assertEquals(expectedList, result)
  }

  @Test
  fun `listing organization summaries returns existing summaries`() {
    val userId = UUID.randomUUID()
    val orgId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    val request =
      ListOrganizationSummariesRequestBody()
        .userId(userId)
        .pagination(Pagination().pageSize(10).rowOffset(0))

    val organizationRead =
      OrganizationRead()
        .organizationId(orgId)
        .organizationName("test-org")
        .email("org@example.com")
    val orgList = OrganizationReadList().organizations(listOf(organizationRead))

    val workspaceRead =
      WorkspaceRead()
        .workspaceId(workspaceId)
        .organizationId(orgId)
        .name("test-workspace")
    val workspaceList = WorkspaceReadList().workspaces(listOf(workspaceRead))

    every { organizationsHandler.listOrganizationsByUser(any()) } returns orgList

    every { workspacesHandler.listWorkspacesByUser(any()) } returns workspaceList

    every { permissionService.getMemberCountsForOrganizationList(listOf(orgId)) } returns listOf(OrgMemberCount(orgId, 5))

    val response = organizationsHandler.getOrganizationSummaries(request)

    assertEquals(1, response.organizationSummaries.size)

    val summary = response.organizationSummaries[0]
    assertEquals(orgId, summary.organization.organizationId)
    assertEquals("test-org", summary.organization.organizationName)
    assertEquals(1, summary.workspaces.size)
    assertEquals("test-workspace", summary.workspaces[0].name)
    assertEquals(5, summary.memberCount)
  }

  @Test
  fun `createOrganization calls EntitlementService with UNIFIED_TRIAL when feature flag is enabled`() {
    val userId = UUID.randomUUID()
    val orgId = UUID.randomUUID()
    val newOrganization =
      Organization()
        .withOrganizationId(orgId)
        .withEmail(organizationEmail)
        .withName(organizationName)
        .withUserId(userId)

    every { uuidSupplier.get() } returns orgId
    every { organizationService.writeOrganization(any()) } just runs
    every { featureFlagClient.boolVariation(any(), any()) } returns true

    val request =
      OrganizationCreateRequestBody()
        .organizationName(organizationName)
        .email(organizationEmail)
        .userId(userId)

    organizationsHandler.createOrganization(request)

    verify { entitlementService.addOrUpdateOrganization(OrganizationId(orgId), EntitlementPlan.UNIFIED_TRIAL) }
  }

  @Test
  fun `createOrganization calls EntitlementService with STANDARD_TRIAL when feature flag is disabled`() {
    val userId = UUID.randomUUID()
    val orgId = UUID.randomUUID()
    val newOrganization =
      Organization()
        .withOrganizationId(orgId)
        .withEmail(organizationEmail)
        .withName(organizationName)
        .withUserId(userId)

    every { uuidSupplier.get() } returns orgId
    every { organizationService.writeOrganization(any()) } just runs
    every { featureFlagClient.boolVariation(any(), any()) } returns false

    val request =
      OrganizationCreateRequestBody()
        .organizationName(organizationName)
        .email(organizationEmail)
        .userId(userId)

    organizationsHandler.createOrganization(request)

    verify { entitlementService.addOrUpdateOrganization(OrganizationId(orgId), EntitlementPlan.STANDARD_TRIAL) }
  }

// TODO: enable these tests once we're ready to enable Stigg

//  @Test
//  fun `createOrganization throws EntitlementServiceUnableToAddOrganizationProblem when EntitlementClient fails`() {
//    val userId = UUID.randomUUID()
//    val orgId = UUID.randomUUID()
//    val newOrganization =
//      Organization()
//        .withOrganizationId(orgId)
//        .withEmail(organizationEmail)
//        .withName(organizationName)
//        .withUserId(userId)
//
//    every { uuidSupplier.get() } returns orgId
//    every { organizationService.writeOrganization(any()) } just runs
//    every { entitlementClient.addOrganization(any(), any()) } throws RuntimeException("Entitlement service unavailable")
//
//    val request =
//      OrganizationCreateRequestBody()
//        .organizationName(organizationName)
//        .email(organizationEmail)
//        .userId(userId)
//
//    val exception =
//      assertThrows(EntitlementServiceUnableToAddOrganizationProblem::class.java) {
//        organizationsHandler.createOrganization(request)
//      }
//
//    assertEquals("Failed to register organization with entitlement service", exception.problem.getDetail())
//    val data = exception.problem.getData() as ProblemEntitlementServiceData
//    assertEquals(orgId, data.organizationId)
//    assertEquals("Entitlement service unavailable", data.errorMessage)
//  }
//
//  @Test
//  fun `createOrganization handles plan downgrade validation errors`() {
//    val userId = UUID.randomUUID()
//    val orgId = UUID.randomUUID()
//    val newOrganization =
//      Organization()
//        .withOrganizationId(orgId)
//        .withEmail(organizationEmail)
//        .withName(organizationName)
//        .withUserId(userId)
//
//    every { uuidSupplier.get() } returns orgId
//    every { organizationService.writeOrganization(any()) } just runs
//    every { entitlementClient.addOrganization(any(), any()) } throws
//      EntitlementServiceUnableToAddOrganizationProblem(
//        ProblemEntitlementServiceData()
//          .organizationId(orgId)
//          .planId(EntitlementPlan.STANDARD_TRIAL.toString())
//          .errorMessage("Cannot downgrade from PRO (value: 1) to STANDARD (value: 0)"),
//      )
//
//    val request =
//      OrganizationCreateRequestBody()
//        .organizationName(organizationName)
//        .email(organizationEmail)
//        .userId(userId)
//
//    val exception =
//      assertThrows(EntitlementServiceUnableToAddOrganizationProblem::class.java) {
//        organizationsHandler.createOrganization(request)
//      }
//
//    val data = exception.problem.getData() as ProblemEntitlementServiceData
//    assertEquals(orgId, data.organizationId)
//    assertEquals(EntitlementPlan.STANDARD_TRIAL.toString(), data.planId)
//    assertTrue(data.errorMessage.contains("Cannot downgrade from PRO (value: 1) to STANDARD (value: 0)"))
//  }
}
