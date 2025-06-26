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
import io.airbyte.config.Organization
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.data.repositories.OrgMemberCount
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.impls.data.OrganizationPaymentConfigServiceDataImpl
import io.mockk.every
import io.mockk.mockk
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
  private lateinit var organizationPersistence: OrganizationPersistence
  private lateinit var uuidSupplier: Supplier<UUID>
  private lateinit var organizationsHandler: OrganizationsHandler
  private lateinit var organizationPaymentConfigService: OrganizationPaymentConfigServiceDataImpl
  private lateinit var workspacesHandler: WorkspacesHandler
  private lateinit var permissionService: PermissionService

  @BeforeEach
  fun setup() {
    permissionHandler = mockk(relaxed = true)
    uuidSupplier = mockk()
    organizationPersistence = mockk()
    organizationPaymentConfigService = mockk(relaxed = true)
    workspacesHandler = mockk()
    permissionService = mockk()

    organizationsHandler =
      spyk(
        OrganizationsHandler(
          organizationPersistence,
          permissionHandler,
          uuidSupplier,
          organizationPaymentConfigService,
          workspacesHandler,
          permissionService,
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
    every { organizationPersistence.createOrganization(any()) } returns newOrganization

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
      organizationPersistence.getOrganization(organizationId1)
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
      organizationPersistence.getOrganization(organizationId1)
    } returns Optional.of(organization.withEmail(organizationEmail).withName(organizationName))

    every {
      organizationPersistence.updateOrganization(any())
    } returns updatedOrg

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
      organizationPersistence.listOrganizationsByUserId(userId, any())
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
      organizationPersistence.listOrganizationsByUserIdPaginated(any(), any())
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
}
