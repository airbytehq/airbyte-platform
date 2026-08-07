/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Configs
import io.airbyte.config.Group
import io.airbyte.config.User
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.GroupManagedByScimException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.InactiveUserAccessException
import io.airbyte.data.services.UserNotOrganizationMemberException
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.publicApi.server.generated.models.GroupMemberAddRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.concurrent.Callable

class GroupMembersControllerTest {
  private val groupService = mockk<GroupService>()
  private val userPersistence = mockk<UserPersistence>()
  private val trackingHelper = mockk<TrackingHelper>()
  private val roleResolver = mockk<RoleResolver>()
  private val currentUserService = mockk<CurrentUserService>()
  private val entitlementService = mockk<EntitlementService>()
  private val request = mockk<RoleResolver.Request>()
  private val groupId = UUID.randomUUID()
  private val userId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val currentUserId = UUID.randomUUID()
  private val controller =
    GroupMembersController(
      groupService,
      userPersistence,
      trackingHelper,
      roleResolver,
      currentUserService,
      entitlementService,
      Configs.AirbyteEdition.ENTERPRISE,
    )

  @BeforeEach
  fun setUp() {
    every { groupService.getGroup(GroupId(groupId)) } returns
      Group(
        groupId = GroupId(groupId),
        name = "Engineering",
        description = null,
        organizationId = OrganizationId(organizationId),
        memberCount = 0,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )
    every { roleResolver.newRequest() } returns request
    every { request.withCurrentUser() } returns request
    every { request.withRef(AuthenticationId.ORGANIZATION_ID, organizationId.toString()) } returns request
    every { request.requireRole(any()) } just Runs
    every { userPersistence.getUser(userId) } returns Optional.of(User().withUserId(userId))
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser().withUserId(currentUserId)
    every { trackingHelper.callWithTracker<Any>(any(), any(), any(), any()) } answers {
      firstArg<Callable<Any>>().call()
    }
  }

  @Test
  fun `adding an inactive SCIM User returns a state conflict`() {
    every {
      groupService.addGroupMember(GroupId(groupId), UserId(userId), OrganizationId(organizationId))
    } throws InactiveUserAccessException("inactive")

    val problem =
      assertThrows<StateConflictProblem> {
        controller.publicAddGroupMember(groupId, GroupMemberAddRequest(userId))
      }

    assertThat(problem.problem.getStatus()).isEqualTo(409)
  }

  @Test
  fun `adding an ordinary user outside the organization returns not found`() {
    every {
      groupService.addGroupMember(GroupId(groupId), UserId(userId), OrganizationId(organizationId))
    } throws UserNotOrganizationMemberException("not a member")

    val problem =
      assertThrows<ResourceNotFoundProblem> {
        controller.publicAddGroupMember(groupId, GroupMemberAddRequest(userId))
      }

    assertThat(problem.problem.getStatus()).isEqualTo(404)
  }

  @Test
  fun `managed Group membership mutations translate to 409`() {
    every { groupService.addGroupMember(GroupId(groupId), UserId(userId), OrganizationId(organizationId)) } throws
      GroupManagedByScimException("Group is managed by SCIM")
    every { groupService.removeGroupMember(GroupId(groupId), UserId(userId), OrganizationId(organizationId)) } throws
      GroupManagedByScimException("Group is managed by SCIM")

    val addProblem =
      assertThrows<StateConflictProblem> {
        controller.publicAddGroupMember(groupId, GroupMemberAddRequest(userId))
      }
    val removeProblem =
      assertThrows<StateConflictProblem> {
        controller.publicRemoveGroupMember(groupId, userId)
      }

    assertThat(addProblem.problem.getStatus()).isEqualTo(409)
    assertThat(removeProblem.problem.getStatus()).isEqualTo(409)
  }
}
