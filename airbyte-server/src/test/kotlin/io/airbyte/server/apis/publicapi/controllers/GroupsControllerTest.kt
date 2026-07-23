/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.problems.throwable.generated.GroupAlreadyExistsProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Configs
import io.airbyte.config.Group
import io.airbyte.data.services.GroupManagedByScimException
import io.airbyte.data.services.GroupNameNotUniqueException
import io.airbyte.data.services.GroupService
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.models.GroupCreateRequest
import io.airbyte.publicApi.server.generated.models.GroupUpdateRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.Callable

class GroupsControllerTest {
  private val groupService = mockk<GroupService>()
  private val trackingHelper = mockk<TrackingHelper>(relaxed = true)
  private val roleResolver = mockk<RoleResolver>(relaxed = true)
  private val currentUserService = mockk<CurrentUserService>()
  private val entitlementService = mockk<EntitlementService>(relaxed = true)
  private lateinit var controller: GroupsController

  @BeforeEach
  fun setUp() {
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser().withUserId(UUID.randomUUID())
    every { trackingHelper.callWithTracker<Any>(any(), any(), any(), any()) } answers {
      (firstArg() as Callable<Any>).call()
    }
    controller =
      GroupsController(
        groupService,
        trackingHelper,
        roleResolver,
        currentUserService,
        entitlementService,
        Configs.AirbyteEdition.ENTERPRISE,
      )
  }

  @Test
  fun `create translates a case-insensitive name collision to 409`() {
    val organizationId = UUID.randomUUID()
    every { groupService.createGroup(any()) } throws GroupNameNotUniqueException("Group name already exists")

    val problem =
      assertThrows<GroupAlreadyExistsProblem> {
        controller.publicCreateGroup(GroupCreateRequest("ENGINEERING", organizationId))
      }

    assertThat(problem.problem.getStatus()).isEqualTo(409)
  }

  @Test
  fun `update translates a case-insensitive name collision to 409`() {
    val group = group()
    every { groupService.getGroup(group.groupId) } returns group
    every { groupService.updateGroup(any()) } throws GroupNameNotUniqueException("Group name already exists")

    val problem =
      assertThrows<GroupAlreadyExistsProblem> {
        controller.publicUpdateGroup(group.groupId.value, GroupUpdateRequest(name = "ENGINEERING"))
      }

    assertThat(problem.problem.getStatus()).isEqualTo(409)
  }

  @Test
  fun `normal managed Group mutations translate to 409`() {
    val group = group()
    every { groupService.getGroup(group.groupId) } returns group
    every { groupService.updateGroup(any()) } throws GroupManagedByScimException("Group is managed by SCIM")
    every { groupService.deleteGroup(group.groupId, group.organizationId) } throws
      GroupManagedByScimException("Group is managed by SCIM")

    val updateProblem =
      assertThrows<StateConflictProblem> {
        controller.publicUpdateGroup(group.groupId.value, GroupUpdateRequest(name = "Platform"))
      }
    val deleteProblem = assertThrows<StateConflictProblem> { controller.publicDeleteGroup(group.groupId.value) }

    assertThat(updateProblem.problem.getStatus()).isEqualTo(409)
    assertThat(deleteProblem.problem.getStatus()).isEqualTo(409)
  }

  private fun group(): Group {
    val now = OffsetDateTime.now()
    return Group(
      groupId = GroupId(UUID.randomUUID()),
      name = "Engineering",
      description = null,
      organizationId = OrganizationId(UUID.randomUUID()),
      memberCount = 0,
      createdAt = now,
      updatedAt = now,
    )
  }
}
