/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.publicApi.server.generated.models.PermissionCreateRequest
import io.airbyte.publicApi.server.generated.models.PublicPermissionType
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class PermissionsControllerTest {
  private val trackingHelper = mockk<TrackingHelper>(relaxed = true)
  private val roleResolver = mockk<RoleResolver>()
  private val currentUserService = mockk<CurrentUserService>()
  private val permissionHandler = mockk<PermissionHandler>()
  private val controller = PermissionsController(trackingHelper, roleResolver, currentUserService, permissionHandler)

  @Test
  fun `permission with both workspace and organization scope is rejected before authorization`() {
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser().withUserId(UUID.randomUUID())

    assertThrows<BadRequestProblem> {
      controller.publicCreatePermission(
        PermissionCreateRequest(
          permissionType = PublicPermissionType.WORKSPACE_ADMIN,
          userId = UUID.randomUUID(),
          workspaceId = UUID.randomUUID(),
          organizationId = UUID.randomUUID(),
        ),
      )
    }

    verify(exactly = 0) { roleResolver.newRequest() }
    verify(exactly = 0) { permissionHandler.createPermission(any()) }
  }
}
