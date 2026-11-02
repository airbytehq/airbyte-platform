/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest
import io.airbyte.publicApi.server.generated.models.ConnectionPatchRequest
import io.airbyte.publicApi.server.generated.models.Tag
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.util.UUID

class PublicConnectionTagCompatibilityTest {
  @Test
  fun `connection creation keeps skipping foreign and missing tags`() {
    val fixture = Fixture()
    val rule = PublicConnectionCreateRule(fixture.data, fixture.permissions)
    val request =
      ConnectionCreateRequest(
        sourceId = SOURCE,
        destinationId = DESTINATION,
        tags = listOf(tag(FOREIGN_TAG, OTHER_WORKSPACE), tag(MISSING_TAG, WORKSPACE)),
      )
    every { fixture.data.tagWorkspaceAndOrganization(FOREIGN_TAG) } returns WorkspaceAndOrganization(OTHER_WORKSPACE, ORGANIZATION)
    every { fixture.data.tagWorkspaceAndOrganization(MISSING_TAG) } throws AuthorizationResourceNotFoundException()

    assertDoesNotThrow {
      rule.authorize(fixture.caller, rule.readInput(arrayOf(request)))
    }
    verify(exactly = 0) { fixture.data.tagWorkspaceAndOrganization(any()) }
  }

  @Test
  fun `connection patch keeps skipping foreign and missing tags`() {
    val fixture = Fixture()
    val rule = PublicConnectionPatchRule(fixture.data, fixture.permissions)
    val request = ConnectionPatchRequest(tags = listOf(tag(FOREIGN_TAG, OTHER_WORKSPACE), tag(MISSING_TAG, WORKSPACE)))
    every { fixture.data.savedConnectionActorIds(CONNECTION) } returns (SOURCE to DESTINATION)
    every { fixture.data.tagWorkspaceAndOrganization(FOREIGN_TAG) } returns WorkspaceAndOrganization(OTHER_WORKSPACE, ORGANIZATION)
    every { fixture.data.tagWorkspaceAndOrganization(MISSING_TAG) } throws AuthorizationResourceNotFoundException()

    assertDoesNotThrow {
      rule.authorize(fixture.caller, rule.readInput(arrayOf(CONNECTION.toString(), request)))
    }
    verify(exactly = 0) { fixture.data.tagWorkspaceAndOrganization(any()) }
  }

  private class Fixture {
    val data = mockk<AuthorizationDataService>().also { it.answerConnectionOwnersFromActorStubs() }
    val caller = Authentication.build("caller")
    val permissions: OperationPermissions

    init {
      val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
      every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns owner
      every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns owner
      val grants = mockk<PermissionService>()
      every { grants.getPermissionsByAuthUserId("caller") } returns
        listOf(Permission().withWorkspaceId(WORKSPACE).withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR))
      permissions = OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true)
    }
  }

  companion object {
    private fun tag(
      id: UUID,
      workspace: UUID,
    ) = Tag(tagId = id, workspaceId = workspace, name = "fixture", color = "#123456")

    private val WORKSPACE = UUID(0, 1)
    private val OTHER_WORKSPACE = UUID(0, 2)
    private val ORGANIZATION = UUID(0, 3)
    private val SOURCE = UUID(0, 4)
    private val DESTINATION = UUID(0, 5)
    private val CONNECTION = UUID(0, 6)
    private val FOREIGN_TAG = UUID(0, 7)
    private val MISSING_TAG = UUID(0, 8)
  }
}
