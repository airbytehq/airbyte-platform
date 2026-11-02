/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.ApplicationAuthorizationService
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.AuthorizationUserIdentity
import io.airbyte.domain.services.auth.MembershipAuthorizationDataService
import io.airbyte.domain.services.auth.PublicDefinitionAuthorizationService
import io.airbyte.domain.services.auth.PublicDefinitionMetadata
import io.airbyte.domain.services.auth.PublicWorkspaceAuthorizationService
import io.airbyte.domain.services.auth.SavedConnectionReferences
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.function.Executable
import java.util.Optional
import java.util.UUID

/** Exercise every protected public operation through HTTP with its supported and rejected grants. */
fun assertPublicAuthorizedRequests(
  cloud: Boolean,
  earlierAllow: Boolean = true,
  callerToken: String = "user",
) {
  val id = UUID.fromString("00000000-0000-0000-0000-000000000101")
  val destination = UUID.fromString("00000000-0000-0000-0000-000000000104")
  val workspace = UUID.fromString("00000000-0000-0000-0000-000000000102")
  val organization = UUID.fromString("00000000-0000-0000-0000-000000000103")
  val owner = WorkspaceAndOrganization(workspace, organization)
  val mapper = MoreMappers.initMapper()
  assertAll(
    publicApiHttpScenarios().map { (ossController, requests) ->
      Executable {
        val controller =
          if (cloud && ossController.simpleName == "WorkspacesController") {
            Class.forName("io.airbyte.server.wrapped.apis.publicapi.controllers.CloudWorkspacesController")
          } else {
            ossController
          }
        val definitions = mockk<PublicDefinitionAuthorizationService>()
        val grants = mockk<PermissionService>()
        RemainingOperationHttpSupport(cloud, listOf(controller), publicApiRuleNames(controller, cloud), earlierAllow = earlierAllow).run(
          configure = { context, business ->
            configurePublicApiBusiness(business, cloud)
            val data = mockk<AuthorizationDataService>()
            every { data.workspaceAndOrganization(any()) } returns owner
            every { data.savedWorkspaceAndOrganization(any(), any()) } returns owner
            every { data.savedSourceWorkspaceAndOrganization(any()) } returns owner
            every { data.savedDestinationWorkspaceAndOrganization(any()) } returns owner
            every { data.tagWorkspaceAndOrganization(any()) } returns owner
            every { data.savedConnectionActorIds(any()) } returns (id to destination)
            every { data.savedConnectionReferences(any()) } returns
              SavedConnectionReferences(id, destination, Optional.empty(), Optional.empty(), emptyList())
            every { data.actorConnectionIds(any(), any(), any()) } returns emptyList()
            every { data.replicationJobConnectionId(any()) } returns id
            every { data.organization(any()) } answers { firstArg() }
            every { data.groupOrganization(any()) } returns organization
            every { data.dataplaneGroupOrganization(any()) } returns organization
            every { data.dataplaneOrganization(any()) } returns organization
            every { data.permissionTarget(any()) } returns AuthorizationTarget.Workspace(owner)
            every { data.requireActorDefinitionType(any(), any()) } just Runs
            every { data.requireUser(any()) } just Runs
            every { data.canUseActorDefinition(any(), any()) } returns true
            every { data.canUseDataplaneGroup(any(), any()) } returns true
            every { data.isDefaultDataplaneGroup(any()) } returns true
            context.registerSingleton(AuthorizationDataService::class.java, data)
            val membership = mockk<MembershipAuthorizationDataService>()
            every { membership.isDirectOrganizationMember(any(), any()) } returns true
            every { membership.permissionGroup(any()) } returns Optional.of(id)
            every { membership.currentIdentity("caller") } returns AuthorizationUserIdentity(id, Optional.empty())
            context.registerSingleton(MembershipAuthorizationDataService::class.java, membership)
            every { definitions.builderWorkspace(any()) } returns workspace
            context.registerSingleton(PublicDefinitionAuthorizationService::class.java, definitions)
            context.registerSingleton(io.airbyte.featureflag.FeatureFlagClient::class.java, mockk<io.airbyte.featureflag.FeatureFlagClient>())
            val bootstrap = mockk<PublicWorkspaceAuthorizationService>()
            every { bootstrap.implicitOrganization(any(), any()) } returns Optional.of(organization)
            context.registerSingleton(PublicWorkspaceAuthorizationService::class.java, bootstrap)
            val applications = mockk<ApplicationAuthorizationService>()
            every { applications.currentUserSubject("caller") } returns "caller"
            every { applications.ownsPublicApplication("caller", any()) } returns true
            context.registerSingleton(ApplicationAuthorizationService::class.java, applications)
            every { grants.getPermissionsByAuthUserId("caller") } returns
              listOf(Permission().withPermissionType(Permission.PermissionType.INSTANCE_ADMIN))
            context.registerSingleton(
              OperationPermissions::class.java,
              OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
            )
          },
          check = { _, server, business ->
            assertAll(
              requests.map { request ->
                Executable {
                  val minimum = publicMinimumPermission(ossController.simpleName, request, cloud)
                  val organizationScope =
                    if (!cloud && ossController.simpleName == "WorkspacesController" && request.method == "POST") {
                      DEFAULT_ORGANIZATION_ID
                    } else {
                      organization
                    }
                  val permission =
                    minimum.map { type ->
                      Permission().withPermissionType(type).apply {
                        if (type.name.startsWith("ORGANIZATION_")) organizationId = organizationScope else workspaceId = workspace
                      }
                    }
                  every { grants.getPermissionsByAuthUserId("caller") } returns permission.map { listOf(it) }.orElse(emptyList())
                  every { definitions.definition(any(), any()) } returns
                    PublicDefinitionMetadata(true, request.path.contains("/declarative_sources/"))
                  val response = RemainingOperationHttpSupport.request(server, request.method, request.path, request.body, callerToken)
                  assertPublicApiResponse(ossController.simpleName, request, cloud, response)
                  if (minimum.isPresent) {
                    val type = minimum.orElseThrow()
                    val foreign =
                      Permission().withPermissionType(type).apply {
                        if (type.name.startsWith("ORGANIZATION_")) organizationId = UUID(0, 999) else workspaceId = UUID(0, 999)
                      }
                    val deniedGrants = mutableListOf(emptyList<Permission>(), listOf(foreign))
                    if (type !in setOf(Permission.PermissionType.WORKSPACE_READER, Permission.PermissionType.ORGANIZATION_MEMBER)) {
                      val reader =
                        Permission().apply {
                          if (type.name.startsWith("ORGANIZATION_")) {
                            organizationId = organizationScope
                            permissionType = Permission.PermissionType.ORGANIZATION_READER
                          } else {
                            workspaceId = workspace
                            permissionType = Permission.PermissionType.WORKSPACE_READER
                          }
                        }
                      deniedGrants.add(listOf(reader))
                    }
                    for (deniedGrant in deniedGrants) {
                      business.forEach { clearMocks(it, answers = false, recordedCalls = true) }
                      every { grants.getPermissionsByAuthUserId("caller") } returns deniedGrant
                      val denied = RemainingOperationHttpSupport.request(server, request.method, request.path, request.body, callerToken)
                      org.junit.jupiter.api.Assertions.assertEquals(
                        403,
                        denied.statusCode(),
                        "${request.method} ${request.path}, grants=$deniedGrant: ${denied.body()}",
                      )
                      org.junit.jupiter.api.Assertions.assertEquals(
                        "https://reference.airbyte.com/reference/errors#forbidden",
                        mapper.readTree(denied.body()).path("type").asText(),
                        denied.body(),
                      )
                      RemainingOperationHttpSupport.requireNoBusiness(business)
                    }
                  }
                }
              },
            )
          },
        )
      }
    },
  )
}

/** Expected actions come from the existing public methods and contract, independently of their new annotations. */
private fun publicMinimumPermission(
  controller: String,
  request: PublicApiHttpScenario,
  cloud: Boolean,
): Optional<Permission.PermissionType> {
  val read = request.method == "GET"
  val self =
    controller == "ApplicationsController" ||
      (controller == "OrganizationsController" && read) ||
      (controller == "EmbeddedController" && read) ||
      (controller == "PermissionsController" && read && request.path == "/api/public/v1/permissions")
  if (self) return Optional.empty()
  return Optional.of(
    when {
      controller in
        setOf(
          "GroupsController",
          "GroupMembersController",
          "GroupPermissionsController",
          "RegionController",
          "DataplaneController",
          "EmbeddedController",
        )
      -> Permission.PermissionType.ORGANIZATION_ADMIN
      controller == "UsersController" -> Permission.PermissionType.ORGANIZATION_MEMBER
      controller == "OrganizationsController" -> Permission.PermissionType.ORGANIZATION_ADMIN
      controller == "WorkspacesController" && request.method == "POST" ->
        if (cloud) Permission.PermissionType.ORGANIZATION_ADMIN else Permission.PermissionType.ORGANIZATION_EDITOR
      controller == "JobsController" && !read -> Permission.PermissionType.WORKSPACE_RUNNER
      controller == "PermissionsController" && !read -> Permission.PermissionType.WORKSPACE_ADMIN
      read -> Permission.PermissionType.WORKSPACE_READER
      controller == "DestinationsController" -> Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR
      controller == "DefinitionsController" && request.path.contains("/definitions/destinations") -> Permission.PermissionType.WORKSPACE_EDITOR
      else -> Permission.PermissionType.WORKSPACE_SOURCE_EDITOR
    },
  )
}
