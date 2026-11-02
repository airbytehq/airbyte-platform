/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.HealthCheckRead
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.HealthCheckHandler
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.data.services.ApplicationService
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.ApplicationAuthorizationService
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.MembershipAuthorizationDataService
import io.airbyte.domain.services.auth.PublicDefinitionAuthorizationService
import io.airbyte.domain.services.auth.PublicDefinitionMetadata
import io.airbyte.domain.services.auth.SavedConnectionReferences
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.publicApi.server.generated.models.SourceResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.controllers.ApplicationsController
import io.airbyte.server.apis.publicapi.controllers.ConnectionsController
import io.airbyte.server.apis.publicapi.controllers.DefinitionsController
import io.airbyte.server.apis.publicapi.controllers.DestinationsController
import io.airbyte.server.apis.publicapi.controllers.GroupMembersController
import io.airbyte.server.apis.publicapi.controllers.GroupPermissionsController
import io.airbyte.server.apis.publicapi.controllers.GroupsController
import io.airbyte.server.apis.publicapi.controllers.HealthController
import io.airbyte.server.apis.publicapi.controllers.PermissionsController
import io.airbyte.server.apis.publicapi.controllers.SourcesController
import io.airbyte.server.apis.publicapi.controllers.StreamsController
import io.airbyte.server.apis.publicapi.controllers.TagsController
import io.airbyte.server.apis.publicapi.controllers.WorkspacesController
import io.airbyte.server.apis.publicapi.services.SourceService
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkClass
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.function.Executable
import java.io.IOException
import java.util.Optional
import java.util.UUID
import java.util.concurrent.Callable

abstract class PublicApiAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `public authentication rejects missing and invalid tokens before saved ownership or business reads`() {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(
      cloud,
      listOf(SourcesController::class.java),
      publicApiRuleNames(SourcesController::class.java, cloud),
      earlierAllow = false,
    ).run(
      configure = { context, _ ->
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
        )
      },
      check = { _, server, business ->
        for (token in listOf("", "invalid")) {
          val response = RemainingOperationHttpSupport.request(server, "GET", "/api/public/v1/sources/$SOURCE", "", token)
          assertEquals(401, response.statusCode(), response.body())
        }
        RemainingOperationHttpSupport.requireNoBusiness(business + data + grants)
      },
    )
  }

  @Test
  fun `stream query preserves the same workspace requirement even when both workspaces are readable`() {
    val destination = UUID(0, 107)
    blocked(StreamsController::class.java, "GET", "/streams?sourceId=$SOURCE&destinationId=$destination", "", 403) { data, _ ->
      every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
      every { data.savedDestinationWorkspaceAndOrganization(destination) } returns WorkspaceAndOrganization(UUID(0, 104), ORGANIZATION)
    }
  }

  @Test
  fun `public health and login token issuance remain accessible without a bearer token`() {
    RemainingOperationHttpSupport(
      cloud,
      listOf(HealthController::class.java, ApplicationsController::class.java),
      emptySet(),
      earlierAllow = false,
    ).run(
      configure = { _, business ->
        business.filterIsInstance<HealthCheckHandler>().forEach { every { it.health() } returns HealthCheckRead(available = true) }
        business.filterIsInstance<ApplicationService>().forEach { every { it.getToken("client", "secret") } returns "fixture-access-token" }
        business.filterIsInstance<AirbyteAuthConfig>().forEach { every { it.tokenExpiration.applicationTokenExpirationInMinutes } returns 30 }
      },
      check = { _, server, _ ->
        val health = RemainingOperationHttpSupport.request(server, "GET", "/api/public/v1/health", "", "")
        assertEquals(200, health.statusCode(), health.body())
        val login =
          RemainingOperationHttpSupport.request(
            server,
            "POST",
            "/api/public/v1/applications/token",
            """{"client_id":"client","client_secret":"secret"}""",
            "",
          )
        assertEquals(200, login.statusCode(), login.body())
        assertEquals(
          "fixture-access-token",
          MoreMappers
            .initMapper()
            .readTree(login.body())
            .get("access_token")
            .asText(),
        )
      },
    )
  }

  @Test
  fun `every protected endpoint uses only the shared decision and rejects insufficient or foreign grants`() = assertPublicAuthorizedRequests(cloud)

  @Test
  fun `public clients reach the shared rule through authentication without legacy role labels`() =
    assertPublicAuthorizedRequests(cloud, earlierAllow = false, callerToken = "user-without-roles")

  @Test
  fun `source paths reject missing rows failed lookups and a different token workspace before business`() {
    assertAll(
      Executable {
        blocked(SourcesController::class.java, "GET", "/sources/$SOURCE", "", 404) { data, _ ->
          every {
            data.savedSourceWorkspaceAndOrganization(SOURCE)
          } throws AuthorizationResourceNotFoundException()
        }
      },
      Executable {
        blocked(SourcesController::class.java, "GET", "/sources/$SOURCE", "", 500) { data, _ ->
          every {
            data.savedSourceWorkspaceAndOrganization(SOURCE)
          } throws IOException("ownership lookup unavailable")
        }
      },
      Executable {
        blocked(SourcesController::class.java, "GET", "/sources/$SOURCE", "", 403, token = "scoped:${UUID(0, 104)}") { data, _ ->
          every {
            data.savedSourceWorkspaceAndOrganization(SOURCE)
          } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        }
      },
    )
  }

  @Test
  fun `source reads keep user application owner matching scoped embedded and no auth responses`() {
    assertAll(
      listOf("user", "application-owner", "scoped:$WORKSPACE", "embedded-scoped:$WORKSPACE", "no-auth").map { token ->
        Executable {
          val security = token != "no-auth"
          RemainingOperationHttpSupport(
            cloud,
            listOf(SourcesController::class.java),
            setOf("io.airbyte.server.auth.PublicSourceReadRule"),
            security = security,
          ).run(
            configure = { context, business ->
              business.filterIsInstance<CurrentUserService>().forEach {
                every { it.getCurrentUser() } returns
                  AuthenticatedUser().withUserId(SOURCE).withAuthUserId("caller")
              }
              business.filterIsInstance<TrackingHelper>().forEach { tracker ->
                every { tracker.callWithTracker<SourceResponse>(any(), any(), any(), any()) } answers { firstArg<Callable<SourceResponse>>().call() }
                every { tracker.trackSuccess(any(), any(), any()) } just Runs
              }
              business.filterIsInstance<SourceService>().forEach { service ->
                every { service.getSource(SOURCE, true) } returns
                  SourceResponse(
                    SOURCE.toString(),
                    "fixture",
                    "postgres",
                    SOURCE.toString(),
                    WORKSPACE.toString(),
                    MoreMappers.initMapper().createObjectNode(),
                    0,
                  )
              }
              val data = mockk<AuthorizationDataService>()
              every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
              context.registerSingleton(AuthorizationDataService::class.java, data)
              val grants = mockk<PermissionService>()
              every { grants.getPermissionsByAuthUserId("caller") } returns
                listOf(Permission().withOrganizationId(ORGANIZATION).withPermissionType(Permission.PermissionType.ORGANIZATION_READER))
              context.registerSingleton(
                OperationPermissions::class.java,
                OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
              )
            },
            check = { _, server, _ ->
              val response =
                RemainingOperationHttpSupport.request(
                  server,
                  "GET",
                  "/api/public/v1/sources/$SOURCE?includeSecretCoordinates=true",
                  "",
                  token,
                )
              assertEquals(200, response.statusCode(), "$token: ${response.body()}")
              assertEquals(
                SOURCE.toString(),
                MoreMappers
                  .initMapper()
                  .readTree(response.body())
                  .get("sourceId")
                  .asText(),
              )
            },
          )
        }
      },
    )
  }

  @Test
  fun `source deletion rejects a saved connection with a different source before cascading`() {
    blocked(SourcesController::class.java, "DELETE", "/sources/$SOURCE", "", 400) { data, _ ->
      every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
      every { data.actorConnectionIds(SOURCE, WORKSPACE, true) } returns listOf(SOURCE)
      every { data.savedConnectionReferences(SOURCE) } returns
        SavedConnectionReferences(UUID(0, 104), SOURCE, Optional.empty(), Optional.empty(), emptyList())
    }
  }

  @Test
  fun `workspace patch rejects a region unavailable to the saved organization`() {
    blocked(WorkspacesController::class.java, "PATCH", "/workspaces/$WORKSPACE", """{"regionId":"$SOURCE"}""", 403) { data, _ ->
      every { data.savedWorkspaceAndOrganization(WORKSPACE, false) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
      every { data.canUseDataplaneGroup(SOURCE, ORGANIZATION) } returns false
    }
  }

  @Test
  fun `connection creation rejects actors from different saved workspaces before discovery`() {
    blocked(ConnectionsController::class.java, "POST", "/connections", """{"sourceId":"$SOURCE","destinationId":"$SOURCE"}""", 400) { data, _ ->
      every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
      every { data.savedDestinationWorkspaceAndOrganization(SOURCE) } returns WorkspaceAndOrganization(UUID(0, 104), ORGANIZATION)
    }
  }

  @Test
  fun `stream query authorizes both independently selected actors before schema discovery`() {
    blocked(StreamsController::class.java, "GET", "/streams?sourceId=$SOURCE&destinationId=$SOURCE", "", 403) { data, _ ->
      every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
      every { data.savedDestinationWorkspaceAndOrganization(SOURCE) } returns WorkspaceAndOrganization(UUID(0, 104), UUID(0, 105))
    }
  }

  @Test
  fun `definition path cannot read a definition unavailable to its workspace`() {
    blocked(DefinitionsController::class.java, "GET", "/workspaces/$WORKSPACE/definitions/sources/$SOURCE", "", 404) { data, _ ->
      every { data.workspaceAndOrganization(WORKSPACE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
      every { data.requireActorDefinitionType(SOURCE, true) } just Runs
      every { data.canUseActorDefinition(SOURCE, WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)) } returns false
    }
  }

  @Test
  fun `definition not found responses preserve definition resource type and id`() {
    assertAll(
      listOf("sources", "destinations", "declarative_sources").flatMap { kind ->
        listOf("missing", "unavailable", "wrong-kind").map { failure ->
          Executable {
            RemainingOperationHttpSupport(
              cloud,
              listOf(DefinitionsController::class.java),
              publicApiRuleNames(DefinitionsController::class.java, cloud),
            ).run(
              configure = { context, _ ->
                val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
                val data = mockk<AuthorizationDataService>()
                every { data.workspaceAndOrganization(WORKSPACE) } returns owner
                every { data.requireActorDefinitionType(SOURCE, kind != "destinations") } answers {
                  if (failure == "missing" || kind == "destinations" && failure == "wrong-kind") throw AuthorizationResourceNotFoundException()
                }
                every { data.canUseActorDefinition(SOURCE, owner) } returns (failure != "unavailable")
                context.registerSingleton(AuthorizationDataService::class.java, data)
                val definitions = mockk<PublicDefinitionAuthorizationService>()
                every { definitions.definition(SOURCE, any()) } answers {
                  if (kind == "destinations" && failure == "wrong-kind") throw AuthorizationResourceNotFoundException()
                  PublicDefinitionMetadata(true, kind != "declarative_sources")
                }
                context.registerSingleton(PublicDefinitionAuthorizationService::class.java, definitions)
                val grants = mockk<PermissionService>()
                every { grants.getPermissionsByAuthUserId("caller") } returns
                  listOf(
                    Permission().withWorkspaceId(WORKSPACE).withPermissionType(Permission.PermissionType.WORKSPACE_READER),
                  )
                context.registerSingleton(
                  OperationPermissions::class.java,
                  OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
                )
              },
              check = { _, server, business ->
                val response =
                  RemainingOperationHttpSupport.request(
                    server,
                    "GET",
                    "/api/public/v1/workspaces/$WORKSPACE/definitions/$kind/$SOURCE",
                    "",
                    "user",
                  )
                assertEquals(404, response.statusCode(), "$kind $failure: ${response.body()}")
                val data = MoreMappers.initMapper().readTree(response.body()).path("data")
                assertEquals(SOURCE.toString(), data.path("resourceId").asText(), "$kind $failure: ${response.body()}")
                assertEquals(
                  if (kind ==
                    "destinations"
                  ) {
                    "STANDARD_DESTINATION_DEFINITION"
                  } else {
                    "STANDARD_SOURCE_DEFINITION"
                  },
                  data.path("resourceType").asText(),
                  response.body(),
                )
                RemainingOperationHttpSupport.requireNoBusiness(business)
              },
            )
          }
        }
      },
    )
  }

  @Test
  fun `missing tag definition and connection actor responses preserve config resource data`() {
    assertAll(
      listOf("GET", "DELETE", "PATCH").map { method ->
        Executable {
          blocked(
            TagsController::class.java,
            method,
            "/tags/$SOURCE",
            if (method ==
              "PATCH"
            ) {
              """{"name":"fixture","color":"#123456"}"""
            } else {
              ""
            },
            404,
            expectedResource = Optional.of("TAG" to SOURCE.toString()),
          ) { data, _ ->
            every { data.tagWorkspaceAndOrganization(SOURCE) } throws AuthorizationResourceNotFoundException()
          }
        }
      } +
        listOf(true, false).flatMap { source ->
          val kind = if (source) "SOURCE" else "DESTINATION"
          listOf(
            Executable {
              blocked(
                if (source) SourcesController::class.java else DestinationsController::class.java,
                "POST",
                if (source) "/sources" else "/destinations",
                """{"name":"fixture","workspaceId":"$WORKSPACE","definitionId":"$SOURCE","configuration":{}}""",
                404,
                expectedResource =
                  Optional.of("STANDARD_${kind}_DEFINITION" to SOURCE.toString()),
              ) { data, _ ->
                every { data.workspaceAndOrganization(WORKSPACE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
                every { data.requireActorDefinitionType(SOURCE, source) } throws AuthorizationResourceNotFoundException()
              }
            },
            Executable {
              blocked(
                ConnectionsController::class.java,
                "POST",
                "/connections",
                """{"sourceId":"$SOURCE","destinationId":"$SOURCE"}""",
                404,
                expectedResource =
                  Optional.of("${kind}_CONNECTION" to SOURCE.toString()),
              ) { data, _ ->
                every { data.savedSourceWorkspaceAndOrganization(SOURCE) } answers {
                  if (source) throw AuthorizationResourceNotFoundException()
                  WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
                }
                every { data.savedDestinationWorkspaceAndOrganization(SOURCE) } throws AuthorizationResourceNotFoundException()
              }
            },
          )
        },
    )
  }

  @Test
  fun `nested group permission path rejects a saved permission belonging to another group`() {
    blocked(
      GroupPermissionsController::class.java,
      "DELETE",
      "/groups/$SOURCE/permissions/$SOURCE",
      "",
      404,
      expectedResource =
        Optional.of("permission" to SOURCE.toString()),
    ) { data, membership ->
      every { data.groupOrganization(SOURCE) } returns ORGANIZATION
      every { membership.permissionGroup(SOURCE) } returns Optional.of(UUID(0, 104))
    }
  }

  @Test
  fun `legacy GLOBAL permission role checks deny the default user without a security service`() {
    val handler = mockk<PermissionHandler>()
    val users = mockk<CurrentUserService>()
    val admin = Permission().withPermissionId(SOURCE).withUserId(DEFAULT_USER_ID).withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)
    every { handler.getPermissionById(SOURCE) } returns admin
    every { handler.getPermissionsByAuthUserId(DEFAULT_USER_ID.toString()) } returns listOf(admin)
    every { users.getCurrentUser() } returns AuthenticatedUser().withUserId(DEFAULT_USER_ID).withAuthUserId(DEFAULT_USER_ID.toString())
    val roles =
      RoleResolver(
        AuthenticationHeaderResolver(mockk(), handler, mockk(), mockk(), mockk(), mockk()),
        users,
        null,
        handler,
      ).newRequest()
        .withCurrentUser()
        .withRef(AuthenticationId.PERMISSION_ID, SOURCE)

    assertTrue(AuthRoleConstants.ADMIN in roles.roles())
    assertFalse(AuthRoleConstants.INSTANCE_ADMIN in roles.roles())
    assertThrows<ForbiddenProblem> { roles.requireRole(AuthRoleConstants.INSTANCE_ADMIN) }
  }

  @Test
  fun `GLOBAL permission reads and writes deny before business with or without auth`() {
    for (security in listOf(true, false)) {
      val grants = mockk<PermissionService>()
      val admin = Permission().withPermissionId(SOURCE).withUserId(SOURCE).withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)
      RemainingOperationHttpSupport(
        cloud,
        listOf(PermissionsController::class.java),
        setOf(PublicPermissionReadRule::class.java.name, PublicPermissionWriteRule::class.java.name),
        security = security,
      ).run(
        configure = { context, business ->
          configurePublicApiBusiness(business, cloud)
          val data = mockk<AuthorizationDataService>()
          every { data.permissionTarget(SOURCE) } returns AuthorizationTarget.Global
          context.registerSingleton(AuthorizationDataService::class.java, data)
          context.registerSingleton(
            OperationPermissions::class.java,
            OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
          )
          business.filterIsInstance<PermissionHandler>().forEach { handler ->
            every { handler.getPermissionById(SOURCE) } returns admin
            every { handler.getPermissionRead(any()) } returns
              io.airbyte.api.server.generated.models.PermissionRead(
                SOURCE,
                io.airbyte.api.server.generated.models.PermissionType.INSTANCE_ADMIN,
                userId = SOURCE,
              )
          }
        },
        check = { _, server, business ->
          every { grants.getPermissionsByAuthUserId("caller") } returns listOf(admin)
          assertAll(
            listOf("GET" to "", "DELETE" to "", "PATCH" to """{"permissionType":"instance_admin"}""").map { (method, body) ->
              Executable {
                val response =
                  RemainingOperationHttpSupport.request(
                    server,
                    method,
                    "/api/public/v1/permissions/$SOURCE",
                    body,
                    if (security) "user" else "",
                  )
                assertEquals(403, response.statusCode(), "$method, security=$security: ${response.body()}")
              }
            },
          )
          RemainingOperationHttpSupport.requireNoBusiness(business)
        },
      )
    }
  }

  @Test
  fun `workspace and organization permission reads and writes remain available with or without auth`() {
    for (security in listOf(true, false)) {
      val data = mockk<AuthorizationDataService>()
      val grants = mockk<PermissionService>()
      every { grants.getPermissionsByAuthUserId("caller") } returns
        listOf(Permission().withOrganizationId(ORGANIZATION).withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN))
      RemainingOperationHttpSupport(
        cloud,
        listOf(PermissionsController::class.java),
        setOf(PublicPermissionReadRule::class.java.name, PublicPermissionWriteRule::class.java.name),
        security = security,
      ).run(
        configure = { context, business ->
          configurePublicApiBusiness(business, cloud)
          context.registerSingleton(AuthorizationDataService::class.java, data)
          context.registerSingleton(
            OperationPermissions::class.java,
            OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
          )
        },
        check = { _, server, business ->
          for (target in listOf(
            AuthorizationTarget.Workspace(WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)),
            AuthorizationTarget.Organization(ORGANIZATION),
          )) {
            every { data.permissionTarget(SOURCE) } returns target
            val permission =
              io.airbyte.api.server.generated.models.PermissionRead(
                SOURCE,
                if (target is AuthorizationTarget.Workspace) {
                  io.airbyte.api.server.generated.models.PermissionType.WORKSPACE_READER
                } else {
                  io.airbyte.api.server.generated.models.PermissionType.ORGANIZATION_READER
                },
                userId = SOURCE,
                workspaceId = if (target is AuthorizationTarget.Workspace) WORKSPACE else null,
                organizationId = if (target is AuthorizationTarget.Organization) ORGANIZATION else null,
              )
            business.filterIsInstance<PermissionHandler>().forEach { handler ->
              every { handler.getPermissionRead(any()) } returns permission
            }
            val type = if (target is AuthorizationTarget.Workspace) "workspace_reader" else "organization_reader"
            for ((method, body, expected) in listOf(
              Triple("GET", "", 200),
              Triple("PATCH", """{"permissionType":"$type"}""", 200),
              Triple("DELETE", "", 204),
            )) {
              val response =
                RemainingOperationHttpSupport.request(server, method, "/api/public/v1/permissions/$SOURCE", body, if (security) "user" else "")
              assertEquals(expected, response.statusCode(), "$method, target=$target, security=$security: ${response.body()}")
              if (method != "DELETE") {
                val entity = MoreMappers.initMapper().readTree(response.body())
                assertEquals(SOURCE.toString(), entity.path("permissionId").asText())
                assertEquals(
                  if (target is AuthorizationTarget.Workspace) WORKSPACE.toString() else ORGANIZATION.toString(),
                  entity.path(if (target is AuthorizationTarget.Workspace) "workspaceId" else "organizationId").asText(),
                )
              }
            }
          }
        },
      )
    }
  }

  @Test
  fun `unknown application provider strings preserve the owned application not found response`() {
    RemainingOperationHttpSupport(
      cloud,
      listOf(ApplicationsController::class.java),
      publicApiRuleNames(ApplicationsController::class.java, cloud),
    ).run(
      configure = { context, _ ->
        val applications = mockk<ApplicationAuthorizationService>()
        every { applications.currentUserSubject("caller") } returns "caller"
        every { applications.ownsPublicApplication("caller", "provider-fixture") } returns false
        context.registerSingleton(ApplicationAuthorizationService::class.java, applications)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(mockk(), RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
        )
      },
      check = { _, server, business ->
        for (method in listOf("GET", "DELETE")) {
          val response = RemainingOperationHttpSupport.request(server, method, "/api/public/v1/applications/provider-fixture", "", "user")
          assertEquals(404, response.statusCode(), response.body())
          val problem = MoreMappers.initMapper().readTree(response.body())
          assertEquals("The application with the provided id was not found.", problem.path("detail").asText())
          RemainingOperationHttpSupport.requireNoBusiness(business)
        }
      },
    )
  }

  @Test
  fun `missing group and member responses retain their resource data`() {
    blocked(
      GroupsController::class.java,
      "GET",
      "/groups/$SOURCE",
      "",
      404,
      expectedResource = Optional.of("group" to SOURCE.toString()),
    ) { data, _ ->
      every { data.groupOrganization(SOURCE) } throws AuthorizationResourceNotFoundException()
    }
    blocked(
      GroupMembersController::class.java,
      "POST",
      "/groups/$SOURCE/members",
      """{"userId":"$SOURCE"}""",
      404,
      expectedResource =
        Optional.of("user" to SOURCE.toString()),
    ) { data, membership ->
      every { data.groupOrganization(SOURCE) } returns ORGANIZATION
      every { membership.isDirectOrganizationMember(SOURCE, ORGANIZATION) } throws AuthorizationResourceNotFoundException()
    }
  }

  private fun blocked(
    controller: Class<*>,
    method: String,
    path: String,
    body: String,
    expected: Int,
    token: String = "user",
    expectedResource: Optional<Pair<String, String>> = Optional.empty(),
    configure: (AuthorizationDataService, MembershipAuthorizationDataService) -> Unit,
  ) {
    val rules =
      controller.methods
        .mapNotNull { it.getAnnotation(AuthorizeOperation::class.java)?.rule?.java }
        .filter {
          it !=
            OperationRule::class.java
        }.toSet()
    RemainingOperationHttpSupport(cloud, listOf(controller), publicApiRuleNames(controller, cloud)).run(
      configure = { context, _ ->
        rules
          .flatMap {
            it.declaredConstructors
              .single { constructor -> !constructor.isSynthetic }
              .parameterTypes
              .toList()
          }.distinct()
          .filter {
            it !in
              setOf(AuthorizationDataService::class.java, MembershipAuthorizationDataService::class.java, OperationPermissions::class.java) &&
              !it.isPrimitive
          }.forEach { type ->
            @Suppress("UNCHECKED_CAST")
            context.registerSingleton(type as Class<Any>, mockkClass(type.kotlin, relaxed = true))
          }
        val data = mockk<AuthorizationDataService>()
        val membership = mockk<MembershipAuthorizationDataService>()
        configure(data, membership)
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(MembershipAuthorizationDataService::class.java, membership)
        val grants = mockk<PermissionService>()
        every { grants.getPermissionsByAuthUserId("caller") } returns
          listOf(Permission().withOrganizationId(ORGANIZATION).withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN))
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
        )
      },
      check = { _, server, business ->
        val response = RemainingOperationHttpSupport.request(server, method, "/api/public/v1$path", body, token)
        assertEquals(expected, response.statusCode(), response.body())
        expectedResource.ifPresent { (type, id) ->
          val data = MoreMappers.initMapper().readTree(response.body()).path("data")
          assertEquals(type, data.path("resourceType").asText(), response.body())
          assertEquals(id, data.path("resourceId").asText(), response.body())
        }
        RemainingOperationHttpSupport.requireNoBusiness(business)
      },
    )
  }

  @Test
  fun `every protected public endpoint rejects workload credentials before controller dependencies`() {
    assertAll(
      publicApiHttpScenarios().map { (ossController, requests) ->
        Executable {
          val controller =
            if (cloud && ossController.simpleName == "WorkspacesController") {
              Class.forName("io.airbyte.server.wrapped.apis.publicapi.controllers.CloudWorkspacesController")
            } else {
              ossController
            }
          val rules =
            controller.methods
              .mapNotNull { it.getAnnotation(AuthorizeOperation::class.java)?.rule?.java }
              .filter { it != OperationRule::class.java }
              .toSet()
          RemainingOperationHttpSupport(cloud, listOf(controller), publicApiRuleNames(controller, cloud)).run(
            configure = { context, _ ->
              rules
                .flatMap {
                  it.declaredConstructors
                    .single { constructor -> !constructor.isSynthetic }
                    .parameterTypes
                    .toList()
                }.distinct()
                .filter { it != OperationPermissions::class.java && !it.isPrimitive }
                .forEach { type ->
                  @Suppress("UNCHECKED_CAST")
                  context.registerSingleton(type as Class<Any>, mockkClass(type.kotlin, relaxed = true))
                }
              context.registerSingleton(
                OperationPermissions::class.java,
                OperationPermissions(mockk<PermissionService>(), RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
              )
            },
            check = { _, server, business ->
              assertAll(
                requests.map { request ->
                  Executable {
                    val response = RemainingOperationHttpSupport.request(server, request.method, request.path, request.body, "workload")
                    assertEquals(403, response.statusCode(), "${request.method} ${request.path}: ${response.body()}")
                    RemainingOperationHttpSupport.requireNoBusiness(business)
                  }
                },
              )
            },
          )
        }
      },
    )
  }

  companion object {
    val SOURCE: UUID = UUID(0, 101)
    val WORKSPACE: UUID = UUID(0, 102)
    val ORGANIZATION: UUID = UUID(0, 103)
  }
}
