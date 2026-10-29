/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.RunCheckCommandRequest
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.auth.TokenType
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.repositories.entities.Actor
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ActorWorkspaceOrganizationIds
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.services.CommandService
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.Optional
import java.util.UUID
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType as JooqActorType

/** The same production route and rules are exercised on OSS and Cloud classpaths. */
abstract class CheckCommandAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `saved source and destination reject a different supplied workspace even with both permissions`() {
    for (actor in listOf(SOURCE, DESTINATION)) {
      request(""""actor_id":"$actor","workspace_id":"$OTHER"""", 400)
      request(""""actor_id":"$actor","workspace_id":"$OTHER","config":{}""", 400)
    }
  }

  @Test
  fun `denied saved actors reject before comparing supplied workspaces`() {
    for (actor in listOf(SOURCE, DESTINATION)) {
      request(""""actor_id":"$actor","workspace_id":"$OTHER"""", 403, granted = false)
      request(""""actor_id":"$actor","workspace_id":"$OTHER","config":{}""", 403, granted = false)
    }
  }

  @Test
  fun `scoped token denials reject before comparing supplied workspaces`() {
    for (actor in listOf(SOURCE, DESTINATION)) {
      request(""""actor_id":"$actor","workspace_id":"$OTHER"""", 403, token = "other-scope")
      request(""""actor_id":"$actor","workspace_id":"$OTHER","config":{}""", 403, token = "other-scope")
    }
  }

  @Test
  fun `saved actor only and matching workspace remain supported`() {
    for (actor in listOf(SOURCE, DESTINATION)) {
      request(""""actor_id":"$actor"""", 200)
      request(""""actor_id":"$actor","workspace_id":"$WORKSPACE"""", 200)
      request(""""actor_id":"$actor","workspace_id":"$WORKSPACE","actor_definition_id":"$DEFINITION"""", 200)
      request(""""actor_id":"$actor","actor_definition_id":"$DEFINITION","config":{}""", 200)
      request(""""actor_id":"$actor","config":{"password":"**********"}""", 200)
    }
  }

  @Test
  fun `inline definition workspace and configuration remain supported`() {
    request(""""workspace_id":"$WORKSPACE","actor_definition_id":"$DEFINITION","config":{}""", 200)
  }

  @Test
  fun `missing or malformed check input is rejected before sensitive work`() {
    for (fields in listOf(
      "",
      """"id":null""",
      """"id":""""",
      """"workspace_id":"$WORKSPACE","actor_definition_id":"$DEFINITION"""",
      """"workspace_id":"$WORKSPACE","actor_definition_id":"$DEFINITION","config":[]""",
      """"actor_id":"$SOURCE","attempt_number":-1""",
      """"workspace_id":"$WORKSPACE"""",
      """"actor_definition_id":"$DEFINITION","config":{}""",
      """"workspace_id":"broken","config":{}""",
    )) {
      request(fields, 400)
    }
  }

  @Test
  fun `missing saved actor and workspace preserve not found`() {
    request(""""actor_id":"$SOURCE"""", 404, missingActor = true)
    request(""""workspace_id":"$WORKSPACE","actor_definition_id":"$DEFINITION","config":{}""", 404, missingWorkspace = true)
  }

  @Test
  fun `denied permissions and saved ownership failures never reach configuration`() {
    request(""""actor_id":"$SOURCE"""", 403, granted = false)
    request(""""actor_id":"$DESTINATION"""", 500, lookupFailure = true)
    request(""""actor_id":"$SOURCE"""", 500, permissionFailure = true)
  }

  @Test
  fun `saved source rejects conflicting definition before configuration reads`() {
    request(""""actor_id":"$SOURCE","actor_definition_id":"$OTHER"""", 400)
    request(""""actor_id":"$SOURCE","actor_definition_id":"$OTHER","config":{}""", 400)
  }

  @Test
  fun `saved destination rejects conflicting definition before configuration reads`() {
    request(""""actor_id":"$DESTINATION","actor_definition_id":"$OTHER"""", 400)
    request(""""actor_id":"$DESTINATION","actor_definition_id":"$OTHER","config":{}""", 400)
  }

  @Test
  fun `saved definition lookup preserves missing resources and dependency failures`() {
    for (actor in listOf(SOURCE, DESTINATION)) {
      val fields = """"actor_id":"$actor","actor_definition_id":"$DEFINITION","config":{}"""
      request(fields, 404, missingSavedDefinition = true)
      request(fields, 500, savedDefinitionFailure = true)
    }
  }

  @Test
  fun `tracking references preserve strings zero and independent optional values`() {
    for (tracking in listOf(""""job_id":"workflow-check","attempt_number":0""", """"job_id":"workflow-check"""", """"attempt_number":0""")) {
      request(""""actor_id":"$SOURCE",$tracking""", 200)
    }
  }

  @Test
  fun `configured no auth still checks ownership relationships`() {
    request(""""actor_id":"$SOURCE"""", 200, security = false)
    request(""""actor_id":"$SOURCE","workspace_id":"$OTHER"""", 400, security = false)
  }

  @Test
  fun `authentication is still required`() {
    request(""""actor_id":"$SOURCE"""", 401, token = "")
  }

  @Test
  fun `inline definitions must exist and be available in the checked workspace`() {
    val inline = """"workspace_id":"$WORKSPACE","actor_definition_id":"$DEFINITION","config":{}"""
    request(inline, 404, missingDefinition = true)
    request(inline, 403, definitionGranted = false)
    request(inline, 500, definitionFailure = true)
  }

  @Test
  fun `scoped embedded internal and worker caller policies remain enforced`() {
    for (token in listOf("embedded", "internal", "worker", "scoped")) {
      request(""""actor_id":"$SOURCE"""", 200, token = token)
    }
    request(""""actor_id":"$SOURCE"""", 403, token = "other-scope")
  }

  private fun request(
    fields: String,
    status: Int,
    granted: Boolean = true,
    missingActor: Boolean = false,
    missingWorkspace: Boolean = false,
    lookupFailure: Boolean = false,
    permissionFailure: Boolean = false,
    security: Boolean = true,
    token: String = "valid",
    missingDefinition: Boolean = false,
    definitionGranted: Boolean = true,
    definitionFailure: Boolean = false,
    missingSavedDefinition: Boolean = false,
    savedDefinitionFailure: Boolean = false,
  ) {
    checkCommandContext(cloud, security).use { context ->
      val workspaces = mockk<WorkspaceService>()
      for (id in listOf(WORKSPACE, OTHER)) {
        every { workspaces.getStandardWorkspaceNoSecrets(id, false) } returns StandardWorkspace().withWorkspaceId(id).withOrganizationId(ORGANIZATION)
      }
      if (missingWorkspace) {
        every { workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } throws
          ConfigNotFoundException(ConfigNotFoundType.STANDARD_WORKSPACE, WORKSPACE)
      }
      val actors = mockk<ActorDefinitionService>()
      for (actor in listOf(SOURCE, DESTINATION)) {
        every { actors.getIdsForActors(listOf(actor)) } returns
          if (missingActor) emptyList() else listOf(ActorWorkspaceOrganizationIds(actor, WORKSPACE, ORGANIZATION))
      }
      if (lookupFailure) every { actors.getIdsForActors(any()) } throws IOException("ownership unavailable")
      every { actors.getDefaultVersionForActorDefinitionIdOptional(DEFINITION) } returns
        Optional.of(ActorDefinitionVersion().withActorDefinitionId(DEFINITION))
      every { actors.scopeCanUseDefinition(DEFINITION, any(), any()) } returns definitionGranted
      if (missingDefinition) every { actors.getDefaultVersionForActorDefinitionIdOptional(DEFINITION) } returns Optional.empty()
      if (definitionFailure) every { actors.getDefaultVersionForActorDefinitionIdOptional(DEFINITION) } throws IOException("definition unavailable")
      val permissions = mockk<PermissionService>()
      every { permissions.getPermissionsByAuthUserId(any()) } returns
        if (granted) {
          listOf(WORKSPACE, OTHER).map {
            Permission().withWorkspaceId(it).withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          }
        } else {
          emptyList()
        }
      every { permissions.getPermissionsByServiceAccountId(any()) } returns
        listOf(Permission().withWorkspaceId(WORKSPACE).withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN))
      if (permissionFailure) every { permissions.getPermissionsByAuthUserId(any()) } throws IOException("permissions unavailable")
      val actorRepository = mockk<ActorRepository>()
      for ((actor, type) in listOf(SOURCE to JooqActorType.source, DESTINATION to JooqActorType.destination)) {
        every { actorRepository.findByActorId(actor) } returns
          Actor(actor, WORKSPACE, DEFINITION, "saved-$type", Jsons.emptyObject(), type)
        every { actorRepository.findActorDefinitionIdByIdAndWorkspaceId(actor, WORKSPACE) } returns
          if (missingSavedDefinition) Optional.empty() else Optional.of(DEFINITION)
      }
      if (savedDefinitionFailure) {
        every { actorRepository.findActorDefinitionIdByIdAndWorkspaceId(any(), any()) } throws IOException("saved definition unavailable")
      }
      val data =
        AuthorizationDataService(
          workspaces,
          permissions,
          actors,
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          privateLinks = mockk(),
          connectionEvents = mockk(),
          actorRepository = actorRepository,
          attempts = mockk(),
          entitlements = mockk(),
          featureFlags = mockk(),
        )
      val commands =
        spyk(
          CommandService(
            actorRepository = actorRepository,
            catalogService = mockk(),
            commandsRepository = mockk(),
            jobInputService = mockk(),
            logClientManager = mockk(),
            organizationService = mockk(),
            workloadService = mockk(),
            workloadQueueService = mockk(),
            workloadOutputReader = mockk(),
            workloadIdGenerator = mockk(),
            workspaceService = workspaces,
            secretSanitizer = mockk(),
            configurationUpdate = mockk(),
            sourceService = mockk(),
            destinationService = mockk(),
            actorDefinitionVersionHelper = mockk(),
            jsonSchemaValidator = mockk(),
            airbyteConfig = mockk(),
            airbyteWorkerConfig = mockk(relaxed = true),
            featureFlagClient = mockk(),
            blockedAddressMatcher = mockk(),
            scopedConfigurationService = mockk(),
            connectRateLimiter = mockk(),
            dataplaneGroupService = mockk(),
            clock = java.time.Clock.systemUTC(),
          ),
        )
      every { commands.processCheckConfig(any(), any(), any(), any()) } answers { lastArg() }
      every { commands.createCheckCommand(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns true
      val legacyRoles = mockk<RoleResolver>(relaxed = true)
      val roleRequest = mockk<RoleResolver.Request>(relaxed = true)
      every { legacyRoles.newRequest() } returns roleRequest
      every { roleRequest.withCurrentAuthentication() } returns roleRequest
      every { roleRequest.withRef(any(), any<UUID>()) } returns roleRequest
      context.registerSingleton(AuthorizationDataService::class.java, data)
      context.registerSingleton(
        OperationPermissions::class.java,
        OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
      )
      context.registerSingleton(CommandService::class.java, commands)
      context.registerSingleton(RoleResolver::class.java, legacyRoles)
      context.registerSingleton(ActorRepository::class.java, actorRepository)
      context.registerSingleton(WorkspaceService::class.java, workspaces)
      context.registerSingleton(
        io.airbyte.commons.server.handlers.helpers.CatalogConverter::class.java,
        mockk<io.airbyte.commons.server.handlers.helpers.CatalogConverter>(),
      )
      context.registerSingleton(ApiPojoConverters::class.java, mockk<ApiPojoConverters>())
      context.registerSingleton(io.airbyte.commons.logging.LogUtils::class.java, mockk<io.airbyte.commons.logging.LogUtils>())
      registerSiblingOperationRules(context, io.airbyte.server.apis.controllers.CommandApiController::class.java, setOf(CheckCommandRule::class.java))
      context.start()
      org.junit.jupiter.api.Assertions
        .assertFalse(io.mockk.isMockKMock(context.getBean(CheckCommandRule::class.java)))
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val builder =
            java.net.http.HttpRequest
              .newBuilder(
                URI("${server.url}/api/v1/commands/run/check"),
              ).header("Content-Type", "application/json")
          if (token.isNotEmpty()) builder.header("Authorization", "Bearer $token")
          val body = """{"id":"checked-command"${if (fields.isEmpty()) "" else ",$fields"}}"""
          val response =
            client.send(
              builder
                .POST(
                  java.net.http.HttpRequest.BodyPublishers
                    .ofString(body),
                ).build(),
              HttpResponse.BodyHandlers.ofString(),
            )
          assertEquals(status, response.statusCode(), response.body())
          if (status == 200) {
            val parsed = Jsons.deserialize(body, RunCheckCommandRequest::class.java)
            verify {
              commands.createCheckCommand(
                commandId = parsed.id,
                actorId = parsed.actorId,
                actorDefinitionId = parsed.actorDefinitionId,
                workspaceId = parsed.workspaceId,
                configuration = any(),
                jobId = parsed.jobId,
                attemptNumber = parsed.attemptNumber?.toLong(),
                workloadPriority = any(),
                signalInput = parsed.signalInput,
                commandInput = Jsons.jsonNode(parsed),
              )
            }
            if (fields.contains("config")) verify { commands.processCheckConfig(any(), DEFINITION, WORKSPACE, any()) }
          } else {
            verify(exactly = 0) { actorRepository.findByActorId(any()) }
            verify(exactly = 0) { commands.getCheckCommandContext(any(), any(), any()) }
            verify(exactly = 0) { commands.processCheckConfig(any(), any(), any(), any()) }
            verify(exactly = 0) { commands.createCheckCommand(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
          }
        }
      }
    }
  }

  companion object {
    val SOURCE: UUID = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val DESTINATION: UUID = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val WORKSPACE: UUID = UUID.fromString("00000000-0000-0000-0000-000000000003")
    val OTHER: UUID = UUID.fromString("00000000-0000-0000-0000-000000000004")
    val DEFINITION: UUID = UUID.fromString("00000000-0000-0000-0000-000000000005")
    val ORGANIZATION: UUID = UUID.fromString("00000000-0000-0000-0000-000000000006")
  }
}

fun checkCommandContext(
  cloud: Boolean = false,
  security: Boolean = true,
): ApplicationContext {
  val allowed =
    listOf(
      "io.airbyte.server.apis.controllers.CommandApiController",
      "io.airbyte.server.auth.CheckCommand",
      "io.airbyte.server.auth.AuthorizedRequest",
      "io.airbyte.server.auth.AuthorizationInterceptor",
      "io.airbyte.server.auth.RouteAccess",
      "io.airbyte.commons.server.errors.problems.AbstractThrowableProblemHandler",
    )
  val isolated =
    object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
      override fun isWithin(reference: BeanDefinitionReference<*>): Boolean = isWithin(reference.beanDefinitionName.replace("$", ""))

      override fun isWithin(type: Class<*>): Boolean = isWithin(type.name)

      override fun isWithin(name: String): Boolean = name.startsWith("io.airbyte.") && allowed.none(name::startsWith)
    }
  return ApplicationContext
    .builder()
    .enableDefaultPropertySources(
      false,
    ).deduceEnvironment(false)
    .environmentPropertySource(false)
    .environments(if (cloud) "cloud" else "test")
    .beanConfigurations(isolated)
    .properties(
      mapOf(
        "spec.name" to "CheckCommandAuthorizationHttpTest",
        "micronaut.security.enabled" to security,
        "micronaut.server.port" to -1,
        "micronaut.metrics.enabled" to false,
      ),
    ).build()
}

@Singleton
@Requires(property = "spec.name", value = "CheckCommandAuthorizationHttpTest")
class CheckCommandAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val token =
      request.headers.authorization
        .orElse("")
        .removePrefix("Bearer ")
    val attributes =
      when (token) {
        "embedded" ->
          mapOf(
            TokenType.EMBEDDED_V1.toClaim(),
            TokenScopeClaim.CLAIM_ID to TokenScopeClaim(CheckCommandAuthorizationHttpContract.WORKSPACE.toString()),
          )
        "internal" -> mapOf(TokenType.INTERNAL_CLIENT.toClaim())
        "worker" -> mapOf(TokenType.SERVICE_ACCOUNT.toClaim())
        "scoped" -> mapOf(TokenScopeClaim.CLAIM_ID to TokenScopeClaim(CheckCommandAuthorizationHttpContract.WORKSPACE.toString()))
        "other-scope" -> mapOf(TokenScopeClaim.CLAIM_ID to TokenScopeClaim(CheckCommandAuthorizationHttpContract.OTHER.toString()))
        "valid" -> emptyMap()
        else -> return Publishers.empty()
      }
    return Publishers.just(Authentication.build(if (token == "worker") UUID(0, 7).toString() else "caller", emptyList(), attributes))
  }
}
