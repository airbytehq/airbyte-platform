/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.server.generated.models.SaveStatsRequestBody
import io.airbyte.api.server.generated.models.SaveStreamAttemptMetadataRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.errors.handlers.UncaughtExceptionHandler
import io.airbyte.commons.server.handlers.AttemptHandler
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.auth.TokenType
import io.airbyte.data.repositories.AttemptsRepository
import io.airbyte.data.repositories.StreamAttemptMetadataRepository
import io.airbyte.data.repositories.entities.Attempt
import io.airbyte.data.repositories.entities.AttemptAuthorizationMetadata
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.StreamAttemptMetadataService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ActorWorkspaceOrganizationIds
import io.airbyte.data.services.shared.JobMetadata
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.server.apis.controllers.AttemptApiController
import io.airbyte.server.apis.controllers.DataplaneController
import io.airbyte.server.apis.controllers.ServiceAccountsController
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.security.filters.SecurityFilter
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import io.mockk.every
import io.mockk.isMockKMock
import io.mockk.mockk
import io.mockk.mockkClass
import io.mockk.spyk
import io.mockk.verify
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.Optional
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/** Both real attempt-write controllers, rules, ownership, permissions, handlers and metadata service. */
abstract class AttemptWriteAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `supported dataplane writes stats and metadata including attempt zero`() {
    request(status = 200)
  }

  @Test
  fun `empty metadata remains a valid successful write`() {
    request(status = 200, metadataBody = """{"jobId":$JOB,"attemptNumber":$ATTEMPT,"streamMetadata":[]}""")
  }

  @Test
  fun `human admin cannot write either attempt operation`() {
    request(token = "human-admin", status = 403)
  }

  @Test
  fun `humans application owners and unsupported identities cannot write with admin claims and saved grants`() {
    for (token in listOf("human", "application-owner", "embedded", "workload", "missing-type", "unknown-type", "non-string-type")) {
      request(status = 403, token = token) { fixture ->
        every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns
          listOf(grant("global", Permission.PermissionType.INSTANCE_ADMIN))
      }
    }
  }

  @Test
  fun `supported accounts require saved worker grants and preserve workspace organization and global inheritance`() {
    for (token in listOf("dataplane", "service-account")) {
      for (scope in listOf("workspace", "organization", "global")) {
        request(status = 200, token = token) { fixture ->
          every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns listOf(grant(scope))
        }
      }
      request(status = 200, token = token) { fixture ->
        every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns
          listOf(grant("global", Permission.PermissionType.INSTANCE_ADMIN))
      }
      for (scope in listOf("none", "other-workspace", "other-organization")) {
        request(status = 403, token = token) { fixture ->
          every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns if (scope == "none") emptyList() else listOf(grant(scope))
        }
      }
      for (type in listOf(Permission.PermissionType.WORKSPACE_EDITOR, Permission.PermissionType.WORKSPACE_READER)) {
        request(status = 403, token = token) { fixture ->
          every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns listOf(grant("workspace", type))
        }
      }
    }
  }

  @Test
  fun `internal client privilege is explicit and workspace token scope still applies`() {
    request(status = 200, token = "internal") { fixture -> every { fixture.permissions.getPermissionsByServiceAccountId(any()) } returns emptyList() }
    request(status = 200, token = "scoped")
    for (token in listOf("other-scope", "internal-other-scope")) request(status = 403, token = token)
  }

  @Test
  fun `stats connection may be omitted but a different permitted connection cannot select the write owner`() {
    request(status = 200, statsBody = statsBody(null))
    for (security in listOf(true, false)) {
      withAttemptServer(security = security) { fixture ->
        every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns listOf(grant("global"))
        every { fixture.connections.getStandardSync(OTHER_CONNECTION) } returns
          StandardSync().withConnectionId(OTHER_CONNECTION).withSourceId(SOURCE).withDestinationId(DESTINATION)
        val response = post(fixture.server, "/api/v1/attempt/save_stats", statsBody(OTHER_CONNECTION), if (security) "dataplane" else "")
        assertEquals(400, response.statusCode(), response.body())
        verify(exactly = 0) { fixture.handler.saveStats(any()) }
        fixture.verifyWrites(0)
        verify(exactly = 0) { fixture.connections.getStandardSync(OTHER_CONNECTION) }
      }
    }
  }

  @Test
  fun `composite attempt lookup cannot authorize an attempt belonging to another job or attempt number`() {
    for (saved in listOf(
      AttemptAuthorizationMetadata(id = ATTEMPT_ROW, jobId = JOB + 1, attemptNumber = ATTEMPT.toLong()),
      AttemptAuthorizationMetadata(id = ATTEMPT_ROW, jobId = JOB, attemptNumber = ATTEMPT + 1L),
      AttemptAuthorizationMetadata(id = null, jobId = JOB, attemptNumber = ATTEMPT.toLong()),
      AttemptAuthorizationMetadata(id = ATTEMPT_ROW, jobId = null, attemptNumber = ATTEMPT.toLong()),
      AttemptAuthorizationMetadata(id = ATTEMPT_ROW, jobId = JOB, attemptNumber = null),
    )) {
      request(status = 404) { fixture ->
        every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns listOf(grant("global"))
        every { fixture.attempts.findAuthorizationMetadata(JOB, ATTEMPT.toLong()) } returns Optional.of(saved)
      }
    }
  }

  @Test
  fun `all saved replication job types remain supported`() {
    for (type in Job.REPLICATION_TYPES) {
      request(status = 200) { fixture ->
        every { fixture.jobs.findJobMetadataById(JOB) } returns
          Optional.of(savedJob(type = type))
      }
    }
  }

  @Test
  fun `missing null and malformed required identifiers reject before ownership reads`() {
    for ((stats, metadata) in listOf(
      "{}" to "{}",
      "null" to "null",
      "" to "",
      "{" to "{",
      statsBody().replace("\"jobId\":$JOB,", "") to metadataBody().replace("\"jobId\":$JOB,", ""),
      statsBody().replace("\"attemptNumber\":$ATTEMPT,", "") to metadataBody().replace("\"attemptNumber\":$ATTEMPT,", ""),
      statsBody().replace("\"jobId\":$JOB", "\"jobId\":\"bad\"") to metadataBody().replace("\"jobId\":$JOB", "\"jobId\":\"bad\""),
      statsBody().replace("\"attemptNumber\":$ATTEMPT", "\"attemptNumber\":\"bad\"") to
        metadataBody().replace("\"attemptNumber\":$ATTEMPT", "\"attemptNumber\":\"bad\""),
    )) {
      request(status = 400, statsBody = stats, metadataBody = metadata) { fixture ->
        // Any attempted ownership lookup would be visible even if binding fails first.
        every { fixture.jobs.findJobMetadataById(any()) } throws AssertionError("invalid input read a saved job")
      }
    }
    withAttemptServer { fixture ->
      val response = post(fixture.server, "/api/v1/attempt/save_stats", statsBody().replace(CONNECTION.toString(), "bad"), "dataplane")
      assertEquals(400, response.statusCode(), response.body())
      fixture.verifyWrites(0)
      verify(exactly = 0) { fixture.jobs.findJobMetadataById(any()) }
    }
  }

  @Test
  fun `missing or inconsistent saved job attempt connection and owner graph return not found`() {
    for (missing in listOf(
      "job",
      "job-id",
      "job-type",
      "scope",
      "attempt",
      "connection",
      "connection-id",
      "source",
      "destination",
      "actor",
      "actor-id",
      "actor-org",
      "actor-workspace",
      "workspace",
      "workspace-id",
      "workspace-org",
      "organization",
      "organization-id",
    )) {
      request(status = 404) { fixture ->
        when (missing) {
          "job" -> every { fixture.jobs.findJobMetadataById(JOB) } returns Optional.empty()
          "job-id" -> every { fixture.jobs.findJobMetadataById(JOB) } returns Optional.of(savedJob(id = JOB + 1))
          "job-type" -> every { fixture.jobs.findJobMetadataById(JOB) } returns Optional.of(savedJob(type = JobConfig.ConfigType.DISCOVER_SCHEMA))
          "scope" -> every { fixture.jobs.findJobMetadataById(JOB) } returns Optional.of(savedJob(scope = "bad"))
          "attempt" -> {
            every { fixture.attempts.findAuthorizationMetadata(JOB, ATTEMPT.toLong()) } returns Optional.empty()
            every { fixture.attempts.findAuthorizationMetadata(JOB + 1, ATTEMPT.toLong()) } returns
              Optional.of(AttemptAuthorizationMetadata(id = ATTEMPT_ROW, jobId = JOB + 1, attemptNumber = ATTEMPT.toLong()))
          }
          "connection" ->
            every { fixture.connections.getStandardSync(CONNECTION) } throws
              ConfigNotFoundException(ConfigNotFoundType.STANDARD_SYNC, CONNECTION)
          "connection-id" -> every { fixture.connections.getStandardSync(CONNECTION) } returns StandardSync().withConnectionId(OTHER_CONNECTION)
          "source" ->
            every { fixture.connections.getStandardSync(CONNECTION) } returns
              StandardSync().withConnectionId(CONNECTION).withDestinationId(DESTINATION)
          "destination" ->
            every { fixture.connections.getStandardSync(CONNECTION) } returns
              StandardSync().withConnectionId(CONNECTION).withSourceId(SOURCE)
          "actor" -> every { fixture.actors.getIdsForActors(listOf(SOURCE)) } returns emptyList()
          "actor-id" ->
            every { fixture.actors.getIdsForActors(listOf(SOURCE)) } returns
              listOf(ActorWorkspaceOrganizationIds(DESTINATION, WORKSPACE, ORGANIZATION))
          "actor-org" ->
            every { fixture.actors.getIdsForActors(listOf(SOURCE)) } returns
              listOf(ActorWorkspaceOrganizationIds(SOURCE, WORKSPACE, null))
          "actor-workspace" ->
            every { fixture.actors.getIdsForActors(listOf(DESTINATION)) } returns
              listOf(ActorWorkspaceOrganizationIds(DESTINATION, OTHER_WORKSPACE, OTHER_ORGANIZATION))
          "workspace" ->
            every { fixture.workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } throws
              ConfigNotFoundException(ConfigNotFoundType.STANDARD_WORKSPACE, WORKSPACE)
          "workspace-id" ->
            every { fixture.workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } returns
              StandardWorkspace().withWorkspaceId(OTHER_WORKSPACE).withOrganizationId(ORGANIZATION)
          "workspace-org" ->
            every { fixture.workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } returns
              StandardWorkspace().withWorkspaceId(WORKSPACE).withOrganizationId(OTHER_ORGANIZATION)
          "organization" -> every { fixture.organizations.getOrganization(ORGANIZATION) } returns Optional.empty()
          "organization-id" ->
            every { fixture.organizations.getOrganization(ORGANIZATION) } returns
              Optional.of(Organization().withOrganizationId(OTHER_ORGANIZATION))
        }
      }
    }
  }

  @Test
  fun `ownership and permission failures surface before either handler and writes`() {
    for (failure in listOf("job", "attempt", "connection", "actor", "workspace", "organization", "permission", "unexpected-job")) {
      request(status = 500) { fixture ->
        when (failure) {
          "job" -> every { fixture.jobs.findJobMetadataById(JOB) } throws IOException("job unavailable")
          "unexpected-job" -> every { fixture.jobs.findJobMetadataById(JOB) } throws IllegalStateException("job unavailable")
          "attempt" -> every { fixture.attempts.findAuthorizationMetadata(JOB, ATTEMPT.toLong()) } throws IOException("attempt unavailable")
          "connection" -> every { fixture.connections.getStandardSync(CONNECTION) } throws IOException("connection unavailable")
          "actor" -> every { fixture.actors.getIdsForActors(any()) } throws IOException("actor unavailable")
          "workspace" -> every { fixture.workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } throws IOException("workspace unavailable")
          "organization" -> every { fixture.organizations.getOrganization(ORGANIZATION) } throws IOException("organization unavailable")
          "permission" -> every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } throws IOException("permissions unavailable")
        }
      }
    }
  }

  @Test
  fun `no auth skips caller permissions while retaining required saved relationships`() {
    request(status = 200, security = false, token = "") { fixture ->
      every { fixture.permissions.getPermissionsByServiceAccountId(any()) } throws
        AssertionError("noauth queried grants")
    }
    request(status = 404, security = false, token = "") { fixture -> every { fixture.jobs.findJobMetadataById(JOB) } returns Optional.empty() }
    request(status = 404, security = false, token = "") { fixture ->
      every { fixture.attempts.findAuthorizationMetadata(JOB, ATTEMPT.toLong()) } returns
        Optional.empty()
    }
    request(status = 400, security = false, token = "", statsBody = "{}", metadataBody = "{}")
  }

  @Test
  fun `authentication floor and earlier allowed results cannot bypass declared worker rules`() {
    request(status = 401, token = "")
    request(status = 403, token = "human-admin", earlierAllow = true)
    request(status = 403, earlierAllow = true) { fixture ->
      every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns
        emptyList()
    }
    request(status = 403, token = "other-scope", earlierAllow = true)
    request(status = 404, earlierAllow = true) { fixture -> every { fixture.jobs.findJobMetadataById(JOB) } returns Optional.empty() }
  }

  protected fun request(
    status: Int,
    token: String = "dataplane",
    statsBody: String = statsBody(),
    metadataBody: String = metadataBody(),
    security: Boolean = true,
    earlierAllow: Boolean = false,
    configure: (AttemptFixture) -> Unit = {},
  ) {
    withAttemptServer(security = security, earlierAllow = earlierAllow, configure = configure) { fixture ->
      assertAll(
        listOf("save_stats" to statsBody, "save_stream_metadata" to metadataBody).map { (path, body) ->
          org.junit.jupiter.api.function.Executable {
            val response = post(fixture.server, "/api/v1/attempt/$path", body, token)
            assertEquals(status, response.statusCode(), "$path with $body: ${response.body()}")
            if (status == 200) assertTrue(Jsons.deserialize(response.body())["succeeded"].asBoolean())
            if (status != 200) {
              assertFalse(response.body().contains(OTHER_WORKSPACE.toString()))
              assertFalse(response.body().contains(OTHER_ORGANIZATION.toString()))
            }
          }
        },
      )
      if (earlierAllow) {
        assertTrue(
          fixture.context
            .getBean(AttemptWriteEarlierAllow::class.java)
            .calls
            .get() > 0,
        )
      }
      if (!security) {
        verify(exactly = 0) { fixture.permissions.getPermissionsByAuthUserId(any()) }
        verify(exactly = 0) { fixture.permissions.getPermissionsByServiceAccountId(any()) }
      }
      if (status == 200) {
        fixture.verifyWrites(1, if (statsBody.contains("connectionId")) CONNECTION else null)
      } else {
        fixture.verifyWrites(0)
        verify(exactly = 0) { fixture.handler.saveStats(any()) }
        verify(exactly = 0) { fixture.handler.saveStreamMetadata(any()) }
      }
    }
  }

  protected fun withAttemptServer(
    security: Boolean = true,
    earlierAllow: Boolean = false,
    signed: Boolean = false,
    configure: (AttemptFixture) -> Unit = {},
    action: (AttemptFixture) -> Unit,
  ) {
    attemptContext(cloud, security, earlierAllow, signed).use { context ->
      val persistence = mockk<JobPersistence>(relaxed = true)
      val attempts = mockk<AttemptsRepository>()
      every { attempts.findByJobIdAndAttemptNumber(any(), any()) } returns null
      every { attempts.findByJobIdAndAttemptNumber(JOB, ATTEMPT.toLong()) } returns
        Attempt(id = ATTEMPT_ROW, jobId = JOB, attemptNumber = ATTEMPT.toLong())
      every { attempts.findAuthorizationMetadata(any(), any()) } returns Optional.empty()
      every { attempts.findAuthorizationMetadata(JOB, ATTEMPT.toLong()) } returns
        Optional.of(AttemptAuthorizationMetadata(id = ATTEMPT_ROW, jobId = JOB, attemptNumber = ATTEMPT.toLong()))
      val metadata = mockk<StreamAttemptMetadataRepository>(relaxed = true)
      val metadataService = spyk(StreamAttemptMetadataService(attempts, metadata))
      val jobs = mockk<JobService>()
      every { jobs.findJobMetadataById(any()) } returns Optional.empty()
      every { jobs.findJobMetadataById(JOB) } returns Optional.of(savedJob())
      val permissions = mockk<PermissionService>()
      every { permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns listOf(grant("organization"))
      every { permissions.getPermissionsByAuthUserId(any()) } returns listOf(grant("global", Permission.PermissionType.INSTANCE_ADMIN))
      val connections = mockk<ConnectionService>()
      every { connections.getStandardSync(CONNECTION) } returns
        StandardSync().withConnectionId(CONNECTION).withSourceId(SOURCE).withDestinationId(DESTINATION)
      val actors = mockk<ActorDefinitionService>()
      for (id in listOf(SOURCE, DESTINATION)) {
        every { actors.getIdsForActors(listOf(id)) } returns listOf(ActorWorkspaceOrganizationIds(id, WORKSPACE, ORGANIZATION))
      }
      val workspaces = mockk<WorkspaceService>()
      every { workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } returns
        StandardWorkspace().withWorkspaceId(WORKSPACE).withOrganizationId(ORGANIZATION)
      val organizations = mockk<OrganizationService>()
      every { organizations.getOrganization(ORGANIZATION) } returns Optional.of(Organization().withOrganizationId(ORGANIZATION))
      val dependencies =
        mapOf<Class<*>, Any>(
          JobService::class.java to jobs,
          AttemptsRepository::class.java to attempts,
          PermissionService::class.java to permissions,
          ConnectionService::class.java to connections,
          ActorDefinitionService::class.java to actors,
          WorkspaceService::class.java to workspaces,
          OrganizationService::class.java to organizations,
        )
      val dataConstructor = AuthorizationDataService::class.java.declaredConstructors.single { !it.isSynthetic }
      val data =
        dataConstructor.newInstance(
          *dataConstructor.parameterTypes
            .map {
              dependencies[it] ?: mockkClass(it.kotlin)
            }.toTypedArray(),
        ) as AuthorizationDataService
      val constructor = AttemptHandler::class.java.declaredConstructors.single { !it.isSynthetic }
      val handler =
        spyk(
          constructor.newInstance(
            *constructor.parameterTypes
              .map { type ->
                when (type) {
                  JobPersistence::class.java -> persistence
                  StreamAttemptMetadataService::class.java -> metadataService
                  else -> mockkClass(type.kotlin, relaxed = true)
                }
              }.toTypedArray(),
          ) as AttemptHandler,
        )
      for (parameter in AttemptApiController::class.java.declaredConstructors
        .single { !it.isSynthetic }
        .parameters) {
        @Suppress("UNCHECKED_CAST")
        val dependency = parameter.type as Class<Any>
        if (dependency != AttemptHandler::class.java &&
          dependency != ObjectMapper::class.java
        ) {
          context.registerSingleton(dependency, mockkClass(dependency.kotlin, relaxed = true))
        }
      }
      context.registerSingleton(AttemptHandler::class.java, handler)
      context.registerSingleton(WorkspaceService::class.java, workspaces)
      context.registerSingleton(PermissionService::class.java, permissions)
      context.registerSingleton(AuthorizationDataService::class.java, data)
      context.registerSingleton(
        OperationPermissions::class.java,
        OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
      )
      val fixture =
        AttemptFixture(
          context,
          persistence,
          attempts,
          metadata,
          metadataService,
          handler,
          jobs,
          connections,
          actors,
          workspaces,
          organizations,
          permissions,
        )
      configure(fixture)
      val attemptRules = setOf(SaveAttemptStatsRule::class.java, SaveAttemptStreamMetadataRule::class.java)
      registerSiblingOperationRules(context, AttemptApiController::class.java, attemptRules)
      if (signed) {
        registerSiblingOperationRules(context, DataplaneController::class.java, attemptRules)
        registerSiblingOperationRules(context, ServiceAccountsController::class.java, attemptRules)
      }
      context.start()
      assertFalse(isMockKMock(context.getBean(SaveAttemptStatsRule::class.java)))
      assertFalse(isMockKMock(context.getBean(SaveAttemptStreamMetadataRule::class.java)))
      assertFalse(isMockKMock(context.getBean(ObjectMapper::class.java)), "HTTP binding must use the real JSON mapper")
      assertEquals(security, context.containsBean(SecurityFilter::class.java))
      assertTrue(context.containsBean(UncaughtExceptionHandler::class.java))
      assertTrue(AttemptApiController::class.java.isInstance(context.getBean(AttemptApiController::class.java)))
      assertEquals(
        AttemptApiController::class.java,
        AttemptApiController::class.java.getMethod("saveStats", SaveStatsRequestBody::class.java).declaringClass,
      )
      assertEquals(
        AttemptApiController::class.java,
        AttemptApiController::class.java.getMethod("saveStreamMetadata", SaveStreamAttemptMetadataRequestBody::class.java).declaringClass,
      )
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        fixture.server = server
        action(fixture)
      }
    }
  }

  protected class AttemptFixture(
    val context: ApplicationContext,
    val persistence: JobPersistence,
    val attempts: AttemptsRepository,
    val metadata: StreamAttemptMetadataRepository,
    val metadataService: StreamAttemptMetadataService,
    val handler: AttemptHandler,
    val jobs: JobService,
    val connections: ConnectionService,
    val actors: ActorDefinitionService,
    val workspaces: WorkspaceService,
    val organizations: OrganizationService,
    val permissions: PermissionService,
  ) {
    lateinit var server: EmbeddedServer

    fun verifyWrites(
      count: Int,
      connectionId: UUID? = CONNECTION,
    ) {
      verify(exactly = count) { persistence.writeStats(JOB, ATTEMPT, 0L, 0L, 0L, 0L, 0L, 0L, 0L, connectionId, match { it.isEmpty() }) }
      verify(exactly = count) { metadataService.upsertStreamAttemptMetadata(JOB, ATTEMPT.toLong(), any()) }
      verify(exactly = count) {
        metadata.saveAll(
          match<List<io.airbyte.data.repositories.entities.StreamAttemptMetadata>> { rows ->
            rows.all { it.attemptId == ATTEMPT_ROW && it.streamName == "orders" && it.wasBackfilled == true && it.wasResumed == false }
          },
        )
      }
      if (count == 0) {
        verify(exactly = 0) { persistence.writeStats(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
        verify(exactly = 0) { metadata.updateAll(any<List<io.airbyte.data.repositories.entities.StreamAttemptMetadata>>()) }
      }
    }
  }

  protected fun post(
    server: EmbeddedServer,
    path: String,
    body: String,
    token: String = "",
  ): HttpResponse<String> =
    HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build().use { client ->
      val builder =
        java.net.http.HttpRequest
          .newBuilder(URI("${server.url}$path"))
          .header("Content-Type", "application/json")
      if (token.isNotEmpty()) builder.header("Authorization", "Bearer $token")
      client.send(
        builder
          .POST(
            java.net.http.HttpRequest.BodyPublishers
              .ofString(body),
          ).build(),
        HttpResponse.BodyHandlers.ofString(),
      )
    }

  companion object {
    const val JOB = 71L
    const val ATTEMPT = 0
    const val ATTEMPT_ROW = 901L
    val SOURCE: UUID = UUID(0, 1)
    val WORKSPACE: UUID = UUID(0, 2)
    val ORGANIZATION: UUID = UUID(0, 3)
    val CONNECTION: UUID = UUID(0, 4)
    val ACCOUNT: UUID = UUID(0, 5)
    val OTHER_WORKSPACE: UUID = UUID(0, 6)
    val OTHER_ORGANIZATION: UUID = UUID(0, 7)
    val DESTINATION: UUID = UUID(0, 8)
    val OTHER_CONNECTION: UUID = UUID(0, 9)

    fun savedJob(
      id: Long = JOB,
      type: JobConfig.ConfigType = JobConfig.ConfigType.SYNC,
      scope: String = CONNECTION.toString(),
    ): JobMetadata = JobMetadata(id, type, scope)

    fun statsBody(connection: UUID? = CONNECTION): String =
      """{"jobId":$JOB,"attemptNumber":$ATTEMPT,"stats":{"recordsEmitted":0,"bytesEmitted":0,"recordsCommitted":0,"bytesCommitted":0,"recordsRejected":0,"estimatedRecords":0,"estimatedBytes":0},"streamStats":[]${connection?.let {
        ",\"connectionId\":\"$it\""
      } ?: ""}}"""

    fun metadataBody(): String =
      """{"jobId":$JOB,"attemptNumber":$ATTEMPT,"streamMetadata":[{"streamName":"orders","wasBackfilled":true,"wasResumed":false}]}"""

    fun grant(
      scope: String,
      type: Permission.PermissionType = Permission.PermissionType.DATAPLANE,
    ): Permission =
      Permission().withPermissionType(type).apply {
        when (scope) {
          "workspace" -> withWorkspaceId(WORKSPACE)
          "organization" -> withOrganizationId(ORGANIZATION)
          "other-workspace" -> withWorkspaceId(OTHER_WORKSPACE)
          "other-organization" -> withOrganizationId(OTHER_ORGANIZATION)
        }
      }
  }
}

private fun attemptContext(
  cloud: Boolean,
  security: Boolean,
  earlierAllow: Boolean,
  signed: Boolean,
): ApplicationContext {
  val allowed =
    listOf(
      "io.airbyte.server.apis.controllers.AttemptApiController",
      "io.airbyte.server.auth.AttemptWrite",
      "io.airbyte.server.auth.SaveAttempt",
      "io.airbyte.server.auth.AuthorizedRequest",
      "io.airbyte.server.auth.AuthorizationInterceptor",
      "io.airbyte.server.auth.RouteAccess",
      "io.airbyte.commons.server.errors.problems.AbstractThrowableProblemHandler",
      "io.airbyte.commons.server.errors.handlers.UncaughtExceptionHandler",
    ) +
      if (signed) {
        listOf(
          "io.airbyte.server.apis.controllers.DataplaneController",
          "io.airbyte.server.apis.controllers.ServiceAccountsController",
          "io.airbyte.commons.server.authorization.AuthenticationFactory",
        )
      } else {
        emptyList()
      }
  val isolated =
    object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
      override fun isWithin(reference: BeanDefinitionReference<*>): Boolean = isWithin(reference.beanDefinitionName.replace("$", ""))

      override fun isWithin(type: Class<*>): Boolean = isWithin(type.name)

      override fun isWithin(name: String): Boolean = name.startsWith("io.airbyte.") && allowed.none(name::startsWith)
    }
  return ApplicationContext
    .builder()
    .enableDefaultPropertySources(false)
    .deduceEnvironment(false)
    .environmentPropertySource(false)
    .environments(if (cloud) "cloud" else "test")
    .beanConfigurations(isolated)
    .properties(
      mapOf(
        "spec.name" to "AttemptWriteAuthorizationHttpTest",
        "test.attempt.signed" to signed,
        "test.attempt.earlier-allow" to earlierAllow,
        "micronaut.security.enabled" to security,
        "micronaut.security.filter.enabled" to security,
        "micronaut.security.token.jwt.enabled" to signed,
        "micronaut.security.token.jwt.signatures.secret.generator.secret" to "catalog-test-signature-secret-long-enough-for-hs256",
        "micronaut.server.port" to -1,
        "micronaut.metrics.enabled" to false,
      ),
    ).build()
}

@Singleton
@Requires(property = "spec.name", value = "AttemptWriteAuthorizationHttpTest")
@Requires(property = "test.attempt.signed", value = "false")
class AttemptWriteAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val token =
      request.headers.authorization
        .orElse("")
        .removePrefix("Bearer ")
    val attributes: MutableMap<String, Any> =
      when (token) {
        "dataplane", "scoped", "other-scope" -> mapOf(TokenType.DATAPLANE_V1.toClaim())
        "service-account" -> mapOf(TokenType.SERVICE_ACCOUNT.toClaim())
        "internal", "internal-other-scope" -> mapOf(TokenType.INTERNAL_CLIENT.toClaim())
        "human", "application-owner", "human-admin" -> mapOf(TokenType.USER.toClaim())
        "embedded" -> mapOf(TokenType.EMBEDDED_V1.toClaim())
        "workload" -> mapOf(TokenType.WORKLOAD_API.toClaim())
        "missing-type" -> emptyMap()
        "unknown-type" -> mapOf("typ" to "io.airbyte.auth.unknown")
        "non-string-type" -> mapOf("typ" to 7)
        else -> return Publishers.empty()
      }.toMutableMap()
    if (token in
      listOf("scoped", "other-scope", "internal-other-scope")
    ) {
      attributes[TokenScopeClaim.CLAIM_ID] =
        TokenScopeClaim(
          (
            if (token ==
              "scoped"
            ) {
              AttemptWriteAuthorizationHttpContract.WORKSPACE
            } else {
              AttemptWriteAuthorizationHttpContract.OTHER_WORKSPACE
            }
          ).toString(),
        )
    }
    return Publishers.just(
      Authentication.build(
        AttemptWriteAuthorizationHttpContract.ACCOUNT.toString(),
        listOf(AuthRoleConstants.AUTHENTICATED_USER, AuthRoleConstants.DATAPLANE, AuthRoleConstants.ADMIN),
        attributes,
      ),
    )
  }
}

@Singleton
@Requires(property = "spec.name", value = "AttemptWriteAuthorizationHttpTest")
@Requires(property = "test.attempt.earlier-allow", value = "true")
class AttemptWriteEarlierAllow : SecurityRule<HttpRequest<*>> {
  val calls = AtomicInteger()

  override fun getOrder(): Int = Int.MIN_VALUE

  override fun check(
    request: HttpRequest<*>,
    authentication: Authentication?,
  ): Publisher<SecurityRuleResult> {
    calls.incrementAndGet()
    return Publishers.just(SecurityRuleResult.ALLOWED)
  }
}
