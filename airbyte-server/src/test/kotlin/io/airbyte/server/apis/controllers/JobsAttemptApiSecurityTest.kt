/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.handlers.AttemptHandler
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.JobInputHandler
import io.airbyte.commons.server.handlers.JobsHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.temporal.StreamResetRecordsHelper
import io.airbyte.server.services.JobObservabilityService
import io.airbyte.workload.services.WorkloadService
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.MutableHttpRequest
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.util.UUID

private const val SPEC = "JobsAttemptApiSecurityTest"
private const val TEST_ROLES_HEADER = "X-Test-Roles"

/**
 * Exercises every jobs, attempt and workload output route over HTTP with security enabled, at the
 * exact path clients call, so a route moved by the controller's generated interface fails here rather
 * than as a generic 403 in prod.
 */
@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class JobsAttemptApiSecurityTest {
  @Requires(property = "spec.name", value = SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun jobHistoryHandler(): JobHistoryHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun schedulerHandler(): SchedulerHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun jobInputHandler(): JobInputHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun jobsHandler(): JobsHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun streamResetRecordsHelper(): StreamResetRecordsHelper = mockk(relaxed = true)

    @Singleton
    @Primary
    fun jobObservabilityService(): JobObservabilityService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun attemptHandler(): AttemptHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun jobConverter(): JobConverter = mockk(relaxed = true)

    @Singleton
    @Primary
    fun workloadService(): WorkloadService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun roleResolver(): RoleResolver = mockk(relaxed = true)

    @Singleton
    @Primary
    @Named("outputDocumentStore")
    fun outputDocumentStore(): StorageClient = mockk(relaxed = true)
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `route is reachable with its role`(route: Route) {
    assertThat(statusOf(route.request(), route.allowedRole))
      .withFailMessage { "${route.path} with ${route.allowedRole} should be routed and authorized" }
      .isNotIn(HttpStatus.UNAUTHORIZED, HttpStatus.FORBIDDEN, HttpStatus.NOT_FOUND, HttpStatus.METHOD_NOT_ALLOWED)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("securedRoutes")
  fun `route is forbidden without its role`(route: Route) {
    assertThat(statusOf(route.request(), AuthRoleConstants.AUTHENTICATED_USER)).isEqualTo(HttpStatus.FORBIDDEN)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `missing required field is a client error`(route: Route) {
    val status = statusOf(route.request(body = emptyMap()), route.allowedRole)
    assertThat(status.code)
      .withFailMessage { "${route.path} with an empty body returned $status" }
      .isBetween(400, 499)
      .isNotIn(HttpStatus.UNAUTHORIZED.code, HttpStatus.FORBIDDEN.code, HttpStatus.NOT_FOUND.code)
  }

  private fun statusOf(
    request: MutableHttpRequest<*>,
    role: String,
  ): HttpStatus =
    try {
      client.toBlocking().exchange(request.header(TEST_ROLES_HEADER, role), Any::class.java).status
    } catch (e: HttpClientResponseException) {
      e.status
    }

  data class Route(
    val path: String,
    val allowedRole: String,
    val body: Map<String, Any>,
  ) {
    fun request(body: Map<String, Any> = this.body): MutableHttpRequest<*> = HttpRequest.POST(path, body).contentType(MediaType.APPLICATION_JSON_TYPE)

    override fun toString() = "POST $path"
  }

  companion object {
    private val id = UUID.randomUUID().toString()
    private const val JOBS = "/api/v1/jobs"
    private const val ATTEMPT = "/api/v1/attempt"
    private const val ADMIN = AuthRoleConstants.ADMIN
    private const val READER = AuthRoleConstants.WORKSPACE_READER
    private const val RUNNER = AuthRoleConstants.WORKSPACE_RUNNER
    private const val AUTHENTICATED = AuthRoleConstants.AUTHENTICATED_USER

    private val jobId = mapOf("id" to 1)
    private val attempt = mapOf("jobId" to 1, "attemptNumber" to 0)

    private val routes =
      listOf(
        Route("$JOBS/cancel", RUNNER, jobId),
        Route("$JOBS/cancelQueuedJob", ADMIN, mapOf("jobId" to 1)),
        Route("$JOBS/checkDataWorkerCapacity", ADMIN, mapOf("jobId" to 1, "connectionId" to id, "organizationId" to id)),
        Route("$JOBS/create", RUNNER, mapOf("connectionId" to id, "isScheduled" to false)),
        Route("$JOBS/delete_stream_reset_records", ADMIN, mapOf("connectionId" to id, "jobId" to 1)),
        Route("$JOBS/did_previous_job_succeed", ADMIN, mapOf("connectionId" to id, "jobId" to 1)),
        Route("$JOBS/evaluate_outlier", ADMIN, jobId),
        Route("$JOBS/explain", READER, jobId),
        Route("$JOBS/fail_non_terminal", ADMIN, mapOf("connectionId" to id)),
        Route("$JOBS/finalize", ADMIN, jobId),
        Route("$JOBS/get_debug_info", READER, jobId),
        Route("$JOBS/get", READER, jobId),
        Route("$JOBS/get_light", READER, jobId),
        Route("$JOBS/get_without_logs", READER, jobId),
        Route("$JOBS/get_input", READER, attempt),
        Route("$JOBS/get_last_replication_job", READER, mapOf("connectionId" to id)),
        Route("$JOBS/get_last_replication_job_with_cancel", READER, mapOf("connectionId" to id)),
        Route("$JOBS/getWebhookConfig", READER, mapOf("jobId" to 1)),
        Route("$JOBS/job_failure", ADMIN, attempt + mapOf("connectionId" to id, "reason" to "r")),
        Route("$JOBS/job_success_with_attempt_number", ADMIN, attempt + ("connectionId" to id)),
        Route("$JOBS/list", READER, mapOf("configTypes" to listOf("sync"))),
        Route("$JOBS/list_for_workspaces", READER, mapOf("configTypes" to listOf("sync"))),
        Route(
          "$JOBS/persist_cancellation",
          ADMIN,
          attempt + mapOf("connectionId" to id, "attemptFailureSummary" to emptyMap<String, Any>()),
        ),
        Route("$JOBS/reportJobStart", ADMIN, mapOf("jobId" to 1, "connectionId" to id)),
        Route("$JOBS/setJobQueued", ADMIN, mapOf("jobId" to 1)),
        Route("$ATTEMPT/create_new_attempt_number", ADMIN, mapOf("jobId" to 1)),
        Route("$ATTEMPT/fail", ADMIN, attempt),
        Route("$ATTEMPT/get_combined_stats", READER, attempt),
        Route("$ATTEMPT/get_for_job", READER, attempt),
        Route("$ATTEMPT/save_stats", AUTHENTICATED, attempt + ("stats" to emptyMap<String, Any>())),
        Route("$ATTEMPT/save_stream_metadata", AUTHENTICATED, attempt),
        Route(
          "$ATTEMPT/save_sync_config",
          ADMIN,
          attempt + ("syncConfig" to mapOf("sourceConfiguration" to emptyMap<String, Any>(), "destinationConfiguration" to emptyMap<String, Any>())),
        ),
        Route("/api/v1/workload_output/write", AUTHENTICATED, mapOf("workloadId" to "w", "output" to "o")),
      )

    @JvmStatic
    fun routes() = routes

    @JvmStatic
    fun securedRoutes() = routes.filter { it.allowedRole != AUTHENTICATED }
  }
}

@Requires(property = "spec.name", value = SPEC)
@Singleton
class JobsAttemptApiAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val roles = listOfNotNull(AuthRoleConstants.AUTHENTICATED_USER, request.headers[TEST_ROLES_HEADER]).distinct()
    return Flux.just(Authentication.build("jobs-security-test-user", roles))
  }
}
