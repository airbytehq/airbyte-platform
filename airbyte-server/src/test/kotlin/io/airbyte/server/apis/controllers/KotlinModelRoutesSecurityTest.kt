/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.ActorType
import io.airbyte.api.server.generated.models.AuthConfiguration
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectorDocumentationRead
import io.airbyte.api.server.generated.models.ConnectorDocumentationRequestBody
import io.airbyte.api.server.generated.models.DeploymentMetadataRead
import io.airbyte.api.server.generated.models.HealthCheckRead
import io.airbyte.api.server.generated.models.InstanceConfigurationResponse
import io.airbyte.api.server.generated.models.InstanceConfigurationSetupRequestBody
import io.airbyte.api.server.generated.models.JobIdRequestBody
import io.airbyte.api.server.generated.models.JobRetryStateRequestBody
import io.airbyte.api.server.generated.models.MigrateSecretStorageRequestBody
import io.airbyte.api.server.generated.models.Notification
import io.airbyte.api.server.generated.models.NotificationRead
import io.airbyte.api.server.generated.models.NotificationTrigger
import io.airbyte.api.server.generated.models.NotificationType
import io.airbyte.api.server.generated.models.NotificationWebhookConfigValidationRequestBody
import io.airbyte.api.server.generated.models.Pagination
import io.airbyte.api.server.generated.models.RetryStateRead
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.api.server.generated.models.SecretStorageCreateRequestBody
import io.airbyte.api.server.generated.models.SecretStorageIdRequestBody
import io.airbyte.api.server.generated.models.SecretStorageListRequestBody
import io.airbyte.api.server.generated.models.SignalInput
import io.airbyte.api.server.generated.models.SlackNotificationConfiguration
import io.airbyte.api.server.generated.models.StreamStatusCreateRequestBody
import io.airbyte.api.server.generated.models.StreamStatusJobType
import io.airbyte.api.server.generated.models.StreamStatusListRequestBody
import io.airbyte.api.server.generated.models.StreamStatusRead
import io.airbyte.api.server.generated.models.StreamStatusReadList
import io.airbyte.api.server.generated.models.StreamStatusRunState
import io.airbyte.api.server.generated.models.StreamStatusUpdateRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.ConnectorDocumentationHandler
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler
import io.airbyte.commons.server.handlers.HealthCheckHandler
import io.airbyte.commons.server.handlers.InstanceConfigurationHandler
import io.airbyte.commons.server.handlers.NotificationsHandler
import io.airbyte.commons.server.handlers.OpenApiConfigHandler
import io.airbyte.commons.server.handlers.SignalHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.services.secrets.SecretMigrationService
import io.airbyte.domain.services.secrets.SecretStorageCredentialsService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.server.handlers.RetryStatesHandler
import io.airbyte.server.handlers.StreamStatusesHandler
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
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.io.File
import java.time.OffsetDateTime
import java.util.UUID

private const val SPEC_NAME = "KotlinModelRoutesSecurityTest"
private const val TEST_ROLES_HEADER = "X-Test-Roles"

/**
 * Calls every route that moved to the Kotlin-generated server models, with security turned on.
 * Routes now come from the generated interface, so a route that silently moved or lost its
 * security annotation shows up here as a 404 or a wrong status.
 */
@MicronautTest(environments = ["test"])
@Property(name = "spec.name", value = SPEC_NAME)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class KotlinModelRoutesSecurityTest {
  @Requires(property = "spec.name", value = SPEC_NAME)
  @Factory
  class TestFactory {
    @Singleton @Primary
    fun healthCheckHandler(): HealthCheckHandler = mockk()

    @Singleton @Primary
    fun deploymentMetadataHandler(): DeploymentMetadataHandler = mockk()

    @Singleton @Primary
    fun openApiConfigHandler(): OpenApiConfigHandler = mockk()

    @Singleton @Primary
    fun instanceConfigurationHandler(): InstanceConfigurationHandler = mockk()

    @Singleton @Primary
    fun notificationsHandler(): NotificationsHandler = mockk()

    @Singleton @Primary
    fun signalHandler(): SignalHandler = mockk()

    @Singleton @Primary
    fun streamStatusesHandler(): StreamStatusesHandler = mockk()

    @Singleton @Primary
    fun retryStatesHandler(): RetryStatesHandler = mockk()

    @Singleton @Primary
    fun connectorDocumentationHandler(): ConnectorDocumentationHandler = mockk()

    @Singleton @Primary
    fun secretStorageService(): SecretStorageService = mockk()

    @Singleton @Primary
    fun secretStorageCredentialsService(): SecretStorageCredentialsService = mockk()

    @Singleton @Primary
    fun secretMigrationService(): SecretMigrationService = mockk()

    @Singleton @Primary
    fun currentUserService(): CurrentUserService = mockk()

    @Singleton @Primary
    fun roleResolver(): RoleResolver = mockk(relaxed = true)
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject lateinit var healthCheckHandler: HealthCheckHandler

  @Inject lateinit var deploymentMetadataHandler: DeploymentMetadataHandler

  @Inject lateinit var openApiConfigHandler: OpenApiConfigHandler

  @Inject lateinit var instanceConfigurationHandler: InstanceConfigurationHandler

  @Inject lateinit var notificationsHandler: NotificationsHandler

  @Inject lateinit var signalHandler: SignalHandler

  @Inject lateinit var streamStatusesHandler: StreamStatusesHandler

  @Inject lateinit var retryStatesHandler: RetryStatesHandler

  @Inject lateinit var connectorDocumentationHandler: ConnectorDocumentationHandler

  @Inject lateinit var secretStorageService: SecretStorageService

  @Inject lateinit var secretStorageCredentialsService: SecretStorageCredentialsService

  @Inject lateinit var secretMigrationService: SecretMigrationService

  @Inject lateinit var currentUserService: CurrentUserService

  @BeforeEach
  fun stubHandlers() {
    every { healthCheckHandler.health() } returns HealthCheckRead(available = true)
    every { deploymentMetadataHandler.getDeploymentMetadata() } returns
      DeploymentMetadataRead(id = UUID.randomUUID(), mode = "COMMUNITY", version = "1.0.0")
    every { openApiConfigHandler.file } returns File.createTempFile("openapi", ".yaml").apply { writeText("openapi: 3.0.0") }
    every { instanceConfigurationHandler.instanceConfiguration } returns instanceConfigurationResponse()
    every { instanceConfigurationHandler.setupInstanceConfiguration(any()) } returns instanceConfigurationResponse()
    every { notificationsHandler.tryNotification(any(), any()) } returns NotificationRead(status = NotificationRead.Status.SUCCEEDED)
    justRun { signalHandler.signal(any()) }
    every { streamStatusesHandler.createStreamStatus(any()) } returns streamStatusRead()
    every { streamStatusesHandler.updateStreamStatus(any()) } returns streamStatusRead()
    every { streamStatusesHandler.listStreamStatus(any()) } returns StreamStatusReadList(streamStatuses = listOf(streamStatusRead()))
    every { streamStatusesHandler.listStreamStatusPerRunState(any()) } returns StreamStatusReadList(streamStatuses = listOf(streamStatusRead()))
    every { retryStatesHandler.getByJobId(any()) } returns retryStateRead()
    justRun { retryStatesHandler.putByJobId(any()) }
    every { connectorDocumentationHandler.getConnectorDocumentation(any()) } returns ConnectorDocumentationRead(doc = "docs")
    every { currentUserService.getCurrentUser() } returns mockk(relaxed = true) { every { userId } returns UUID.randomUUID() }
    every { secretStorageService.createSecretStorage(any(), any()) } returns secretStorage()
    justRun { secretStorageCredentialsService.writeStorageCredentials(any(), any(), any(), any()) }
    every { secretStorageService.deleteSecretStorage(any(), any()) } returns secretStorage()
    every { secretStorageService.getById(any()) } returns secretStorage()
    every { secretStorageService.listSecretStorage(any(), any()) } returns listOf(secretStorage())
    justRun { secretMigrationService.migrateSecrets(any(), any(), any(), any()) }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `allowed caller gets the expected status`(route: Route) {
    // the blocking client throws on 4xx, and some routes answer with one on purpose
    val status =
      runCatching { client.toBlocking().exchange(route.request(route.allowedRoles), String::class.java).status }
        .getOrElse { (it as HttpClientResponseException).status }

    assertThat(status).isEqualTo(route.allowedStatus)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("protectedRoutes")
  fun `caller without access is rejected`(route: Route) {
    val deniedRoles = route.deniedRoles!!
    val error =
      runCatching { client.toBlocking().exchange(route.request(deniedRoles), String::class.java) }
        .exceptionOrNull() as HttpClientResponseException

    // no roles at all means the caller is not logged in
    val expected = if (deniedRoles.isEmpty()) HttpStatus.UNAUTHORIZED else HttpStatus.FORBIDDEN
    assertThat(error.status).isEqualTo(expected)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routesWithBody")
  fun `body missing required fields gets a client error`(route: Route) {
    val request =
      HttpRequest
        .POST(route.path, "{}")
        .contentType(MediaType.APPLICATION_JSON_TYPE)
        .withRoles(route.allowedRoles)
    val error =
      runCatching { client.toBlocking().exchange(request, String::class.java) }
        .exceptionOrNull() as HttpClientResponseException

    assertThat(error.status.code).isBetween(400, 499)
  }

  /**
   * @param allowedRoles roles that may call the route; empty means anyone, logged in or not
   * @param deniedRoles roles that must be rejected; null means nobody is rejected, empty means "not logged in"
   */
  data class Route(
    val name: String,
    val path: String,
    val body: Any?,
    val allowedRoles: List<String>,
    val deniedRoles: List<String>?,
    val allowedStatus: HttpStatus = HttpStatus.OK,
    val isGet: Boolean = false,
  ) {
    fun request(roles: List<String>): MutableHttpRequest<*> {
      val request = if (isGet) HttpRequest.GET<Any>(path) else HttpRequest.POST(path, body ?: "")
      return request.withRoles(roles)
    }

    override fun toString() = name
  }

  companion object {
    private val admin = listOf(AuthRoleConstants.ADMIN)
    private val workspaceReader = listOf(AuthRoleConstants.WORKSPACE_READER)
    private val authenticatedUser = listOf(AuthRoleConstants.AUTHENTICATED_USER)
    private val notLoggedIn = emptyList<String>()

    @JvmStatic
    fun routes(): List<Route> =
      listOf(
        Route("health", "/api/v1/health", null, notLoggedIn, null, isGet = true),
        Route("deployment metadata", "/api/v1/deployment/metadata", null, authenticatedUser, notLoggedIn),
        Route("openapi", "/api/v1/openapi", null, authenticatedUser, workspaceReader, isGet = true),
        Route("instance configuration get", "/api/v1/instance_configuration", null, notLoggedIn, null, isGet = true),
        Route("instance configuration setup", "/api/v1/instance_configuration/setup", setupRequestBody(), notLoggedIn, null),
        // deprecated no-op endpoint, see NotificationsApiController
        Route(
          "notifications try",
          "/api/v1/notifications/try",
          Notification(notificationType = NotificationType.SLACK),
          authenticatedUser,
          workspaceReader,
          allowedStatus = HttpStatus.UNPROCESSABLE_ENTITY,
        ),
        Route(
          "notifications try webhook",
          "/api/v1/notifications/try_webhook",
          NotificationWebhookConfigValidationRequestBody(
            slackConfiguration = SlackNotificationConfiguration(webhook = "https://example.com"),
            notificationTrigger = NotificationTrigger.SYNC_SUCCESS,
          ),
          authenticatedUser,
          workspaceReader,
        ),
        Route("signal", "/api/v1/signal", SignalInput(workflowType = "sync", workflowId = "workflow"), admin, workspaceReader),
        Route("stream statuses create", "/api/v1/stream_statuses/create", streamStatusCreate(), admin, workspaceReader, HttpStatus.CREATED),
        Route("stream statuses update", "/api/v1/stream_statuses/update", streamStatusUpdate(), admin, workspaceReader),
        Route("stream statuses list", "/api/v1/stream_statuses/list", streamStatusList(), workspaceReader, authenticatedUser),
        Route(
          "stream statuses latest per run state",
          "/api/v1/stream_statuses/latest_per_run_state",
          ConnectionIdRequestBody(connectionId = UUID.randomUUID()),
          workspaceReader,
          authenticatedUser,
        ),
        Route("job retry states get", "/api/v1/jobs/retry_states/get", JobIdRequestBody(id = 1L), admin, workspaceReader),
        Route(
          "job retry states create or update",
          "/api/v1/jobs/retry_states/create_or_update",
          retryStateRequest(),
          admin,
          workspaceReader,
          HttpStatus.NO_CONTENT,
        ),
        Route(
          "connector documentation get",
          "/api/v1/connector_documentation/get",
          connectorDocumentationRequest(),
          workspaceReader,
          authenticatedUser,
        ),
        Route("secret storage create", "/api/v1/secret_storage/create", secretStorageCreate(), admin, workspaceReader),
        Route(
          "secret storage delete",
          "/api/v1/secret_storage/delete",
          SecretStorageIdRequestBody(secretStorageId = UUID.randomUUID()),
          admin,
          workspaceReader,
        ),
        // role checks for this route happen inside the controller (RoleResolver), so only login is enforced here
        Route(
          "secret storage get",
          "/api/v1/secret_storage/get",
          SecretStorageIdRequestBody(secretStorageId = UUID.randomUUID()),
          authenticatedUser,
          notLoggedIn,
        ),
        Route(
          "secret storage list",
          "/api/v1/secret_storage/list",
          SecretStorageListRequestBody(scopeType = ScopeType.WORKSPACE, scopeId = UUID.randomUUID()),
          admin,
          workspaceReader,
        ),
        Route(
          "secret storage migrate",
          "/api/v1/secret_storage/migrate",
          MigrateSecretStorageRequestBody(
            fromSecretStorageId = UUID.randomUUID(),
            toSecretStorageId = UUID.randomUUID(),
            scopeType = ScopeType.WORKSPACE,
            scopeId = UUID.randomUUID(),
          ),
          admin,
          workspaceReader,
        ),
      )

    @JvmStatic
    fun protectedRoutes(): List<Route> = routes().filter { it.deniedRoles != null }

    @JvmStatic
    fun routesWithBody(): List<Route> = routes().filter { it.body != null }

    private fun setupRequestBody() =
      InstanceConfigurationSetupRequestBody(
        email = "test@airbyte.io",
        anonymousDataCollection = false,
        initialSetupComplete = true,
        displaySetupWizard = false,
      )

    private fun streamStatusCreate() =
      StreamStatusCreateRequestBody(
        workspaceId = UUID.randomUUID(),
        connectionId = UUID.randomUUID(),
        jobId = 1L,
        jobType = StreamStatusJobType.SYNC,
        attemptNumber = 0,
        streamName = "stream",
        runState = StreamStatusRunState.PENDING,
        transitionedAt = 0L,
      )

    private fun streamStatusUpdate() =
      StreamStatusUpdateRequestBody(
        id = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        connectionId = UUID.randomUUID(),
        jobId = 1L,
        jobType = StreamStatusJobType.SYNC,
        attemptNumber = 0,
        streamName = "stream",
        runState = StreamStatusRunState.PENDING,
        transitionedAt = 0L,
      )

    private fun streamStatusList() =
      StreamStatusListRequestBody(
        workspaceId = UUID.randomUUID(),
        connectionId = UUID.randomUUID(),
        pagination = Pagination(pageSize = 10, rowOffset = 0),
      )

    private fun retryStateRequest() =
      JobRetryStateRequestBody(
        connectionId = UUID.randomUUID(),
        jobId = 1L,
        successiveCompleteFailures = 0,
        totalCompleteFailures = 0,
        successivePartialFailures = 0,
        totalPartialFailures = 0,
      )

    private fun connectorDocumentationRequest() =
      ConnectorDocumentationRequestBody(
        actorType = ActorType.SOURCE,
        actorDefinitionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
      )

    private fun secretStorageCreate() =
      SecretStorageCreateRequestBody(
        descriptor = "storage",
        secretStorageType = io.airbyte.api.server.generated.models.SecretStorageType.VAULT,
        scopeType = ScopeType.ORGANIZATION,
        scopeId = UUID.randomUUID(),
        config = Jsons.jsonNode(mapOf("key" to "value")),
      )
  }
}

private fun MutableHttpRequest<*>.withRoles(roles: List<String>): MutableHttpRequest<*> =
  if (roles.isEmpty()) this else header(TEST_ROLES_HEADER, roles.joinToString(","))

private fun instanceConfigurationResponse() =
  InstanceConfigurationResponse(
    edition = InstanceConfigurationResponse.Edition.COMMUNITY,
    version = "1.0.0",
    auth = AuthConfiguration(mode = AuthConfiguration.Mode.NONE),
    airbyteUrl = "http://localhost:8000",
    initialSetupComplete = true,
    defaultUserId = UUID.randomUUID(),
    defaultOrganizationId = UUID.randomUUID(),
    defaultOrganizationEmail = "test@airbyte.io",
    trackingStrategy = InstanceConfigurationResponse.TrackingStrategy.LOGGING,
  )

private fun streamStatusRead() =
  StreamStatusRead(
    id = UUID.randomUUID(),
    workspaceId = UUID.randomUUID(),
    connectionId = UUID.randomUUID(),
    jobId = 1L,
    jobType = StreamStatusJobType.SYNC,
    attemptNumber = 0,
    streamName = "stream",
    runState = StreamStatusRunState.PENDING,
    transitionedAt = 0L,
  )

private fun retryStateRead() =
  RetryStateRead(
    id = UUID.randomUUID(),
    connectionId = UUID.randomUUID(),
    jobId = 1L,
    successiveCompleteFailures = 0,
    totalCompleteFailures = 0,
    successivePartialFailures = 0,
    totalPartialFailures = 0,
  )

private fun secretStorage() =
  SecretStorage(
    id = SecretStorageId(UUID.randomUUID()),
    scopeType = SecretStorageScopeType.WORKSPACE,
    scopeId = UUID.randomUUID(),
    descriptor = "storage",
    storageType = SecretStorageType.VAULT,
    // skips hydrating the config from the secret store
    configuredFromEnvironment = true,
    createdBy = UUID.randomUUID(),
    updatedBy = UUID.randomUUID(),
    createdAt = OffsetDateTime.now(),
    updatedAt = OffsetDateTime.now(),
  )

/** Logs the caller in with whatever roles the test puts in [TEST_ROLES_HEADER]; no header means not logged in. */
@Requires(property = "spec.name", value = SPEC_NAME)
@Singleton
class RoleHeaderAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val roles = request.headers[TEST_ROLES_HEADER] ?: return Flux.empty()
    return Flux.just(Authentication.build("routes-security-test-user", roles.split(",")))
  }
}
