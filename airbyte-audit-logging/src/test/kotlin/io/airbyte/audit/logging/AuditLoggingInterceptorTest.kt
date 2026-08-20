/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

import io.airbyte.api.model.generated.PermissionCreate
import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.audit.logging.model.Actor
import io.airbyte.audit.logging.provider.AuditProvider
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.AuditLoggingEntitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.StoreAuditLogs
import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.micronaut.aop.MethodInvocationContext
import io.micronaut.context.ApplicationContext
import io.micronaut.context.event.ShutdownEvent
import io.micronaut.context.event.StartupEvent
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.type.MutableArgumentValue
import io.micronaut.http.HttpHeaders
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.http.server.netty.NettyHttpRequest
import io.micronaut.inject.qualifiers.Qualifiers
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.spyk
import io.mockk.unmockkAll
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class AuditLoggingInterceptorTest {
  private lateinit var interceptor: AuditLoggingInterceptor
  private lateinit var context: MethodInvocationContext<Any, Any>
  private lateinit var applicationContext: ApplicationContext
  private lateinit var auditLoggingHelper: AuditLoggingHelper
  private lateinit var auditScopeResolver: AuditScopeResolver
  private lateinit var entitlementService: EntitlementService
  private lateinit var airbyteStorageConfig: AirbyteStorageConfig
  private lateinit var storageClientFactory: StorageClientFactory
  private lateinit var featureFlagClient: FeatureFlagClient

  @BeforeEach
  fun setUp() {
    context = mockk()
    applicationContext = mockk()
    auditLoggingHelper = mockk()
    auditScopeResolver = mockk()
    entitlementService = mockk()
    storageClientFactory = mockk(relaxed = true)
    featureFlagClient = mockk()
    airbyteStorageConfig = mockk(relaxed = true)
  }

  @AfterEach
  fun tearDown() {
    unmockkAll()
  }

  @Test
  fun `skips the entry when the feature flag is disabled for the organization`() {
    val organizationId = UUID.randomUUID()
    val storageClient = mockk<StorageClient>()
    every { storageClient.write(any(), any()) } just Runs
    every { storageClientFactory.create(DocumentType.AUDIT_LOGS) } returns storageClient
    every { airbyteStorageConfig.bucket.auditLogging } returns "test-audit-log-bucket"
    every { featureFlagClient.boolVariation(StoreAuditLogs, Organization(organizationId)) } returns false

    interceptor = buildInterceptor()
    interceptor.onStartupEvent(mockk<StartupEvent>())
    interceptor.logAuditInfo(
      actor = Actor("userId", "email", null, "userAgent"),
      operationName = "updateOrganization",
      request = "{}",
      response = null,
      success = true,
      organizationId = organizationId,
    )
    interceptor.onShutdownEvent(mockk<ShutdownEvent>())

    verify(exactly = 0) { entitlementService.checkEntitlement(any(), any()) }
    verify(exactly = 0) { storageClient.write(any(), any()) }
  }

  @Test
  fun `should proceed the request and log the result`() {
    every { airbyteStorageConfig.bucket.log } returns "test-audit-log-bucket"
    every { airbyteStorageConfig.bucket.auditLogging } returns "test-audit-log-bucket"
    interceptor =
      spyk(
        AuditLoggingInterceptor(
          applicationContext,
          auditLoggingHelper,
          auditScopeResolver,
          entitlementService,
          featureFlagClient,
          airbyteStorageConfig,
          storageClientFactory,
        ),
      )
    val request = mockk<NettyHttpRequest<Any>>()
    val headers = mockk<HttpHeaders>()

    val actionName = "createPermission"
    every { context.methodName } returns actionName
    every { request.headers } returns headers
    every { headers.get("User-Agent") } returns "userAgent"
    every { headers.get("X-Forwarded-For") } returns null
    every { auditLoggingHelper.buildActor(headers) } returns Actor("userId", "email", null, "userAgent")

    val storageClient = mockk<StorageClient>()
    every { storageClient.write(any(), any()) } just Runs
    every { storageClientFactory.create(DocumentType.AUDIT_LOGS) } returns storageClient
    every { featureFlagClient.boolVariation(StoreAuditLogs, any()) } returns true

    val parameterValue = mockk<MutableArgumentValue<Any>>()
    val permissionUpdate =
      PermissionCreate().apply {
        permissionId = UUID.randomUUID()
        permissionType = PermissionType.WORKSPACE_EDITOR
      }
    every { parameterValue.value } returns permissionUpdate

    val parameters = mutableMapOf<String, MutableArgumentValue<*>>("permissionCreate" to parameterValue)
    every { context.parameters } returns parameters

    mockkStatic(ServerRequestContext::class)
    every { ServerRequestContext.currentRequest<Any>() } returns Optional.of(request)

    // Mock the audit logging annotation
    val auditLoggingAnnotation = mockk<AnnotationValue<AuditLogging>>()
    every { context.getAnnotation(AuditLogging::class.java) } returns auditLoggingAnnotation
    every { auditLoggingAnnotation.stringValue("provider") } returns Optional.of("testProvider")

    // Mock the application context to return a fake provider
    val fakeProvider = mockk<AuditProvider>()
    every { fakeProvider.generateSummaryFromRequest(any()) } returns "{}"
    every { fakeProvider.generateSummaryFromResult(any()) } returns "{\"result\": \"summary\"}"
    every { auditLoggingHelper.generateSummary(any(), any()) } returns "{}"
    every { applicationContext.findBean(AuditProvider::class.java, Qualifiers.byName("testProvider")) } returns Optional.of(fakeProvider)

    val targetUserId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    every { context.proceed() } returns
      PermissionRead()
        .userId(targetUserId)
        .workspaceId(workspaceId)
        .organizationId(null)
        .permissionType(PermissionType.WORKSPACE_EDITOR)

    val resolvedOrganizationId = UUID.randomUUID()
    every { auditScopeResolver.resolveScope(any(), any(), any()) } returns
      ResolvedAuditScope(organizationId = resolvedOrganizationId, workspaceId = workspaceId)
    every { entitlementService.checkEntitlement(OrganizationId(resolvedOrganizationId), AuditLoggingEntitlement) } returns
      EntitlementResult(featureId = AuditLoggingEntitlement.featureId, isEntitled = true)

    interceptor.intercept(context)
    // Verifying that request is proceeded
    verify { context.proceed() }
    // Verify logAuditInfo was called with the correct parameters, including the resolved scope
    verify {
      interceptor.logAuditInfo(
        actor = match { it.actorId == "userId" && it.userAgent == "userAgent" },
        operationName = "createPermission",
        request = "{}",
        response = "{\"result\": \"summary\"}",
        success = true,
        error = null,
        organizationId = resolvedOrganizationId,
        workspaceId = workspaceId,
      )
    }
  }

  @Test
  fun `stores the entry when the organization is entitled`() {
    val organizationId = UUID.randomUUID()
    val storageClient = mockk<StorageClient>()
    every { storageClient.write(any(), any()) } just Runs
    every { storageClientFactory.create(DocumentType.AUDIT_LOGS) } returns storageClient
    every { airbyteStorageConfig.bucket.auditLogging } returns "test-audit-log-bucket"
    every { featureFlagClient.boolVariation(StoreAuditLogs, Organization(organizationId)) } returns true
    every { entitlementService.checkEntitlement(OrganizationId(organizationId), AuditLoggingEntitlement) } returns
      EntitlementResult(featureId = AuditLoggingEntitlement.featureId, isEntitled = true)

    interceptor = buildInterceptor()
    interceptor.onStartupEvent(mockk<StartupEvent>())
    interceptor.logAuditInfo(
      actor = Actor("userId", "email", null, "userAgent"),
      operationName = "updateOrganization",
      request = "{}",
      response = null,
      success = true,
      organizationId = organizationId,
    )
    interceptor.onShutdownEvent(mockk<ShutdownEvent>())

    verify { storageClient.write(match { it.contains("/$organizationId/") }, any()) }
  }

  @Test
  fun `skips the entry when the organization is not entitled`() {
    val organizationId = UUID.randomUUID()
    val storageClient = mockk<StorageClient>()
    every { storageClient.write(any(), any()) } just Runs
    every { storageClientFactory.create(DocumentType.AUDIT_LOGS) } returns storageClient
    every { airbyteStorageConfig.bucket.auditLogging } returns "test-audit-log-bucket"
    every { featureFlagClient.boolVariation(StoreAuditLogs, Organization(organizationId)) } returns true
    every { entitlementService.checkEntitlement(OrganizationId(organizationId), AuditLoggingEntitlement) } returns
      EntitlementResult(featureId = AuditLoggingEntitlement.featureId, isEntitled = false)

    interceptor = buildInterceptor()
    interceptor.onStartupEvent(mockk<StartupEvent>())
    interceptor.logAuditInfo(
      actor = Actor("userId", "email", null, "userAgent"),
      operationName = "updateOrganization",
      request = "{}",
      response = null,
      success = true,
      organizationId = organizationId,
    )
    interceptor.onShutdownEvent(mockk<ShutdownEvent>())

    verify(exactly = 0) { storageClient.write(any(), any()) }
  }

  @Test
  fun `skips the entry when no organization is resolved`() {
    val storageClient = mockk<StorageClient>()
    every { storageClient.write(any(), any()) } just Runs
    every { storageClientFactory.create(DocumentType.AUDIT_LOGS) } returns storageClient
    every { airbyteStorageConfig.bucket.auditLogging } returns "test-audit-log-bucket"

    interceptor = buildInterceptor()
    interceptor.onStartupEvent(mockk<StartupEvent>())
    interceptor.logAuditInfo(
      actor = Actor("userId", "email", null, "userAgent"),
      operationName = "updateOrganization",
      request = "{}",
      response = null,
      success = true,
      organizationId = null,
    )
    interceptor.onShutdownEvent(mockk<ShutdownEvent>())

    verify(exactly = 0) { featureFlagClient.boolVariation(StoreAuditLogs, any()) }
    verify(exactly = 0) { entitlementService.checkEntitlement(any(), any()) }
    verify(exactly = 0) { storageClient.write(any(), any()) }
  }

  private fun buildInterceptor(): AuditLoggingInterceptor =
    AuditLoggingInterceptor(
      applicationContext,
      auditLoggingHelper,
      auditScopeResolver,
      entitlementService,
      featureFlagClient,
      airbyteStorageConfig,
      storageClientFactory,
    )
}
