/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

import io.airbyte.api.model.generated.PermissionCreate
import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.audit.logging.model.Actor
import io.airbyte.audit.logging.provider.AuditProvider
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.StoreAuditLogs
import io.micronaut.aop.MethodInvocationContext
import io.micronaut.context.ApplicationContext
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
  private lateinit var storageClientFactory: StorageClientFactory
  private lateinit var featureFlagClient: FeatureFlagClient

  @BeforeEach
  fun setUp() {
    context = mockk()
    applicationContext = mockk()
    auditLoggingHelper = mockk()
    storageClientFactory = mockk(relaxed = true)
    featureFlagClient = mockk()
  }

  @AfterEach
  fun tearDown() {
    unmockkAll()
  }

  @Test
  fun `should only proceed the request without logging the result if it is not enabled`() {
    interceptor =
      AuditLoggingInterceptor(
        false,
        null,
        applicationContext,
        auditLoggingHelper,
        featureFlagClient,
        storageClientFactory,
      )

    every { context.methodName } returns "createPermission"
    every { storageClientFactory.create(DocumentType.AUDIT_LOGS) } returns mockk()
    every { featureFlagClient.boolVariation(StoreAuditLogs, any()) } returns false
    every { context.proceed() } returns
      PermissionRead()
        .userId(UUID.randomUUID())
        .workspaceId(UUID.randomUUID())
        .organizationId(null)
        .permissionType(PermissionType.WORKSPACE_EDITOR)

    interceptor.intercept(context)

    verify { context.proceed() }
  }

  @Test
  fun `should proceed the request and log the result`() {
    interceptor =
      spyk(
        AuditLoggingInterceptor(
          true,
          "test-audit-log-bucket",
          applicationContext,
          auditLoggingHelper,
          featureFlagClient,
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

    interceptor.intercept(context)
    // Verifying that request is proceeded
    verify { context.proceed() }
    // Verify logAuditInfo was called with the correct parameters
    verify {
      interceptor.logAuditInfo(
        actor = match { it.actorId == "userId" && it.userAgent == "userAgent" },
        operationName = "createPermission",
        request = "{}",
        response = "{\"result\": \"summary\"}",
        success = true,
        error = null,
      )
    }
  }
}
