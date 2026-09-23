/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.WorkloadPriority
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.workload.api.domain.GcsDownscopedOAuthLogUploadAuthorization
import io.airbyte.workload.api.domain.KnownExceptionInfo
import io.airbyte.workload.api.domain.LogDeliveryMode
import io.airbyte.workload.api.domain.WorkloadCancelRequest
import io.airbyte.workload.api.domain.WorkloadClaimRequest
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.airbyte.workload.api.domain.WorkloadListRequest
import io.airbyte.workload.api.domain.WorkloadQueuePollRequest
import io.airbyte.workload.api.domain.WorkloadQueueQueryRequest
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import io.airbyte.workload.common.WorkloadQueueService
import io.airbyte.workload.errors.ConflictException
import io.airbyte.workload.errors.ForbiddenException
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.LogUploadAuthorizationBrokerException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.errors.UnauthorizedException
import io.airbyte.workload.handler.ApiWorkload
import io.airbyte.workload.handler.WorkloadHandler
import io.airbyte.workload.handler.WorkloadHandlerImpl
import io.airbyte.workload.logging.LogUploadAuthorizationService
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.env.Environment
import io.micronaut.core.util.SupplierUtil
import io.micronaut.http.HttpMethod
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

@Property(name = "airbyte.workload-api.workload-redelivery-window", value = "PT30M")
@MicronautTest(environments = [Environment.TEST])
class WorkloadApiTest(
  val embeddedServer: EmbeddedServer,
) {
  private val client = SupplierUtil.memoizedNonEmpty { embeddedServer.applicationContext.createBean(HttpClient::class.java, embeddedServer.url) }

  @Singleton
  fun mockMeterRegistry(): MeterRegistry = SimpleMeterRegistry()

  private val workloadQueueService = mockk<WorkloadQueueService>()

  @MockBean(WorkloadQueueService::class)
  @Replaces(WorkloadQueueService::class)
  fun workloadService(): WorkloadQueueService = workloadQueueService

  private val workloadHandler = mockk<WorkloadHandlerImpl>()

  @MockBean(WorkloadHandler::class)
  @Replaces(WorkloadHandler::class)
  fun workloadHandler(): WorkloadHandler = workloadHandler

  private val airbyteApiClient: AirbyteApiClient = mockk()

  @MockBean(AirbyteApiClient::class)
  @Replaces(AirbyteApiClient::class)
  fun airbyteApiClient(): AirbyteApiClient = airbyteApiClient

  private val roleResolver = mockk<RoleResolver>(relaxed = true)

  @Singleton
  @Replaces(RoleResolver::class)
  fun roleResolver(): RoleResolver = roleResolver

  private val dataplaneGroupService = mockk<DataplaneGroupService>()

  @MockBean(DataplaneGroupService::class)
  @Replaces(DataplaneGroupService::class)
  fun dataplaneGroupService(): DataplaneGroupService = dataplaneGroupService

  private val logUploadAuthorizationService = mockk<LogUploadAuthorizationService>()

  @MockBean(LogUploadAuthorizationService::class)
  @Replaces(LogUploadAuthorizationService::class)
  fun logUploadAuthorizationService(): LogUploadAuthorizationService = logUploadAuthorizationService

  @Test
  fun `test create success`() {
    every { workloadHandler.workloadAlreadyExists(any()) } returns false
    every { workloadHandler.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } just Runs
    every { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) } just Runs
    testEndpointStatus(HttpRequest.POST("/api/v1/workload/create", Jsons.serialize(WorkloadCreateRequest())), HttpStatus.NO_CONTENT)
    verify(exactly = 1) { workloadHandler.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
    verify(exactly = 1) { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `test create conflict`() {
    every { workloadHandler.workloadAlreadyExists(any()) } returns true
    testEndpointStatus(
      HttpRequest.POST("/api/v1/workload/create", WorkloadCreateRequest()),
      HttpStatus.OK,
    )
  }

  @Test
  fun `test claim success`() {
    every { workloadHandler.claimWorkload(any(), any(), any(), any()) }.returns(true)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/claim", Jsons.serialize(WorkloadClaimRequest())), HttpStatus.OK)
  }

  @Test
  fun `test claim workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.claimWorkload(any(), any(), any(), any()) } throws NotFoundException(exceptionMessage)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/claim", WorkloadClaimRequest()),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test claim workload has already been claimed`() {
    val exceptionMessage = "workload has already been claimed"
    every { workloadHandler.claimWorkload(any(), any(), any(), any()) } throws InvalidStatusTransitionException(exceptionMessage)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/claim", WorkloadClaimRequest()),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  @Test
  fun `GET delegates once to canonical getWorkload and returns the server-derived log delivery mode`() {
    every { workloadHandler.getWorkload("1") }.returns(ApiWorkload(logDeliveryMode = LogDeliveryMode.FLEX))

    val workload =
      client
        .get()
        .toBlocking()
        .exchange(HttpRequest.GET<Any>("/api/v1/workload/1"), ApiWorkload::class.java)
        .body()

    assertEquals(LogDeliveryMode.FLEX, workload.logDeliveryMode)
    verify(exactly = 1) { workloadHandler.getWorkload("1") }
  }

  @Test
  fun `generated Workload schema makes log delivery mode read only and optional`() {
    val contract =
      requireNotNull(
        javaClass.classLoader.getResourceAsStream("META-INF/swagger/airbyte-workload-api-server-1.0.0.yml"),
      ) { "generated workload OpenAPI contract must be available on the test classpath" }.use { input ->
        ObjectMapper(YAMLFactory()).readTree(input)
      }
    val workloadSchema = contract["components"]["schemas"]["Workload"]

    assertEquals(true, workloadSchema["properties"]["logDeliveryMode"]["readOnly"].asBoolean())
    assertEquals(false, workloadSchema["required"].map(JsonNode::asText).contains("logDeliveryMode"))
  }

  @Test
  fun `POST log upload authorization is bodyless and returns a fresh authorization`() {
    val authorization =
      GcsDownscopedOAuthLogUploadAuthorization(
        accessToken = "opaque-token",
        expiresAt = OffsetDateTime.parse("2026-09-02T13:00:00Z"),
        bucketName = "managed-log-bucket",
        objectKeyPrefix = "job-logging/job/7/attempt/2/",
      )
    every { logUploadAuthorizationService.authorize("1") } returns authorization

    val response =
      client
        .get()
        .toBlocking()
        .exchange(
          HttpRequest.create<Any>(HttpMethod.POST, "/api/v1/workload/1/log-upload-authorization"),
          GcsDownscopedOAuthLogUploadAuthorization::class.java,
        )

    assertEquals(HttpStatus.OK, response.status)
    assertEquals(authorization, response.body())
  }

  @Test
  fun `POST log upload authorization returns no content when issuance is disabled`() {
    every { logUploadAuthorizationService.authorize("1") } returns null

    testEndpointStatus(
      HttpRequest.create(HttpMethod.POST, "/api/v1/workload/1/log-upload-authorization"),
      HttpStatus.NO_CONTENT,
    )
  }

  @Test
  fun `POST log upload authorization inherits authenticated-only controller security`() {
    val security = WorkloadApi::class.java.getAnnotation(Secured::class.java)

    assertEquals(listOf(SecurityRule.IS_AUTHENTICATED), security.value.toList())
  }

  @Test
  fun `POST log upload authorization maps sanitized service rejections`() {
    val cases =
      listOf(
        UnauthorizedException("Log upload authorization is unavailable.") to HttpStatus.UNAUTHORIZED,
        NotFoundException("Log upload authorization is unavailable.") to HttpStatus.NOT_FOUND,
        ForbiddenException("Log upload authorization is unavailable.") to HttpStatus.FORBIDDEN,
        ConflictException("Log upload authorization is unavailable.") to HttpStatus.CONFLICT,
        InvalidStatusTransitionException("Log upload authorization is unavailable.") to HttpStatus.GONE,
        LogUploadAuthorizationBrokerException() to HttpStatus.INTERNAL_SERVER_ERROR,
      )

    cases.forEach { (failure, status) ->
      every { logUploadAuthorizationService.authorize("1") } throws failure
      testErrorEndpointResponse(
        HttpRequest.create(HttpMethod.POST, "/api/v1/workload/1/log-upload-authorization"),
        status,
        "Log upload authorization is unavailable.",
      )
    }
  }

  @Test
  fun `test get workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.getWorkload(any()) } throws NotFoundException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.GET("/api/v1/workload/1"),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test heartbeat success`() {
    every { workloadHandler.heartbeat(any(), any(), any()) }.returns(Unit)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/heartbeat", Jsons.serialize(WorkloadHeartbeatRequest())), HttpStatus.NO_CONTENT)
    verify(exactly = 1) { workloadHandler.getWorkloadOrganizationId(any()) }
    verify(exactly = 0) { workloadHandler.getWorkload(any()) }
  }

  @Test
  fun `test heartbeat workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.heartbeat(any(), any(), any()) } throws NotFoundException(exceptionMessage)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/heartbeat", WorkloadHeartbeatRequest()),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test heartbeat workload in invalid status`() {
    val exceptionMessage = "workload in invalid status"
    every { workloadHandler.heartbeat(any(), any(), any()) } throws InvalidStatusTransitionException(exceptionMessage)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/heartbeat", WorkloadHeartbeatRequest()),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  @Test
  fun `test list success`() {
    every { workloadHandler.getWorkloads(any(), any(), any()) }.returns(emptyList())
    testEndpointStatus(HttpRequest.POST("/api/v1/workload/list", WorkloadListRequest()), HttpStatus.OK)
  }

  @Test
  fun `test cancel success`() {
    every { workloadHandler.cancelWorkload(any(), any(), any()) } just Runs
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/cancel", WorkloadCancelRequest()), HttpStatus.NO_CONTENT)
  }

  @Test
  fun `test cancel workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.cancelWorkload(any(), any(), any()) } throws NotFoundException(exceptionMessage)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/cancel", WorkloadCancelRequest()),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test cancel workload in invalid status`() {
    val exceptionMessage = "workload in invalid status"
    every { workloadHandler.cancelWorkload(any(), any(), any()) } throws InvalidStatusTransitionException(exceptionMessage)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/cancel", WorkloadCancelRequest()),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  @Test
  fun `test failure success`() {
    every { workloadHandler.failWorkload(any(), any(), any(), any()) } just Runs
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/failure", WorkloadFailureRequest()), HttpStatus.NO_CONTENT)
  }

  @Test
  fun `test failure workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.failWorkload(any(), any(), any(), any()) } throws NotFoundException(exceptionMessage)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/failure", WorkloadFailureRequest()),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test failure workload in invalid status`() {
    val exceptionMessage = "workload in invalid status"
    every { workloadHandler.failWorkload(any(), any(), any(), any()) } throws InvalidStatusTransitionException(exceptionMessage)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/failure", WorkloadFailureRequest()),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  @Test
  fun `test success succeeded`() {
    every { workloadHandler.succeedWorkload(any(), any()) } just Runs
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/success", WorkloadSuccessRequest()), HttpStatus.NO_CONTENT)
  }

  @Test
  fun `test success workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.succeedWorkload(any(), any()) } throws NotFoundException(exceptionMessage)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/success", WorkloadSuccessRequest()),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test success workload in invalid status`() {
    val exceptionMessage = "workload in invalid status"
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    every { workloadHandler.succeedWorkload(any(), any()) } throws InvalidStatusTransitionException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/success", WorkloadSuccessRequest()),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  @Test
  fun `test running succeeded`() {
    every { workloadHandler.setWorkloadStatusToRunning(any(), any(), any()) } just Runs
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/running", Jsons.serialize(WorkloadRunningRequest())), HttpStatus.NO_CONTENT)
  }

  @Test
  fun `test running workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.setWorkloadStatusToRunning(any(), any(), any()) } throws NotFoundException(exceptionMessage)
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/running", WorkloadRunningRequest()),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test running workload in invalid status`() {
    val exceptionMessage = "workload in invalid status"
    every { workloadHandler.setWorkloadStatusToRunning(any(), any(), any()) } throws InvalidStatusTransitionException("workload in invalid status")
    every { workloadHandler.getWorkloadOrganizationId(any()) } returns null
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/running", WorkloadRunningRequest()),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  @Test
  fun `poll workloads happy path`() {
    val req =
      WorkloadQueuePollRequest(
        dataplaneGroup = UUID.randomUUID().toString(),
        priority = WorkloadPriority.DEFAULT,
        10,
      )

    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(any()) } returns UUID.randomUUID()
    every { workloadHandler.pollWorkloadQueue(req.dataplaneGroup, req.priority, 10) }.returns(emptyList())
    testEndpointStatus(HttpRequest.POST("/api/v1/workload/queue/poll", req), HttpStatus.OK)
  }

  @Test
  fun `count queue depth happy path`() {
    val req =
      WorkloadQueueQueryRequest(
        dataplaneGroup = UUID.randomUUID().toString(),
        priority = WorkloadPriority.DEFAULT,
      )

    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(any()) } returns UUID.randomUUID()
    every { workloadHandler.countWorkloadQueueDepth(req.dataplaneGroup, req.priority) }.returns(1)
    testEndpointStatus(HttpRequest.POST("/api/v1/workload/queue/depth", req), HttpStatus.OK)
  }

  @Test
  fun `get queue stats happy path`() {
    every { workloadHandler.getWorkloadQueueStats() }.returns(listOf())
    testEndpointStatus(HttpRequest.GET("/api/v1/workload/queue/stats"), HttpStatus.OK)
  }

  private fun testEndpointStatus(
    request: HttpRequest<Any>,
    expectedStatus: HttpStatus,
  ) {
    assertEquals(
      expectedStatus,
      client
        .get()
        .toBlocking()
        .exchange(request, String::class.java)
        .status,
    )
  }

  private fun testErrorEndpointResponse(
    request: HttpRequest<Any>,
    expectedStatus: HttpStatus,
    expectedMessage: String,
  ) {
    val exception =
      assertThrows<HttpClientResponseException> {
        client.get().toBlocking().exchange(request, String::class.java)
      }

    val deserializedBody = Jsons.deserialize(exception.response.getBody(String::class.java).get(), KnownExceptionInfo::class.java)

    assertEquals(expectedMessage, deserializedBody.message)
    assertEquals(expectedStatus, exception.status)
  }
}
