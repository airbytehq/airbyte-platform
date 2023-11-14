package io.airbyte.workload.api

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.temporal.WorkflowClientWrapped
import io.airbyte.workload.api.domain.WorkloadClaimRequest
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.airbyte.workload.api.domain.WorkloadListRequest
import io.airbyte.workload.api.domain.WorkloadStatusUpdateRequest
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.errors.NotModifiedException
import io.airbyte.workload.handler.ApiWorkload
import io.airbyte.workload.handler.WorkloadHandler
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micronaut.context.annotation.Replaces
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
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

@MicronautTest
class WorkloadApiTest(
  @Client("/") val client: HttpClient,
) {
  @Singleton
  fun mockMeterRegistry(): MeterRegistry {
    return SimpleMeterRegistry()
  }

  private val workloadService = mockk<WorkloadService>()

  @MockBean(WorkloadService::class)
  @Replaces(WorkloadService::class)
  fun workloadService(): WorkloadService {
    return workloadService
  }

  private val workloadHandler = mockk<WorkloadHandler>()

  @MockBean(WorkloadHandler::class)
  @Replaces(WorkloadHandler::class)
  fun workloadHandler(): WorkloadHandler {
    return workloadHandler
  }

  private val workloadClientWrapped = mockk<WorkflowClientWrapped>()

  @MockBean(WorkflowClientWrapped::class)
  @Replaces(WorkflowClientWrapped::class)
  fun workloadClientWrapped(): WorkflowClientWrapped {
    return workloadClientWrapped
  }

  @Test
  fun `test create success`() {
    every { workloadHandler.createWorkload(any(), any()) } just Runs
    every { workloadService.create(any(), any()) } just Runs
    testEndpointStatus(HttpRequest.POST("/api/v1/workload/create", Jsons.serialize(WorkloadCreateRequest())), HttpStatus.NO_CONTENT)
    verify(exactly = 1) { workloadHandler.createWorkload(any(), any()) }
    verify(exactly = 1) { workloadService.create(any(), any()) }
  }

  @Test
  fun `test create conflict`() {
    every { workloadHandler.createWorkload(any(), any()) } throws NotModifiedException("test")
    testEndpointStatus(HttpRequest.POST("/api/v1/workload/create", Jsons.serialize(WorkloadCreateRequest())), HttpStatus.NOT_MODIFIED)
  }

  @Test
  fun `test claim success`() {
    every { workloadHandler.claimWorkload(any(), any()) }.returns(true)
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/claim", Jsons.serialize(WorkloadClaimRequest())), HttpStatus.OK)
  }

  @Test
  fun `test claim workload id not found`() {
    every { workloadHandler.claimWorkload(any(), any()) } throws NotFoundException("test")
    testErrorEndpointStatus(HttpRequest.PUT("/api/v1/workload/claim", Jsons.serialize(WorkloadClaimRequest())), HttpStatus.NOT_FOUND)
  }

  @Test
  fun `test claim workload has already been claimed`() {
    every { workloadHandler.claimWorkload(any(), any()) } throws InvalidStatusTransitionException("test")
    testErrorEndpointStatus(HttpRequest.PUT("/api/v1/workload/claim", Jsons.serialize(WorkloadClaimRequest())), HttpStatus.GONE)
  }

  @Test
  fun `test get success`() {
    every { workloadHandler.getWorkload(any()) }.returns(ApiWorkload())
    testEndpointStatus(HttpRequest.GET("/api/v1/workload/1"), HttpStatus.OK)
  }

  @Test
  fun `test get workload id not found`() {
    every { workloadHandler.getWorkload(any()) } throws NotFoundException("test")
    testErrorEndpointStatus(HttpRequest.GET("/api/v1/workload/1"), HttpStatus.NOT_FOUND)
  }

  @Test
  fun `test heartbeat success`() {
    every { workloadHandler.heartbeat(any()) }.returns(Unit)
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/heartbeat", Jsons.serialize(WorkloadHeartbeatRequest())), HttpStatus.NO_CONTENT)
  }

  @Test
  fun `test heartbeat workload id not found`() {
    every { workloadHandler.heartbeat(any()) } throws NotFoundException("test")
    testErrorEndpointStatus(HttpRequest.PUT("/api/v1/workload/heartbeat", Jsons.serialize(WorkloadHeartbeatRequest())), HttpStatus.NOT_FOUND)
  }

  @Test
  fun `test heartbeat workload in invalid status`() {
    every { workloadHandler.heartbeat(any()) } throws InvalidStatusTransitionException("test")
    testErrorEndpointStatus(HttpRequest.PUT("/api/v1/workload/heartbeat", Jsons.serialize(WorkloadHeartbeatRequest())), HttpStatus.GONE)
  }

  @Test
  fun `test list success`() {
    every { workloadHandler.getWorkloads(any(), any(), any()) }.returns(emptyList())
    testEndpointStatus(HttpRequest.POST("/api/v1/workload/list", Jsons.serialize(WorkloadListRequest())), HttpStatus.OK)
  }

  @Test
  fun `test status update success`() {
    every { workloadHandler.updateWorkload(any(), any()) }.returns(mockk())
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/status", Jsons.serialize(WorkloadStatusUpdateRequest())), HttpStatus.NO_CONTENT)
  }

  @Test
  fun `test status update workload id not found`() {
    every { workloadHandler.updateWorkload(any(), any()) } throws NotFoundException("test")
    testErrorEndpointStatus(HttpRequest.PUT("/api/v1/workload/status", Jsons.serialize(WorkloadStatusUpdateRequest())), HttpStatus.NOT_FOUND)
  }

  private fun testEndpointStatus(
    request: HttpRequest<Any>,
    expectedStatus: HttpStatus,
  ) {
    assertEquals(expectedStatus, client.toBlocking().exchange(request, String::class.java).status)
  }

  private fun testErrorEndpointStatus(
    request: HttpRequest<Any>,
    expectedStatus: HttpStatus,
  ) {
    val exception =
      assertThrows<HttpClientResponseException> {
        client.toBlocking().exchange(request, String::class.java)
      }

    assertEquals(expectedStatus, exception.status)
  }
}
