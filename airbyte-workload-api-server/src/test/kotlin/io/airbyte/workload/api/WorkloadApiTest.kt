package io.airbyte.workload.api

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.temporal.WorkflowClientWrapped
import io.airbyte.workload.api.domain.KnownExceptionInfo
import io.airbyte.workload.api.domain.WorkloadCancelRequest
import io.airbyte.workload.api.domain.WorkloadClaimRequest
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.airbyte.workload.api.domain.WorkloadListRequest
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.handler.ApiWorkload
import io.airbyte.workload.handler.WorkloadHandler
import io.airbyte.workload.handler.WorkloadHandlerImpl
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

  private val workloadHandler = mockk<WorkloadHandlerImpl>()

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
    every { workloadHandler.workloadAlreadyExists(any()) } returns false
    every { workloadHandler.createWorkload(any(), any(), any(), any(), any(), any(), any()) } just Runs
    every { workloadService.create(any(), any(), any(), any(), any(), any(), any()) } just Runs
    testEndpointStatus(HttpRequest.POST("/api/v1/workload/create", Jsons.serialize(WorkloadCreateRequest())), HttpStatus.NO_CONTENT)
    verify(exactly = 1) { workloadHandler.createWorkload(any(), any(), any(), any(), any(), any(), any()) }
    verify(exactly = 1) { workloadService.create(any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `test create conflict`() {
    every { workloadHandler.workloadAlreadyExists(any()) } returns true
    testEndpointStatus(
      HttpRequest.POST("/api/v1/workload/create", Jsons.serialize(WorkloadCreateRequest())),
      HttpStatus.OK,
    )
  }

  @Test
  fun `test claim success`() {
    every { workloadHandler.claimWorkload(any(), any()) }.returns(true)
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/claim", Jsons.serialize(WorkloadClaimRequest())), HttpStatus.OK)
  }

  @Test
  fun `test claim workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.claimWorkload(any(), any()) } throws NotFoundException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/claim", Jsons.serialize(WorkloadClaimRequest())),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test claim workload has already been claimed`() {
    val exceptionMessage = "workload has already been claimed"
    every { workloadHandler.claimWorkload(any(), any()) } throws InvalidStatusTransitionException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/claim", Jsons.serialize(WorkloadClaimRequest())),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  @Test
  fun `test get success`() {
    every { workloadHandler.getWorkload(any()) }.returns(ApiWorkload())
    testEndpointStatus(HttpRequest.GET("/api/v1/workload/1"), HttpStatus.OK)
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
    every { workloadHandler.heartbeat(any()) }.returns(Unit)
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/heartbeat", Jsons.serialize(WorkloadHeartbeatRequest())), HttpStatus.NO_CONTENT)
  }

  @Test
  fun `test heartbeat workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.heartbeat(any()) } throws NotFoundException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/heartbeat", Jsons.serialize(WorkloadHeartbeatRequest())),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test heartbeat workload in invalid status`() {
    val exceptionMessage = "workload in invalid status"
    every { workloadHandler.heartbeat(any()) } throws InvalidStatusTransitionException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/heartbeat", Jsons.serialize(WorkloadHeartbeatRequest())),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  @Test
  fun `test list success`() {
    every { workloadHandler.getWorkloads(any(), any(), any()) }.returns(emptyList())
    testEndpointStatus(HttpRequest.POST("/api/v1/workload/list", Jsons.serialize(WorkloadListRequest())), HttpStatus.OK)
  }

  @Test
  fun `test cancel success`() {
    every { workloadHandler.cancelWorkload(any(), any(), any()) } just Runs
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/cancel", Jsons.serialize(WorkloadCancelRequest())), HttpStatus.NO_CONTENT)
  }

  @Test
  fun `test cancel workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.cancelWorkload(any(), any(), any()) } throws NotFoundException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/cancel", Jsons.serialize(WorkloadCancelRequest())),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test cancel workload in invalid status`() {
    val exceptionMessage = "workload in invalid status"
    every { workloadHandler.cancelWorkload(any(), any(), any()) } throws InvalidStatusTransitionException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/cancel", Jsons.serialize(WorkloadCancelRequest())),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  @Test
  fun `test failure success`() {
    every { workloadHandler.failWorkload(any(), any(), any()) } just Runs
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/failure", Jsons.serialize(WorkloadFailureRequest())), HttpStatus.NO_CONTENT)
  }

  @Test
  fun `test failure workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.failWorkload(any(), any(), any()) } throws NotFoundException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/failure", Jsons.serialize(WorkloadFailureRequest())),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test failure workload in invalid status`() {
    val exceptionMessage = "workload in invalid status"
    every { workloadHandler.failWorkload(any(), any(), any()) } throws InvalidStatusTransitionException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/failure", Jsons.serialize(WorkloadFailureRequest())),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  @Test
  fun `test success succeeded`() {
    every { workloadHandler.succeedWorkload(any()) } just Runs
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/success", Jsons.serialize(WorkloadSuccessRequest())), HttpStatus.NO_CONTENT)
  }

  @Test
  fun `test success workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.succeedWorkload(any()) } throws NotFoundException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/success", Jsons.serialize(WorkloadSuccessRequest())),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test success workload in invalid status`() {
    val exceptionMessage = "workload in invalid status"
    every { workloadHandler.succeedWorkload(any()) } throws InvalidStatusTransitionException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/success", Jsons.serialize(WorkloadSuccessRequest())),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  @Test
  fun `test running succeeded`() {
    every { workloadHandler.setWorkloadStatusToRunning(any()) } just Runs
    testEndpointStatus(HttpRequest.PUT("/api/v1/workload/running", Jsons.serialize(WorkloadRunningRequest())), HttpStatus.NO_CONTENT)
  }

  @Test
  fun `test running workload id not found`() {
    val exceptionMessage = "workload id not found"
    every { workloadHandler.setWorkloadStatusToRunning(any()) } throws NotFoundException(exceptionMessage)
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/running", Jsons.serialize(WorkloadRunningRequest())),
      HttpStatus.NOT_FOUND,
      exceptionMessage,
    )
  }

  @Test
  fun `test running workload in invalid status`() {
    val exceptionMessage = "workload in invalid status"
    every { workloadHandler.setWorkloadStatusToRunning(any()) } throws InvalidStatusTransitionException("workload in invalid status")
    testErrorEndpointResponse(
      HttpRequest.PUT("/api/v1/workload/running", Jsons.serialize(WorkloadRunningRequest())),
      HttpStatus.GONE,
      exceptionMessage,
    )
  }

  private fun testEndpointStatus(
    request: HttpRequest<Any>,
    expectedStatus: HttpStatus,
  ) {
    assertEquals(expectedStatus, client.toBlocking().exchange(request, String::class.java).status)
  }

  private fun testErrorEndpointResponse(
    request: HttpRequest<Any>,
    expectedStatus: HttpStatus,
    expectedMessage: String,
  ) {
    val exception =
      assertThrows<HttpClientResponseException> {
        client.toBlocking().exchange(request, String::class.java)
      }

    val deserializedBody = Jsons.deserialize(exception.response.getBody(String::class.java).get(), KnownExceptionInfo::class.java)

    assertEquals(expectedMessage, deserializedBody.message)
    assertEquals(expectedStatus, exception.status)
  }
}
