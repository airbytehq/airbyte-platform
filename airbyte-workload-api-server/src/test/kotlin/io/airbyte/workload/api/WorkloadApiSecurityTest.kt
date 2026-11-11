/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api

import io.airbyte.commons.PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.config.Dataplane
import io.airbyte.config.Permission
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.handler.WorkloadHandler
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
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.util.UUID

@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-only-jwt-signature-secret-1234567890")
@MicronautTest(environments = [Environment.TEST])
class WorkloadApiSecurityTest(
  embeddedServer: EmbeddedServer,
) {
  private val client = SupplierUtil.memoizedNonEmpty { embeddedServer.applicationContext.createBean(HttpClient::class.java, embeddedServer.url) }

  @Inject
  lateinit var tokenGenerator: JwtTokenGenerator

  private val workloadHandler = mockk<WorkloadHandler>()

  @MockBean(WorkloadHandler::class)
  fun workloadHandler(): WorkloadHandler = workloadHandler

  @Inject
  lateinit var permissionHandler: PermissionHandler

  @Singleton
  @Replaces(PermissionHandler::class)
  fun permissionHandler(): PermissionHandler = mockk()

  @Inject
  lateinit var dataplanes: DataplaneService

  @Singleton
  @Replaces(DataplaneService::class)
  fun dataplanes(): DataplaneService = mockk()

  @Inject
  lateinit var groups: DataplaneGroupService

  @Singleton
  @Replaces(DataplaneGroupService::class)
  fun groups(): DataplaneGroupService = mockk()

  @ParameterizedTest
  @EnumSource(value = TokenType::class, names = ["SERVICE_ACCOUNT", "DATAPLANE_V1"])
  fun `customer dataplane reads only its own customer workload`(tokenType: TokenType) {
    val customer = UUID.randomUUID()
    val foreign = UUID.randomUUID()
    val account = UUID.randomUUID()
    val foreignDataplane = UUID.randomUUID()
    val foreignGroup = UUID.randomUUID()
    val privateLinkDataplane = UUID.randomUUID()
    val ownDataplane = UUID.randomUUID()
    val privateLinkGroup = UUID.randomUUID()
    every { permissionHandler.getPermissionsByServiceAccountId(account) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.DATAPLANE).withOrganizationId(customer))
    every { workloadHandler.getWorkloadOrganizationId("own") } returns customer
    every { workloadHandler.getWorkloadOrganizationId("foreign") } returns foreign
    every { workloadHandler.getWorkload("own") } returns
      Workload(id = "own", organizationId = customer, dataplaneGroup = privateLinkGroup.toString())
    every { workloadHandler.workloadAlreadyExists("foreign") } returns true
    every { workloadHandler.claimWorkload("own", foreignDataplane.toString(), any(), any()) } returns true
    every { dataplanes.getDataplane(foreignDataplane) } returns Dataplane().withDataplaneGroupId(foreignGroup)
    every { groups.getOrganizationIdFromDataplaneGroup(foreignGroup) } returns foreign
    every { dataplanes.getDataplane(privateLinkDataplane) } returns
      Dataplane().withDataplaneGroupId(privateLinkGroup).withServiceAccountId(UUID.randomUUID())
    every { dataplanes.getDataplane(ownDataplane) } returns
      Dataplane().withDataplaneGroupId(privateLinkGroup).withServiceAccountId(account)
    every { groups.getOrganizationIdFromDataplaneGroup(privateLinkGroup) } returns PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
    every { workloadHandler.claimWorkload("own", privateLinkDataplane.toString(), any(), any()) } returns true
    every { workloadHandler.claimWorkload("own", ownDataplane.toString(), any(), any()) } returns true
    val token =
      tokenGenerator
        .generateToken(
          mapOf("sub" to account.toString(), tokenType.toClaim()),
        ).orElseThrow()

    assertEquals(HttpStatus.OK, status(HttpRequest.GET<Any>("/api/v1/workload/own").bearerAuth(token)))
    assertEquals(HttpStatus.FORBIDDEN, status(HttpRequest.GET<Any>("/api/v1/workload/foreign").bearerAuth(token)))
    assertEquals(
      HttpStatus.FORBIDDEN,
      status(
        HttpRequest
          .PUT(
            "/api/v1/workload/claim",
            mapOf("workloadId" to "own", "dataplaneId" to foreignDataplane.toString()),
          ).bearerAuth(token),
      ),
    )
    assertEquals(
      HttpStatus.FORBIDDEN,
      status(
        HttpRequest
          .PUT(
            "/api/v1/workload/claim",
            mapOf("workloadId" to "own", "dataplaneId" to privateLinkDataplane.toString()),
          ).bearerAuth(token),
      ),
    )
    assertEquals(
      HttpStatus.OK,
      status(
        HttpRequest
          .PUT(
            "/api/v1/workload/claim",
            mapOf("workloadId" to "own", "dataplaneId" to ownDataplane.toString()),
          ).bearerAuth(token),
      ),
    )
    assertEquals(
      HttpStatus.FORBIDDEN,
      status(
        HttpRequest
          .POST(
            "/api/v1/workload/create",
            mapOf("workloadId" to "foreign", "workloadInput" to "{}", "organizationId" to customer.toString()),
          ).bearerAuth(token),
      ),
    )
    assertEquals(HttpStatus.FORBIDDEN, status(HttpRequest.GET<Any>("/api/v1/workload/queue/stats").bearerAuth(token)))
  }

  private fun status(request: HttpRequest<*>): HttpStatus =
    try {
      client
        .get()
        .toBlocking()
        .exchange(request, String::class.java)
        .status
    } catch (failure: HttpClientResponseException) {
      failure.status
    }

  @Test
  fun `queue stats reject a caller without global dataplane permission`() {
    every { workloadHandler.getWorkloadQueueStats() } returns emptyList()
    val token = tokenGenerator.generateToken(mapOf("sub" to "ordinary-user")).orElseThrow()

    val error =
      assertThrows<HttpClientResponseException> {
        client.get().toBlocking().exchange(
          HttpRequest.GET<Any>("/api/v1/workload/queue/stats").bearerAuth(token),
          String::class.java,
        )
      }

    assertEquals(HttpStatus.FORBIDDEN, error.status)
  }

  @Test
  fun `POST data plane log upload authorization rejects an unauthenticated request`() {
    val error =
      assertThrows<HttpClientResponseException> {
        client
          .get()
          .toBlocking()
          .exchange(
            HttpRequest.POST("/api/v1/workload/dataplane/log-upload-authorization", """{"dataplaneId":"${UUID.randomUUID()}"}"""),
            String::class.java,
          )
      }

    assertEquals(HttpStatus.UNAUTHORIZED, error.status)
  }

  @Test
  fun `POST log upload authorization rejects an unauthenticated request`() {
    val error =
      assertThrows<HttpClientResponseException> {
        client
          .get()
          .toBlocking()
          .exchange(
            HttpRequest.create<Any>(HttpMethod.POST, "/api/v1/workload/1/log-upload-authorization"),
            String::class.java,
          )
      }

    assertEquals(HttpStatus.UNAUTHORIZED, error.status)
  }
}
