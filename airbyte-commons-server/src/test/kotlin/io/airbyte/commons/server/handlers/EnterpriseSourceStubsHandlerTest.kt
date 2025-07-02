/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.persistence.job.WorkspaceHelper
import io.mockk.every
import io.mockk.mockk
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class EnterpriseSourceStubsHandlerTest {
  private lateinit var enterpriseSourceHandler: EnterpriseSourceStubsHandler
  private lateinit var mockWebServer: MockWebServer
  private val workspaceHelper = mockk<WorkspaceHelper>()
  private val licenseEntitlementChecker = mockk<LicenseEntitlementChecker>()

  @BeforeEach
  fun setUp() {
    mockWebServer = MockWebServer()
    mockWebServer.start()
    val baseUrl = mockWebServer.url("/").toString()

    enterpriseSourceHandler = EnterpriseSourceStubsHandler(baseUrl, 5000, workspaceHelper, licenseEntitlementChecker)
  }

  @AfterEach
  fun tearDown() {
    mockWebServer.shutdown()
  }

  @Test
  fun testListEnterpriseSourceStubs_Success() {
    val mockJsonResponse = "[{\"name\":\"Test Source\",\"type\":\"enterprise_source\",\"id\":\"test-id\",\"unknownField\":\"value\"}]"
    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(200)
        .setBody(mockJsonResponse)
        .addHeader("Content-Type", "application/json"),
    )

    val result = enterpriseSourceHandler.listEnterpriseSourceStubs()

    assertNotNull(result)
    assertEquals(1, result.enterpriseSourceStubs.size)
    assertEquals("Test Source", result.enterpriseSourceStubs[0].name)
    assertEquals("test-id", result.enterpriseSourceStubs[0].id)
  }

  @Test
  fun testListEnterpriseSourceStubs_HttpError() {
    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(400)
        .setBody("Bad Request"),
    )

    val result = enterpriseSourceHandler.listEnterpriseSourceStubs()
    assertEquals(0, result.enterpriseSourceStubs.size)
  }

  @Test
  fun testListEnterpriseSourceStubs_InvalidJsonResponse() {
    val invalidJsonResponse = "Invalid JSON"
    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(200)
        .setBody(invalidJsonResponse)
        .addHeader("Content-Type", "application/json"),
    )

    val result = enterpriseSourceHandler.listEnterpriseSourceStubs()
    assertEquals(0, result.enterpriseSourceStubs.size)
  }

  @Test
  fun testListEnterpriseSourceStubs_EmptyResponse() {
    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(200)
        .setBody("")
        .addHeader("Content-Type", "application/json"),
    )

    val result = enterpriseSourceHandler.listEnterpriseSourceStubs()
    assertEquals(0, result.enterpriseSourceStubs.size)
  }

  @Test
  fun testListEnterpriseSourceStubsForWorkspace() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()

    val licensedDefinitionId = UUID.randomUUID()
    val unlicensedDefinitionId = UUID.randomUUID()

    val mockJsonResponse =
      """
      [
        {
          "name": "Test Source",
          "type": "enterprise_source",
          "id": "test-id",
          "definitionId": "$unlicensedDefinitionId"
        },
        {
          "name": "Test Fake Source",
          "type": "enterprise_source",
          "id": "test-fake-id"
        },
        {
          "name": "Test Already Licensed Source",
          "type": "enterprise_source",
          "id": "test-other-id",
          "definitionId": "$licensedDefinitionId"
        }
      ]
      """.trimIndent()

    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(200)
        .setBody(mockJsonResponse)
        .addHeader("Content-Type", "application/json"),
    )

    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId
    every {
      licenseEntitlementChecker.checkEntitlement(organizationId, Entitlement.SOURCE_CONNECTOR, unlicensedDefinitionId)
    } returns false
    every {
      licenseEntitlementChecker.checkEntitlement(organizationId, Entitlement.SOURCE_CONNECTOR, licensedDefinitionId)
    } returns true

    val result = enterpriseSourceHandler.listEnterpriseSourceStubsForWorkspace(workspaceId)

    assertNotNull(result)
    assertEquals(2, result.enterpriseSourceStubs.size)
    assertEquals("Test Source", result.enterpriseSourceStubs[0].name)
    assertEquals("test-id", result.enterpriseSourceStubs[0].id)
    assertEquals(unlicensedDefinitionId.toString(), result.enterpriseSourceStubs[0].definitionId)
    assertEquals("Test Fake Source", result.enterpriseSourceStubs[1].name)
    assertEquals("test-fake-id", result.enterpriseSourceStubs[1].id)
    assertNull(result.enterpriseSourceStubs[1].definitionId)
  }

  @Test
  fun testListEnterpriseSourceStubsForWorkspace_DefinitionNotFound() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()

    val definitionId = UUID.randomUUID()

    val mockJsonResponse =
      """
      [
        {
          "name": "Test Source",
          "type": "enterprise_source",
          "id": "test-id",
          "definitionId": "$definitionId"
        }
      ]
      """.trimIndent()

    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(200)
        .setBody(mockJsonResponse)
        .addHeader("Content-Type", "application/json"),
    )

    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId
    every {
      licenseEntitlementChecker.checkEntitlement(organizationId, Entitlement.SOURCE_CONNECTOR, definitionId)
    } throws ConfigNotFoundException(ConfigNotFoundType.SOURCE_CONNECTION, definitionId.toString())

    val result = enterpriseSourceHandler.listEnterpriseSourceStubsForWorkspace(workspaceId)

    assertNotNull(result)
    assertEquals(1, result.enterpriseSourceStubs.size)
    assertEquals("Test Source", result.enterpriseSourceStubs[0].name)
    assertEquals("test-id", result.enterpriseSourceStubs[0].id)
    assertEquals(definitionId.toString(), result.enterpriseSourceStubs[0].definitionId)
  }
}
