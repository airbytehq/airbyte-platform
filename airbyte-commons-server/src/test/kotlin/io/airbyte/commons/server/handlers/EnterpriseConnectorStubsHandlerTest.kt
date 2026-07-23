/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.micronaut.runtime.AirbyteConnectorRegistryConfig
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

class EnterpriseConnectorStubsHandlerTest {
  private lateinit var enterpriseConnectorHandler: EnterpriseConnectorStubsHandler
  private lateinit var mockWebServer: MockWebServer
  private val workspaceHelper = mockk<WorkspaceHelper>()
  private val licenseEntitlementChecker = mockk<LicenseEntitlementChecker>()
  private lateinit var airbyteConnectorRegistryConfig: AirbyteConnectorRegistryConfig

  @BeforeEach
  fun setUp() {
    mockWebServer = MockWebServer()
    mockWebServer.start()
    val baseUrl = mockWebServer.url("/").toString()

    airbyteConnectorRegistryConfig =
      AirbyteConnectorRegistryConfig(
        enterprise =
          AirbyteConnectorRegistryConfig.AirbyteConnectorRegistryEnterpriseConfig(
            enterpriseStubsUrl = baseUrl,
          ),
        remote =
          AirbyteConnectorRegistryConfig.AirbyteConnectorRegistryRemoteConfig(
            timeoutMs = 5000L,
          ),
      )
    enterpriseConnectorHandler =
      EnterpriseConnectorStubsHandler(
        airbyteConnectorRegistryConfig = airbyteConnectorRegistryConfig,
        workspaceHelper = workspaceHelper,
        licenseEntitlementChecker = licenseEntitlementChecker,
      )
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

    val result = enterpriseConnectorHandler.listEnterpriseSourceStubs()

    assertNotNull(result)
    assertEquals(1, result.enterpriseConnectorStubs.size)
    assertEquals("Test Source", result.enterpriseConnectorStubs[0].name)
    assertEquals("test-id", result.enterpriseConnectorStubs[0].id)
  }

  @Test
  fun testListEnterpriseSourceStubs_HttpError() {
    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(400)
        .setBody("Bad Request"),
    )

    val result = enterpriseConnectorHandler.listEnterpriseSourceStubs()
    assertEquals(0, result.enterpriseConnectorStubs.size)
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

    val result = enterpriseConnectorHandler.listEnterpriseSourceStubs()
    assertEquals(0, result.enterpriseConnectorStubs.size)
  }

  @Test
  fun testListEnterpriseSourceStubs_EmptyResponse() {
    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(200)
        .setBody("")
        .addHeader("Content-Type", "application/json"),
    )

    val result = enterpriseConnectorHandler.listEnterpriseSourceStubs()
    assertEquals(0, result.enterpriseConnectorStubs.size)
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

    val result = enterpriseConnectorHandler.listEnterpriseSourceStubsForWorkspace(workspaceId)

    assertNotNull(result)
    assertEquals(2, result.enterpriseConnectorStubs.size)
    assertEquals("Test Source", result.enterpriseConnectorStubs[0].name)
    assertEquals("test-id", result.enterpriseConnectorStubs[0].id)
    assertEquals(unlicensedDefinitionId.toString(), result.enterpriseConnectorStubs[0].definitionId)
    assertEquals("Test Fake Source", result.enterpriseConnectorStubs[1].name)
    assertEquals("test-fake-id", result.enterpriseConnectorStubs[1].id)
    assertNull(result.enterpriseConnectorStubs[1].definitionId)
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

    val result = enterpriseConnectorHandler.listEnterpriseSourceStubsForWorkspace(workspaceId)

    assertNotNull(result)
    assertEquals(1, result.enterpriseConnectorStubs.size)
    assertEquals("Test Source", result.enterpriseConnectorStubs[0].name)
    assertEquals("test-id", result.enterpriseConnectorStubs[0].id)
    assertEquals(definitionId.toString(), result.enterpriseConnectorStubs[0].definitionId)
  }

  @Test
  fun testListEnterpriseDestinationStubsForWorkspace() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()

    val licensedDefinitionId = UUID.randomUUID()
    val unlicensedDefinitionId = UUID.randomUUID()

    val mockJsonResponse =
      """
      [
        {
          "name": "Test Destination",
          "type": "enterprise_destination",
          "id": "test-dest-id",
          "definitionId": "$unlicensedDefinitionId"
        },
        {
          "name": "Test Fake Destination",
          "type": "enterprise_destination",
          "id": "test-fake-dest-id"
        },
        {
          "name": "Test Already Licensed Destination",
          "type": "enterprise_destination",
          "id": "test-other-dest-id",
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
      licenseEntitlementChecker.checkEntitlement(organizationId, Entitlement.DESTINATION_CONNECTOR, unlicensedDefinitionId)
    } returns false
    every {
      licenseEntitlementChecker.checkEntitlement(organizationId, Entitlement.DESTINATION_CONNECTOR, licensedDefinitionId)
    } returns true

    val result = enterpriseConnectorHandler.listEnterpriseDestinationStubsForWorkspace(workspaceId)

    assertNotNull(result)
    assertEquals(2, result.enterpriseConnectorStubs.size)
    assertEquals("Test Destination", result.enterpriseConnectorStubs[0].name)
    assertEquals("test-dest-id", result.enterpriseConnectorStubs[0].id)
    assertEquals(unlicensedDefinitionId.toString(), result.enterpriseConnectorStubs[0].definitionId)
    assertEquals("Test Fake Destination", result.enterpriseConnectorStubs[1].name)
    assertEquals("test-fake-dest-id", result.enterpriseConnectorStubs[1].id)
    assertNull(result.enterpriseConnectorStubs[1].definitionId)
  }

  @Test
  fun testListEnterpriseDestinationStubsForWorkspace_DefinitionNotFound() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()

    val definitionId = UUID.randomUUID()

    val mockJsonResponse =
      """
      [
        {
          "name": "Test Destination",
          "type": "enterprise_destination",
          "id": "test-dest-id",
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
      licenseEntitlementChecker.checkEntitlement(organizationId, Entitlement.DESTINATION_CONNECTOR, definitionId)
    } throws ConfigNotFoundException(ConfigNotFoundType.SOURCE_CONNECTION, definitionId.toString())

    val result = enterpriseConnectorHandler.listEnterpriseDestinationStubsForWorkspace(workspaceId)

    assertNotNull(result)
    assertEquals(1, result.enterpriseConnectorStubs.size)
    assertEquals("Test Destination", result.enterpriseConnectorStubs[0].name)
    assertEquals("test-dest-id", result.enterpriseConnectorStubs[0].id)
    assertEquals(definitionId.toString(), result.enterpriseConnectorStubs[0].definitionId)
  }
}
