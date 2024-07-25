@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.handlers

import io.airbyte.connector_builder.api.model.generated.ConnectorContributionReadRequestBody
import io.airbyte.connector_builder.services.GithubContributionService
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ConnectorContributionHandlerTest {
  private var testConnectorId = "source-test-connector"
  private val requestBodyMock = mockk<ConnectorContributionReadRequestBody>()
  private lateinit var connectorContributionHandler: ConnectorContributionHandler

  @BeforeEach
  fun setUp() {
    mockkConstructor(GithubContributionService::class)
    every { requestBodyMock.connectorId } returns testConnectorId
    connectorContributionHandler = ConnectorContributionHandler()
  }

  @Test
  fun `returns existing connector if found in target repository`() {
    every { anyConstructed<GithubContributionService>().checkConnectorExistsOnMain() } returns true
    every { anyConstructed<GithubContributionService>().readConnectorMetadataName() } returns "Test Connector"

    val response = connectorContributionHandler.connectorContributionRead(requestBodyMock)

    assertFalse(response.available)
    assertEquals("Test Connector", response.connectorImageName)
    assertNotNull(response.githubUrl)
  }

  @Test
  fun `returns truthy boolean if connector not found in target repository`() {
    every { anyConstructed<GithubContributionService>().checkConnectorExistsOnMain() } returns false

    val response = connectorContributionHandler.connectorContributionRead(requestBodyMock)

    assertTrue(response.available)
    assertNull(response.connectorImageName)
    assertNull(response.githubUrl)
  }

  @Test
  fun `throws IllegalArgumentException for invalid connectorId`() {
    val invalidRequestBodyMock = mockk<ConnectorContributionReadRequestBody>()
    every { invalidRequestBodyMock.connectorId } returns "not-a-valid_id"

    val exception =
      assertThrows<IllegalArgumentException> {
        connectorContributionHandler.connectorContributionRead((invalidRequestBodyMock))
      }

    assertEquals("not-a-valid_id is not a valid connector ID.", exception.message)
  }
}
