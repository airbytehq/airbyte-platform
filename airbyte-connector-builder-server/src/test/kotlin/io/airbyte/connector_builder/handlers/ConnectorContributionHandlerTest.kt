@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.handlers

import io.airbyte.connector_builder.api.model.generated.CheckContributionRequestBody
import io.airbyte.connector_builder.services.GithubContributionService
import io.airbyte.connector_builder.templates.ContributionTemplates
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
  private var testConnectorImageName = "source-test-connector"
  private val requestBodyMock = mockk<CheckContributionRequestBody>()
  private lateinit var connectorContributionHandler: ConnectorContributionHandler

  @BeforeEach
  fun setUp() {
    mockkConstructor(GithubContributionService::class)
    every { requestBodyMock.connectorImageName } returns testConnectorImageName
    val templateService = ContributionTemplates()
    connectorContributionHandler = ConnectorContributionHandler(templateService, null)
  }

  @Test
  fun `returns details of an existing connector if found in target repository`() {
    every { anyConstructed<GithubContributionService>().checkIfConnectorExistsOnMain() } returns true
    every { anyConstructed<GithubContributionService>().readConnectorMetadataName() } returns "Test Connector"

    val response = connectorContributionHandler.checkContribution(requestBodyMock)

    assertTrue(response.connectorExists)
    assertEquals("Test Connector", response.connectorName)
    assertNotNull(response.githubUrl)
  }

  @Test
  fun `returns 'false' for connectorExists if connector not found in target repository`() {
    every { anyConstructed<GithubContributionService>().checkIfConnectorExistsOnMain() } returns false

    val response = connectorContributionHandler.checkContribution(requestBodyMock)

    assertFalse(response.connectorExists)
    assertNull(response.connectorName)
    assertNull(response.githubUrl)
  }

  @Test
  fun `throws IllegalArgumentException for invalid connectorImageName`() {
    val invalidRequestBodyMock = mockk<CheckContributionRequestBody>()
    every { invalidRequestBodyMock.connectorImageName } returns "not-a-valid_image_name"

    val exception =
      assertThrows<IllegalArgumentException> {
        connectorContributionHandler.checkContribution((invalidRequestBodyMock))
      }

    assertEquals("not-a-valid_image_name is not a valid image name.", exception.message)
  }
}
