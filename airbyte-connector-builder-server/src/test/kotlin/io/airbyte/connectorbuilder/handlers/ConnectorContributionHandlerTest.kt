/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers

import io.airbyte.connectorbuilder.api.model.generated.CheckContributionRequestBody
import io.airbyte.connectorbuilder.services.GithubContributionService
import io.airbyte.connectorbuilder.templates.ContributionTemplates
import io.airbyte.connectorbuilder.utils.BuilderContributionInfo
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
  fun `checkContribution returns details of an existing connector if found in target repository`() {
    every { anyConstructed<GithubContributionService>().checkIfConnectorExistsOnMain() } returns true
    every { anyConstructed<GithubContributionService>().readConnectorMetadataValue("name") } returns "Test Connector"
    every { anyConstructed<GithubContributionService>().readConnectorDescription() } returns "This is a mocked test connector description."

    val response = connectorContributionHandler.checkContribution(requestBodyMock)

    assertTrue(response.connectorExists)
    assertEquals("Test Connector", response.connectorName)
    assertEquals("This is a mocked test connector description.", response.connectorDescription)
    assertNotNull(response.githubUrl)
  }

  @Test
  fun `checkContribution returns 'false' for connectorExists if connector not found in target repository`() {
    every { anyConstructed<GithubContributionService>().checkIfConnectorExistsOnMain() } returns false

    val response = connectorContributionHandler.checkContribution(requestBodyMock)

    assertFalse(response.connectorExists)
    assertNull(response.connectorName)
    assertNull(response.connectorDescription)
    assertNull(response.githubUrl)
  }

  @Test
  fun `checkContribution throws IllegalArgumentException for invalid connectorImageName`() {
    val invalidRequestBodyMock = mockk<CheckContributionRequestBody>()
    every { invalidRequestBodyMock.connectorImageName } returns "not-a-valid_image_name"

    val exception =
      assertThrows<IllegalArgumentException> {
        connectorContributionHandler.checkContribution((invalidRequestBodyMock))
      }

    assertEquals("not-a-valid_image_name is not a valid image name.", exception.message)
  }

  @Test
  fun `getFilesToCommitGenerationMap gets all files if they don't exist`() {
    val contributionInfo = mockk<BuilderContributionInfo>(relaxed = true)
    val githubContributionService = mockk<GithubContributionService>(relaxed = true)
    every { githubContributionService.connectorManifestPath } returns "manifestPath"
    every { githubContributionService.connectorReadmePath } returns "readmePath"
    every { githubContributionService.connectorMetadataPath } returns "metadataPath"
    every { githubContributionService.connectorIconPath } returns "iconPath"
    every { githubContributionService.connectorAcceptanceTestConfigPath } returns "acceptanceTestConfigPath"
    every { githubContributionService.connectorDocsPath } returns "docsPath"
    every { githubContributionService.connectorCustomComponentsPath } returns "customComponentsPath"
    every { githubContributionService.checkFileExistsOnMain(any()) } returns false

    val filesToCommit = connectorContributionHandler.getFilesToCommitGenerationMap(contributionInfo, githubContributionService)
    assertEquals(7, filesToCommit.size)
    assertEquals(
      setOf("manifestPath", "readmePath", "metadataPath", "iconPath", "acceptanceTestConfigPath", "docsPath", "customComponentsPath"),
      filesToCommit.keys,
    )
  }

  @Test
  fun `getFilesToCommitGenerationMap gets only manifest and custom components file if all files exist`() {
    val contributionInfo = mockk<BuilderContributionInfo>(relaxed = true)
    val githubContributionService = mockk<GithubContributionService>(relaxed = true)
    every { githubContributionService.connectorManifestPath } returns "manifestPath"
    every { githubContributionService.connectorCustomComponentsPath } returns "customComponentsPath"
    every { githubContributionService.checkFileExistsOnMain(any()) } returns true

    val filesToCommit = connectorContributionHandler.getFilesToCommitGenerationMap(contributionInfo, githubContributionService)
    assertEquals(2, filesToCommit.size)
    assertEquals(setOf("manifestPath", "customComponentsPath"), filesToCommit.keys)
  }
}
