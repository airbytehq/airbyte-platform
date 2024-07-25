@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.services

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.kohsuke.github.GHContentUpdateResponse
import org.kohsuke.github.GHFileNotFoundException
import org.kohsuke.github.GHPullRequest
import org.kohsuke.github.GHRef
import org.kohsuke.github.GHRepository
import org.kohsuke.github.GitHub

class GithubContributionServiceTest {
  var testConnectorImageName = "source-test"
  var testUserName = "testusername"

  private val githubMock = mockk<GitHub>()
  private val repoMock = mockk<GHRepository>()
  private val refMock = mockk<GHRef>()
  private val prMock = mockk<GHPullRequest>()
  private val contentUpdateResponseMock = mockk<GHContentUpdateResponse>()

  private lateinit var contributionService: GithubContributionService

  @BeforeEach
  fun setUp() {
    every { githubMock.myself } returns
      mockk {
        every { login } returns testUserName
      }

    every { githubMock.getRepository(any()) } returns repoMock
    every { refMock.getObject() } returns
      mockk {
        every { sha } returns "dummySha"
      }
    every { repoMock.createRef(any(), any()) } returns refMock
    every { repoMock.getRef(any()) } returns refMock
    every { repoMock.createPullRequest(any(), any(), any(), any()) } returns prMock
    every { repoMock.getPullRequests(any()) } returns listOf(prMock)
    every { repoMock.getFileContent(any(), any()) } returns
      mockk {
        every { sha } returns "dummySha"
      }
    every { repoMock.defaultBranch } returns "main"
    every { repoMock.fork() } returns repoMock
    every { repoMock.createContent() } returns
      mockk {
        every { content(String()) } returns this
        every { path(any()) } returns this
        every { sha(any()) } returns this
        every { branch(any()) } returns this
        every { message(any()) } returns this
        every { commit() } returns contentUpdateResponseMock
      }

    contributionService =
      GithubContributionService(testConnectorImageName, "testtoken").apply {
        githubService = githubMock
      }
  }

  @Test
  fun `connectorDirectoryPath returns correct directory path`() {
    // get the connector directory path
    val connectorDirectoryPath = contributionService.connectorDirectoryPath

    // assert that the connector directory path is correct
    assertEquals("airbyte-integrations/connectors/$testConnectorImageName", connectorDirectoryPath)
  }

  @Test
  fun `contributionBranchName returns correct branch name`() {
    // get the contribution branch name
    val contributionBranchName = contributionService.contributionBranchName

    // assert that the contribution branch name is correct
    assertEquals("$testUserName/builder-contribute/$testConnectorImageName", contributionBranchName)
  }

  @Test
  fun `constructConnectorFilePath returns correct path`() {
    val expectedPath = "airbyte-integrations/connectors/$testConnectorImageName/dummyFile.txt"
    val actualPath = contributionService.constructConnectorFilePath("dummyFile.txt")
    assertEquals(expectedPath, actualPath)
  }

  @Test
  fun `createBranch successfully creates a new branch`() {
    val branchRef = contributionService.createBranch("new-feature-branch", repoMock)
    assertNotNull(branchRef)
  }

  @Test
  fun `getBranchRef returns null when branch does not exist`() {
    every { repoMock.getRef(any()) } throws GHFileNotFoundException("Branch does not exist")
    assertNull(contributionService.getBranchRef("non-existing-branch", repoMock))
  }

  @Test
  fun `getExistingOpenPullRequest returns null when no open PR exists`() {
    every { repoMock.getBranch(any()) } returns null
    assertNull(contributionService.getExistingOpenPullRequest())
  }

  @Test
  fun `deleteBranch deletes the specified branch`() {
    every { refMock.delete() } returns Unit
    assertDoesNotThrow { contributionService.deleteBranch("feature-to-delete", repoMock) }
  }

  @Test
  fun `getExistingFileSha returns null when file does not exist`() {
    every { repoMock.getFileContent(any(), any()) } throws GHFileNotFoundException("File does not exist")
    assertNull(contributionService.getExistingFileSha("non-existing-file.txt"))
  }

  @Test
  fun `createPullRequest creates a new pull request successfully`() {
    assertNotNull(contributionService.createPullRequest())
  }
}
