/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.contributions

import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.kohsuke.github.GHBranch
import org.kohsuke.github.GHBranchSync
import org.kohsuke.github.GHContentUpdateResponse
import org.kohsuke.github.GHFileNotFoundException
import org.kohsuke.github.GHPullRequest
import org.kohsuke.github.GHRef
import org.kohsuke.github.GHRepository
import org.kohsuke.github.GitHub
import org.kohsuke.github.HttpException
import org.kohsuke.github.PagedSearchIterable

class GithubContributionServiceTest {
  var testConnectorImageName = "source-test-case"
  var testUserName = "testusername"

  private val githubMock = mockk<GitHub>()
  private val airbyteRepoMock = mockk<GHRepository>()
  private val forkedRepoMock = mockk<GHRepository>()
  private val refMock = mockk<GHRef>()
  private val prMock = mockk<GHPullRequest>()
  private val pagedSearchPRMock = mockk<PagedSearchIterable<GHPullRequest>>()
  private val branchMock = mockk<GHBranch>()
  private val syncMock = mockk<GHBranchSync>()
  private val contentUpdateResponseMock = mockk<GHContentUpdateResponse>()

  private lateinit var contributionService: GithubContributionService

  @BeforeEach
  fun setUp() {
    every { githubMock.myself } returns
      mockk {
        every { login } returns testUserName
      }

    every { githubMock.getRepository(any()) } returns airbyteRepoMock
    every { airbyteRepoMock.defaultBranch } returns "main"
    every { airbyteRepoMock.fork() } returns forkedRepoMock
    every { airbyteRepoMock.createPullRequest(any(), any(), any(), any()) } returns prMock
    every { airbyteRepoMock.searchPullRequests() } returns
      mockk {
        every { isOpen() } returns this
        every { head(any()) } returns this
        every { list() } returns pagedSearchPRMock
      }

    justRun { refMock.delete() }
    justRun { refMock.updateTo(any()) }

    every { refMock.getObject() } returns
      mockk {
        every { sha } returns "dummySha"
      }

    every { forkedRepoMock.createRef(any(), any()) } returns refMock
    every { forkedRepoMock.getRef(any()) } returns refMock
    every { forkedRepoMock.sync(any()) } returns syncMock

    every { forkedRepoMock.getBranch(any()) } returns branchMock
    every { forkedRepoMock.getFileContent(any(), any()) } returns
      mockk {
        every { sha } returns "dummySha"
      }

    every { forkedRepoMock.createContent() } returns
      mockk {
        every { content(String()) } returns this
        every { path(any()) } returns this
        every { sha(any()) } returns this
        every { branch(any()) } returns this
        every { message(any()) } returns this
        every { commit() } returns contentUpdateResponseMock
      }

    every { pagedSearchPRMock.toList() } returns emptyList()

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
    Assertions.assertEquals("airbyte-integrations/connectors/$testConnectorImageName", connectorDirectoryPath)
  }

  @Test
  fun `contributionBranchName returns correct branch name`() {
    // get the contribution branch name
    val contributionBranchName = contributionService.contributionBranchName

    // assert that the contribution branch name is correct
    Assertions.assertEquals("$testUserName/builder-contribute/$testConnectorImageName", contributionBranchName)
  }

  @Test
  fun `constructConnectorFilePath returns correct path`() {
    val expectedPath = "airbyte-integrations/connectors/$testConnectorImageName/dummyFile.txt"
    val actualPath = contributionService.constructConnectorFilePath("dummyFile.txt")
    Assertions.assertEquals(expectedPath, actualPath)
  }

  @Test
  fun `createBranch successfully creates a new branch`() {
    val branchRef = contributionService.createBranch("new-feature-branch", forkedRepoMock)
    Assertions.assertNotNull(branchRef)
  }

  @Test
  fun `getExistingOpenPullRequest returns null when no open PR exists`() {
    every { forkedRepoMock.getBranch(any()) } returns null
    Assertions.assertNull(contributionService.getExistingOpenPullRequest())
  }

  @Test
  fun `deleteBranch deletes the specified branch`() {
    every { refMock.delete() } returns Unit
    Assertions.assertDoesNotThrow { contributionService.deleteBranch("feature-to-delete", forkedRepoMock) }
  }

  @Test
  fun `getExistingFileSha returns null when file does not exist`() {
    every { forkedRepoMock.getFileContent(any(), any()) } throws GHFileNotFoundException("File does not exist")
    Assertions.assertNull(contributionService.getExistingFileSha("non-existing-file.txt"))
  }

  @Test
  fun `createPullRequest creates a new pull request successfully`() {
    Assertions.assertNotNull(contributionService.createPullRequest("dummy description"))
  }

  @Test
  fun `prepareBranchForContribution with existing branch and no PR deletes branch, creates new branch and syncs upstream`() {
    every { forkedRepoMock.getRef(any()) } returns refMock
    every { pagedSearchPRMock.toList() } returns emptyList()

    contributionService.prepareBranchForContribution()

    verify { refMock.delete() }
    verify { forkedRepoMock.sync("main") }
    verify { forkedRepoMock.sync("testusername/builder-contribute/source-test-case") }
  }

  @Test
  fun `prepareBranchForContribution with existing branch and a PR syncs upstream`() {
    every { forkedRepoMock.getRef(any()) } returns refMock
    every { pagedSearchPRMock.toList() } returns listOf(prMock)

    contributionService.prepareBranchForContribution()

    verify(exactly = 0) { refMock.delete() }
    verify { forkedRepoMock.sync("main") }
    verify { forkedRepoMock.sync("testusername/builder-contribute/source-test-case") }
  }

  @Test
  fun `prepareBranchForContribution with no existing branch creates new branch and syncs upstream`() {
    every {
      forkedRepoMock.getRef("refs/heads/$testUserName/builder-contribute/$testConnectorImageName")
    } throws GHFileNotFoundException("Branch does not exist")
    every { forkedRepoMock.createRef(any(), any()) } returns refMock

    contributionService.prepareBranchForContribution()

    verify { forkedRepoMock.createRef(any(), any()) }
    verify { forkedRepoMock.sync("main") }
    verify { forkedRepoMock.sync("testusername/builder-contribute/source-test-case") }
  }

  @Test
  fun `connectorDocsPath formats correctly`() {
    val expectedPath = "docs/integrations/sources/test-case.md"
    val actualPath = contributionService.connectorDocsPath
    Assertions.assertEquals(expectedPath, actualPath)
  }

  @Test
  fun `getBranchRef returns branch ref when branch exists`() {
    every { forkedRepoMock.getRef("refs/heads/existing-branch") } returns refMock

    val result = contributionService.getBranchRef("existing-branch", forkedRepoMock)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(refMock, result)
  }

  @Test
  fun `getBranchRef returns null when branch does not exist`() {
    every { forkedRepoMock.getRef("refs/heads/non-existing-branch") } throws GHFileNotFoundException("Branch does not exist")

    val result = contributionService.getBranchRef("non-existing-branch", forkedRepoMock)

    Assertions.assertNull(result)
  }

  @Test
  fun `getBranchRef returns null on HttpException with response code 409`() {
    every { forkedRepoMock.getRef("refs/heads/conflict-branch") } throws HttpException("", 409, "", "")

    val result = contributionService.getBranchRef("conflict-branch", forkedRepoMock)

    Assertions.assertNull(result)
  }

  @Test
  fun `getBranchRef throws exception on HttpException with other response code`() {
    every { forkedRepoMock.getRef("refs/heads/error-branch") } throws HttpException("", 500, "", "")

    val exception =
      assertThrows<HttpException> {
        contributionService.getBranchRef("error-branch", forkedRepoMock)
      }

    Assertions.assertEquals(500, exception.responseCode)
  }

  @Test
  fun `getBranch returns branch ref when branch exists`() {
    every { forkedRepoMock.getBranch("existing-branch") } returns branchMock

    val result = contributionService.getBranch("existing-branch", forkedRepoMock)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(branchMock, result)
  }

  @Test
  fun `getBranch returns null when branch does not exist`() {
    every { forkedRepoMock.getBranch("non-existing-branch") } throws GHFileNotFoundException("Branch does not exist")

    val result = contributionService.getBranch("non-existing-branch", forkedRepoMock)

    Assertions.assertNull(result)
  }

  @Test
  fun `getBranch returns null on HttpException with response code 409`() {
    every { forkedRepoMock.getBranch("conflict-branch") } throws HttpException("", 409, "", "")

    val result = contributionService.getBranch("conflict-branch", forkedRepoMock)

    Assertions.assertNull(result)
  }

  @Test
  fun `getBranch throws exception on HttpException with other response code`() {
    every { forkedRepoMock.getBranch("error-branch") } throws HttpException("", 500, "", "")

    val exception =
      assertThrows<HttpException> {
        contributionService.getBranch("error-branch", forkedRepoMock)
      }

    Assertions.assertEquals(500, exception.responseCode)
  }
}
