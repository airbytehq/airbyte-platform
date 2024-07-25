@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.services

import org.kohsuke.github.GHBranch
import org.kohsuke.github.GHContent
import org.kohsuke.github.GHContentUpdateResponse
import org.kohsuke.github.GHFileNotFoundException
import org.kohsuke.github.GHPullRequest
import org.kohsuke.github.GHRef
import org.kohsuke.github.GHRepository
import org.kohsuke.github.GitHub
import org.kohsuke.github.GitHubBuilder
import org.yaml.snakeyaml.Yaml

class GithubContributionService(var connectorImageName: String, personalAccessToken: String?) {
  var githubService: GitHub? = null
  val repositoryName = "connector-archive" // TODO - change to airbyte before release
  val repoOwner = "airbytehq"
  val connectorDirectory = "airbyte-integrations/connectors"

  init {
    val builder = GitHubBuilder()
    if (personalAccessToken != null) {
      builder.withOAuthToken(personalAccessToken)
    }

    githubService = builder.build()
  }

  // PROPERTIES

  val airbyteRepository: GHRepository
    get() = githubService!!.getRepository("$repoOwner/$repositoryName")

  val forkedRepository: GHRepository
    get() = airbyteRepository.fork()

  val connectorDirectoryPath: String
    get() = "$connectorDirectory/$connectorImageName"

  val connectorMetadataPath: String
    get() = "$connectorDirectoryPath/metadata.yaml"

  val username: String
    get() = githubService!!.myself.login

  val contributionBranchName: String
    get() = "$username/builder-contribute/$connectorImageName"

  val forkedContributonBranchName: String
    get() = "$username:$contributionBranchName"

  // PRIVATE METHODS

  private fun getDefaultBranchSha(targetRepository: GHRepository): String {
    return targetRepository.getRef("heads/${targetRepository.defaultBranch}").getObject().sha
  }

  fun safeReadFileContent(
    path: String,
    targetRepository: GHRepository,
  ): GHContent? {
    return try {
      targetRepository.getFileContent(path)
    } catch (e: GHFileNotFoundException) {
      null
    }
  }

  fun checkFileExistsOnMain(path: String): Boolean {
    val fileInfo = safeReadFileContent(path, airbyteRepository)
    return fileInfo != null
  }

  // PUBLIC METHODS

  fun checkConnectorExistsOnMain(): Boolean {
    return checkFileExistsOnMain(connectorMetadataPath)
  }

  fun readConnectorMetadataName(): String? {
    val metadataFile = safeReadFileContent(connectorMetadataPath, forkedRepository) ?: return null

    val rawYamlString = metadataFile.content

    // parse yaml
    val yaml = Yaml()
    val parsedYaml = yaml.load<Map<String, Any>>(rawYamlString)

    // get name from the path "data.name"
    return parsedYaml["data"]?.let { (it as Map<*, *>)["name"] } as String? ?: ""
  }

  fun constructConnectorFilePath(fileName: String): String {
    return "$connectorDirectoryPath/$fileName"
  }

  fun createBranch(
    branchName: String,
    targetRepository: GHRepository,
  ): GHRef? {
    val baseBranchSha = getDefaultBranchSha(targetRepository)
    return targetRepository.createRef("refs/heads/$branchName", baseBranchSha)
  }

  fun getBranchRef(
    branchName: String,
    targetRepository: GHRepository,
  ): GHRef? {
    try {
      return targetRepository.getRef("refs/heads/$branchName")
    } catch (e: GHFileNotFoundException) {
      return null
    }
  }

  fun getBranch(
    branchName: String,
    targetRepository: GHRepository,
  ): GHBranch? {
    try {
      return targetRepository.getBranch(branchName)
    } catch (e: GHFileNotFoundException) {
      return null
    }
  }

  fun getExistingOpenPullRequest(): GHPullRequest? {
    val contributionBranch = getBranch(contributionBranchName, forkedRepository)
    if (contributionBranch == null) {
      return null
    }

    val pullRequests = airbyteRepository.searchPullRequests().isOpen().head(contributionBranch).list().toList()
    if (pullRequests.isEmpty()) {
      return null
    }

    return pullRequests.first()
  }

  fun deleteBranch(
    branchName: String,
    targetRepository: GHRepository,
  ) {
    targetRepository.getRef("refs/heads/$branchName").delete()
  }

  fun getOrCreateContributionBranch(): GHRef? {
    val existingBranch = getBranchRef(contributionBranchName, forkedRepository)
    if (existingBranch != null) {
      return existingBranch
    }

    return createBranch(contributionBranchName, forkedRepository)
  }

  fun getExistingFileSha(path: String): String? {
    try {
      return forkedRepository.getFileContent(path, contributionBranchName).sha
    } catch (e: GHFileNotFoundException) {
      return null
    }
  }

  fun commitFile(
    message: String,
    path: String,
    content: String,
  ): GHContentUpdateResponse? {
    val existingSha = getExistingFileSha(path)
    val contentToCommit =
      forkedRepository.createContent()
        .content(content)
        .path(path)

    if (existingSha != null) {
      contentToCommit.sha(existingSha)
    }

    return contentToCommit.branch(contributionBranchName)
      .message(message)
      .commit()
  }

  fun createPullRequest(): GHPullRequest {
    val title = "$connectorImageName contribution from $username"
    val body = "Auto-generated PR by the Connector Builder for $connectorImageName"
    return airbyteRepository.createPullRequest(title, forkedContributonBranchName, airbyteRepository.defaultBranch, body)
  }

  fun getOrCreatePullRequest(): GHPullRequest {
    val existingPullRequest = getExistingOpenPullRequest()
    if (existingPullRequest != null) {
      return existingPullRequest
    }

    return createPullRequest()
  }

  fun prepareBranchForContribution() {
    // TODO: Handle updating the forked repository and branch
    val existingBranch = getBranchRef(contributionBranchName, forkedRepository)
    val existingPR = getExistingOpenPullRequest()

    // Ensure we don't bring in any changes from previous contributions.
    // By deleting the branch if it exists but the PR does not
    if (existingBranch != null && existingPR == null) {
      deleteBranch(contributionBranchName, forkedRepository)
    }

    getOrCreateContributionBranch()
  }
}
