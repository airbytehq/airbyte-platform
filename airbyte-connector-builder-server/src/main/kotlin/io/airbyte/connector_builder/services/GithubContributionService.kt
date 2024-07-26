@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.services

import org.kohsuke.github.GHBranch
import org.kohsuke.github.GHCommit
import org.kohsuke.github.GHContent
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

  val connectorReadmePath: String
    get() = "$connectorDirectoryPath/README.md"

  val connectorManifestPath: String
    get() = "$connectorDirectoryPath/manifest.yaml"

  val connectorIconPath: String
    get() = "$connectorDirectoryPath/icon.svg"

  val connectorAcceptanceTestConfigPath: String
    get() = "$connectorDirectoryPath/acceptance-test-config.yml"

  val username: String
    get() = githubService!!.myself.login

  val contributionBranchName: String
    get() = "$username/builder-contribute/$connectorImageName"

  val forkedContributonBranchName: String
    get() = "$username:$contributionBranchName"

  // PRIVATE METHODS

  fun getBranchSha(
    branchName: String,
    targetRepository: GHRepository,
  ): String {
    return targetRepository.getRef("heads/$branchName").getObject().sha
  }

  fun getDefaultBranchSha(targetRepository: GHRepository): String {
    return getBranchSha(airbyteRepository.defaultBranch, targetRepository)
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

  // TODO: Cache the metadata
  fun readConnectorMetadata(): Map<String, Any>? {
    val metadataFile = safeReadFileContent(connectorMetadataPath, airbyteRepository) ?: return null
    val rawYamlString = metadataFile.content

    // Parse YAML
    val yaml = Yaml()
    return yaml.load(rawYamlString)
  }

  fun readConnectorMetadataName(): String? {
    val parsedYaml = readConnectorMetadata() ?: return null

    // Extract "name" from the "data" section
    val dataSection = parsedYaml["data"] as? Map<*, *>
    return dataSection?.get("name") as? String
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

  fun commitFiles(
    message: String,
    files: Map<String, String>,
  ): GHCommit {
    val branchSha = getBranchSha(contributionBranchName, forkedRepository)
    val treeBuilder = forkedRepository.createTree().baseTree(branchSha)

    for ((path, content) in files) {
      treeBuilder.add(path, content, false)
    }

    val tree = treeBuilder.create()
    val commit =
      forkedRepository.createCommit()
        .message(message)
        .tree(tree.sha)
        .parent(branchSha)
        .create()

    val branchRef = getBranchRef(contributionBranchName, forkedRepository)
    branchRef?.updateTo(commit.shA1)
    return commit
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

  fun updateForkedBranchAndRepoToLatest() {
    val airbyteMasterSha = getBranchSha(airbyteRepository.defaultBranch, airbyteRepository)
    val forkedMasterRef = getBranchRef(airbyteRepository.defaultBranch, forkedRepository)
    val forkedContributionBranch = getBranch(contributionBranchName, forkedRepository)

    forkedMasterRef?.updateTo(airbyteMasterSha)
    forkedContributionBranch?.merge(airbyteMasterSha, "Merge latest changes from main branch")
  }

  fun prepareBranchForContribution() {
    val existingBranch = getBranchRef(contributionBranchName, forkedRepository)
    val existingPR = getExistingOpenPullRequest()

    if (existingBranch != null) {
      if (existingPR != null) {
        // Make sure the existing PR stays up to date with master
        updateForkedBranchAndRepoToLatest()
      } else {
        // Delete the branch with old contributions in the PR doesn't exist anymore
        deleteBranch(contributionBranchName, forkedRepository)
      }
    }

    getOrCreateContributionBranch()
  }
}
