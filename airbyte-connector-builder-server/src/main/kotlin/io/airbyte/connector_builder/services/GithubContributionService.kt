@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.services

import io.airbyte.commons.server.handlers.logger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.kohsuke.github.GHBranch
import org.kohsuke.github.GHCommit
import org.kohsuke.github.GHContent
import org.kohsuke.github.GHFileNotFoundException
import org.kohsuke.github.GHPullRequest
import org.kohsuke.github.GHRef
import org.kohsuke.github.GHRepository
import org.kohsuke.github.GitHub
import org.kohsuke.github.GitHubBuilder
import org.kohsuke.github.HttpException
import org.yaml.snakeyaml.Yaml

private val logger = KotlinLogging.logger {}

class GithubContributionService(
  var connectorImageName: String,
  personalAccessToken: String?,
) {
  var githubService: GitHub? = null
  val repositoryName = "airbyte"
  val repoOwner = "airbytehq"
  val connectorDirectory = "airbyte-integrations/connectors"

  init {
    val builder = GitHubBuilder()
    if (!personalAccessToken.isNullOrEmpty()) {
      builder.withOAuthToken(personalAccessToken)
    } else {
      logger.warn { "No personal access token provided for GitHub API. This may cause you to be rate limited by the Github API" }
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

  fun imageNameAsDocPath(imageName: String): String {
    // source-google-sheets -> sources/google-sheets
    // destination-google-sheets -> destinations/google-sheets

    val parts = imageName.split("-")
    val connectorType = parts[0] + "s"
    val connectorName = parts.drop(1).joinToString("-")

    return "$connectorType/$connectorName"
  }

  val connectorDocsSlug: String
    get() = "integrations/${imageNameAsDocPath(connectorImageName)}"

  val connectorDocsPath: String
    get() = "docs/integrations/${imageNameAsDocPath(connectorImageName)}.md"

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
  ): String = targetRepository.getRef("heads/$branchName").getObject().sha

  fun getDefaultBranchSha(targetRepository: GHRepository): String = getBranchSha(airbyteRepository.defaultBranch, targetRepository)

  fun safeReadFileContent(
    path: String,
    targetRepository: GHRepository,
  ): GHContent? =
    try {
      targetRepository.getFileContent(path)
    } catch (e: GHFileNotFoundException) {
      null
    }

  fun checkFileExistsOnMain(path: String): Boolean {
    val fileInfo = safeReadFileContent(path, airbyteRepository)
    return fileInfo != null
  }

  // PUBLIC METHODS

  fun checkIfConnectorExistsOnMain(): Boolean = checkFileExistsOnMain(connectorMetadataPath)

  // TODO: Cache the metadata
  fun readConnectorMetadata(): Map<String, Any>? {
    val metadataFile = safeReadFileContent(connectorMetadataPath, airbyteRepository) ?: return null
    val rawYamlString = metadataFile.content

    // Parse YAML
    val yaml = Yaml()
    return yaml.load(rawYamlString)
  }

/*
 * Read a top-level field from the connector metadata
 */
  fun readConnectorMetadataValue(field: String): String? {
    val parsedYaml = readConnectorMetadata() ?: return null

    // Extract a top-level field from the "data" section
    val dataSection = parsedYaml["data"] as? Map<*, *>
    return dataSection?.get(field) as? String
  }

  fun constructConnectorFilePath(fileName: String): String = "$connectorDirectoryPath/$fileName"

  fun createBranch(
    branchName: String,
    targetRepository: GHRepository,
  ): GHRef? {
    val baseBranchSha = getDefaultBranchSha(targetRepository)
    return targetRepository.createRef("refs/heads/$branchName", baseBranchSha)
  }

  private inline fun <T> safeHandleBranchExceptions(block: () -> T): T? =
    try {
      block()
    } catch (e: GHFileNotFoundException) {
      // Handle the case where the branch does not exist
      logger.debug(e) { "Suppressed GHFileNotFoundException getting branch ref" }
      null
    } catch (e: HttpException) {
      // Handle the case where the fork does not exist
      if (e.responseCode == 409) {
        logger.debug(e) { "Suppressed Repository Does not exist error getting branch ref" }
        null
      } else {
        throw e
      }
    }

  fun getBranchRef(
    branchName: String,
    targetRepository: GHRepository,
  ): GHRef? =
    safeHandleBranchExceptions {
      targetRepository.getRef("refs/heads/$branchName")
    }

  fun getBranch(
    branchName: String,
    targetRepository: GHRepository,
  ): GHBranch? =
    safeHandleBranchExceptions {
      targetRepository.getBranch(branchName)
    }

  fun getExistingOpenPullRequest(): GHPullRequest? {
    val contributionBranch = getBranch(contributionBranchName, forkedRepository) ?: return null

    val pullRequests =
      airbyteRepository
        .searchPullRequests()
        .isOpen()
        .head(contributionBranch)
        .list()
        .toList()
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

  fun getExistingFileSha(path: String): String? =
    try {
      forkedRepository.getFileContent(path, contributionBranchName).sha
    } catch (e: GHFileNotFoundException) {
      null
    }

  fun commitFiles(files: Map<String, String>): GHCommit {
    val message = "Submission for $connectorImageName from Connector Builder"
    val branchSha = getBranchSha(contributionBranchName, forkedRepository)
    val treeBuilder = forkedRepository.createTree().baseTree(branchSha)

    for ((path, content) in files) {
      treeBuilder.add(path, content, false)
    }

    val tree = treeBuilder.create()
    val commit =
      forkedRepository
        .createCommit()
        .message(message)
        .tree(tree.sha)
        .parent(branchSha)
        .create()

    val branchRef = getBranchRef(contributionBranchName, forkedRepository)
    branchRef?.updateTo(commit.shA1)
    return commit
  }

  fun createPullRequest(description: String): GHPullRequest {
    val title = "$connectorImageName contribution from $username"
    return airbyteRepository.createPullRequest(title, forkedContributonBranchName, airbyteRepository.defaultBranch, description)
  }

  fun getOrCreatePullRequest(description: String): GHPullRequest {
    val existingPullRequest = getExistingOpenPullRequest()
    if (existingPullRequest != null) {
      existingPullRequest.body = description
      return existingPullRequest
    }

    return createPullRequest(description)
  }

  fun attemptUpdateForkedBranchAndRepoToLatest() {
    val airbyteMasterSha = getBranchSha(airbyteRepository.defaultBranch, airbyteRepository)
    val forkedMasterRef = getBranchRef(airbyteRepository.defaultBranch, forkedRepository)
    val forkedContributionBranch = getBranch(contributionBranchName, forkedRepository)

    try {
      forkedMasterRef?.updateTo(airbyteMasterSha)
      forkedContributionBranch?.merge(airbyteMasterSha, "Merge latest changes from main branch")
    } catch (e: GHFileNotFoundException) {
      // HACK: This method is flaky and relies on an eventually consistent GitHub API
      // TODO: Update to use `merge-upstream` instead
      // WHEN: When a version of org.kohsuke:github-api < 1.323 is available
      // WHY: https://github.com/hub4j/github-api/pull/1898 will be part of that change
      logger.error(e) { "Failed to update forked branch to latest" }
    }
  }

  fun prepareBranchForContribution() {
    val existingBranch = getBranchRef(contributionBranchName, forkedRepository)
    val existingPR = getExistingOpenPullRequest()

    if (existingBranch != null) {
      if (existingPR != null) {
        // Make sure the existing PR stays up to date with master
        attemptUpdateForkedBranchAndRepoToLatest()
      } else {
        // Delete the branch with old contributions in the PR doesn't exist anymore
        deleteBranch(contributionBranchName, forkedRepository)
      }
    }

    getOrCreateContributionBranch()
  }
}
