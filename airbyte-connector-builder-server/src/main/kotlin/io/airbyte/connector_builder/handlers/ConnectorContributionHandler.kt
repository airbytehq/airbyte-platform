@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.handlers

import io.airbyte.api.problems.model.generated.GithubContributionProblemData
import io.airbyte.api.problems.throwable.generated.ConnectorImageNameInUseProblem
import io.airbyte.api.problems.throwable.generated.GithubContributionFailedProblem
import io.airbyte.api.problems.throwable.generated.InsufficientGithubTokenPermissionsProblem
import io.airbyte.api.problems.throwable.generated.InvalidGithubTokenProblem
import io.airbyte.connector_builder.api.model.generated.CheckContributionRead
import io.airbyte.connector_builder.api.model.generated.CheckContributionRequestBody
import io.airbyte.connector_builder.api.model.generated.GenerateContributionRequestBody
import io.airbyte.connector_builder.api.model.generated.GenerateContributionResponse
import io.airbyte.connector_builder.services.GithubContributionService
import io.airbyte.connector_builder.templates.ContributionTemplates
import io.airbyte.connector_builder.utils.ManifestParser
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import org.kohsuke.github.GHFileNotFoundException
import org.kohsuke.github.HttpException
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
class ConnectorContributionHandler(
  private val contributionTemplates: ContributionTemplates,
  @Value("\${airbyte.connector-builder-server.github.airbyte-pat-token}") private val publicPatToken: String?,
) {
  fun checkContribution(request: CheckContributionRequestBody): CheckContributionRead {
    // Validate the request connector name
    checkConnectorImageNameIsValid(request.connectorImageName)

    // Instantiate the Connectors Contribution Service
    val githubContributionService = GithubContributionService(request.connectorImageName, publicPatToken)

    // Check for existing connector
    val connectorExists = githubContributionService.checkIfConnectorExistsOnMain()
    val connectorName = if (connectorExists) githubContributionService.readConnectorMetadataName() else null
    val connectorPath = if (connectorExists) "airbytehq/airbyte/tree/master/airbyte-integrations/connectors/${request.connectorImageName}" else null

    return CheckContributionRead().apply {
      this.connectorName = connectorName
      githubUrl = connectorPath
      this.connectorExists = connectorExists
    }
  }

  private fun checkConnectorImageNameIsValid(connectorImageName: String) {
    // Connector Image Names must begin with "source-" and can contain only lowercase letters, numbers and dashes
    val validPattern = Regex("^source-[a-z0-9-]+$")
    if (!connectorImageName.matches(validPattern)) {
      throw IllegalArgumentException("$connectorImageName is not a valid image name.")
    }
  }

  fun createFileCommitMap(
    connectorImageName: String,
    connectorName: String,
    connectorDescription: String,
    rawManifestYaml: String,
    baseImage: String,
    githubContributionService: GithubContributionService,
  ): Map<String, String> {
    val versionTag = "0.0.1"
    val releaseDate = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDate.now())
    val readmeContent =
      contributionTemplates.renderContributionReadmeMd(
        connectorImageName = connectorImageName,
        connectorName = connectorName,
        description = connectorDescription,
      )

    val manifestParser = ManifestParser(rawManifestYaml)
    var docsContent =
      contributionTemplates.renderContributionDocsMd(
        connectorImageName = connectorImageName,
        connectorName = connectorName,
        connectorVersionTag = versionTag,
        description = connectorDescription,
        manifestParser = manifestParser,
        authorUsername = githubContributionService.username,
        releaseDate = releaseDate,
      )

    // TODO: Ensure metadata is correctly formatted
    // TODO: Merge metadata with existing metadata if it exists
    val metadataContent =
      contributionTemplates.renderContributionMetadataYaml(
        connectorImageName = connectorImageName,
        connectorName = connectorName,
        actorDefinitionId = UUID.randomUUID().toString(),
        versionTag = versionTag,
        baseImage = baseImage,
        // TODO: Parse Allowed Hosts from manifest
        allowedHosts = listOf("*"),
        connectorDocsSlug = githubContributionService.connectorDocsSlug,
        releaseDate = releaseDate,
      )

    val iconContent = contributionTemplates.renderIconSvg()
    val acceptanceTestConfigContent = contributionTemplates.renderAcceptanceTestConfigYaml(connectorImageName = connectorImageName)

    // TODO: Decern which files are update (metadata.yaml), always overwrite (manifest.yaml), or only create if missing (icon.svg)
    val filesToCommit =
      mapOf(
        githubContributionService.connectorReadmePath to readmeContent,
        githubContributionService.connectorManifestPath to rawManifestYaml,
        githubContributionService.connectorMetadataPath to metadataContent,
        githubContributionService.connectorIconPath to iconContent,
        githubContributionService.connectorAcceptanceTestConfigPath to acceptanceTestConfigContent,
        githubContributionService.connectorDocsPath to docsContent,
      )

    return filesToCommit
  }

  fun generateContributionPullRequest(generateContributionRequestBody: GenerateContributionRequestBody): GenerateContributionResponse {
    val githubToken = generateContributionRequestBody.githubToken
    val connectorImageName = generateContributionRequestBody.connectorImageName

    val githubContributionService = GithubContributionService(connectorImageName, githubToken)

    // 0. Error if connector already exists
    if (githubContributionService.checkIfConnectorExistsOnMain()) {
      throw ConnectorImageNameInUseProblem()
    }

    // 1. Create a branch
    githubContributionService.prepareBranchForContribution()

    // 2. Generate Files
    val filesToCommit =
      createFileCommitMap(
        connectorImageName,
        generateContributionRequestBody.name,
        generateContributionRequestBody.description,
        generateContributionRequestBody.manifestYaml,
        generateContributionRequestBody.baseImage,
        githubContributionService,
      )

    // 3. Commit Files
    githubContributionService.commitFiles(
      "Submission for $connectorImageName from Connector Builder",
      filesToCommit,
    )

    // 4. Create / update pull request
    val pullRequest = githubContributionService.getOrCreatePullRequest()

    return GenerateContributionResponse().pullRequestUrl(pullRequest.htmlUrl.toString())
  }

  fun convertGithubExceptionToContributionException(e: HttpException): Exception {
    return when (e.responseCode) {
      401 -> InvalidGithubTokenProblem()
      409 -> InsufficientGithubTokenPermissionsProblem(e)
      else -> GithubContributionFailedProblem(GithubContributionProblemData().status(e.responseCode).message(e.message))
    }
  }

  fun convertToContributionException(e: Exception): Exception {
    logger.error(e) { "Failed to generate contribution" }
    return when (e) {
      is HttpException -> convertGithubExceptionToContributionException(e)
      // GHFileNotFoundException is encountered when the GitHub token has insufficient permissions to fork the airbyte repo
      is GHFileNotFoundException -> InsufficientGithubTokenPermissionsProblem(e)
      else -> e
    }
  }

  fun generateContribution(generateContributionRequestBody: GenerateContributionRequestBody): GenerateContributionResponse {
    try {
      return generateContributionPullRequest(generateContributionRequestBody)
    } catch (e: Exception) {
      throw convertToContributionException(e)
    }
  }
}
