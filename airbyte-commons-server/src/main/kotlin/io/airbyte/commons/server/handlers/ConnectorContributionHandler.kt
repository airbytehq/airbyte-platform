/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.problems.model.generated.GithubContributionProblemData
import io.airbyte.api.problems.throwable.generated.GithubContributionFailedProblem
import io.airbyte.api.problems.throwable.generated.InsufficientGithubTokenPermissionsProblem
import io.airbyte.api.problems.throwable.generated.InvalidGithubTokenProblem
import io.airbyte.commons.server.builder.contributions.BuilderContributionInfo
import io.airbyte.commons.server.builder.contributions.ContributionCreate
import io.airbyte.commons.server.builder.contributions.ContributionCreateResult
import io.airbyte.commons.server.builder.contributions.ContributionRead
import io.airbyte.commons.server.builder.contributions.ContributionTemplates
import io.airbyte.commons.server.builder.contributions.GithubContributionService
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import org.kohsuke.github.GHFileNotFoundException
import org.kohsuke.github.HttpException
import java.util.UUID

@Singleton
class ConnectorContributionHandler(
  private val contributionTemplates: ContributionTemplates,
  @Value("\${airbyte.connector-builder-server.github.airbyte-pat-token}") private val publicPatToken: String?,
) {
  fun checkContribution(connectorImageName: String): ContributionRead {
    // Validate the request connector name
    checkConnectorImageNameIsValid(connectorImageName)

    // Instantiate the Connectors Contribution Service
    val githubContributionService = GithubContributionService(connectorImageName, publicPatToken)

    // Check for existing connector
    val connectorExists = githubContributionService.checkIfConnectorExistsOnMain()

    // only fetch connector name, description, and Github url if connector exists
    val (connectorName, connectorDescription, connectorPath) =
      if (connectorExists) {
        Triple(
          githubContributionService.readConnectorMetadataValue("name"),
          githubContributionService.readConnectorDescription(),
          "airbytehq/airbyte/tree/master/airbyte-integrations/connectors/$connectorImageName",
        )
      } else {
        Triple(null, null, null)
      }

    return ContributionRead(
      connectorName = connectorName,
      connectorDescription = connectorDescription,
      githubUrl = connectorPath,
      connectorExists = connectorExists,
    )
  }

  private fun checkConnectorImageNameIsValid(connectorImageName: String) {
    // Connector Image Names must begin with "source-" and can contain only lowercase letters, numbers and dashes
    val validPattern = Regex("^source-[a-z0-9-]+$")
    if (!connectorImageName.matches(validPattern)) {
      throw IllegalArgumentException("$connectorImageName is not a valid image name.")
    }
  }

  fun getFilesToCommitGenerationMap(
    contributionInfo: BuilderContributionInfo,
    githubContributionService: GithubContributionService,
  ): Map<String, () -> String?> {
    // Always generate and overwrite the manifest and custom components python file
    // If the value of the map is null (in the case of an optional file), this will delete the file if it exists
    val filesToCommit =
      mutableMapOf(
        githubContributionService.connectorManifestPath to { contributionInfo.manifestYaml },
        githubContributionService.connectorCustomComponentsPath to { contributionInfo.customComponents },
      )

    // Do not regenerate these if they already exist
    val createIfNotExistsFiles =
      listOf(
        githubContributionService.connectorReadmePath to { contributionTemplates.renderContributionReadmeMd(contributionInfo) },
        githubContributionService.connectorIconPath to { contributionTemplates.renderIconSvg() },
        githubContributionService.connectorAcceptanceTestConfigPath to { contributionTemplates.renderAcceptanceTestConfigYaml(contributionInfo) },
        githubContributionService.connectorDocsPath to { contributionTemplates.renderContributionDocsMd(contributionInfo) },
        githubContributionService.connectorMetadataPath to
          { contributionTemplates.renderContributionMetadataYaml(contributionInfo, githubContributionService) },
      )

    createIfNotExistsFiles.forEach { (filePath, generationFunction) ->
      if (!githubContributionService.checkFileExistsOnMain(filePath)) {
        filesToCommit[filePath] = generationFunction
      }
    }
    return filesToCommit
  }

  private fun getContributionInfo(
    contributionCreate: ContributionCreate,
    githubContributionService: GithubContributionService,
  ): BuilderContributionInfo {
    val isEdit = githubContributionService.checkIfConnectorExistsOnMain()
    val actorDefinitionId = githubContributionService.readConnectorMetadataValue("definitionId") ?: UUID.randomUUID().toString()
    val authorUsername = githubContributionService.username
    return BuilderContributionInfo(
      isEdit = isEdit,
      connectorName = contributionCreate.name,
      connectorImageName = contributionCreate.connectorImageName,
      actorDefinitionId = actorDefinitionId,
      connectorDescription = contributionCreate.connectorDescription,
      contributionDescription = contributionCreate.contributionDescription,
      githubToken = contributionCreate.githubToken,
      manifestYaml = contributionCreate.manifestYaml,
      customComponents = contributionCreate.customComponents,
      baseImage = contributionCreate.baseImage,
      versionTag = "0.0.1",
      authorUsername = authorUsername,
      changelogMessage = "Initial release by [@$authorUsername](https://github.com/$authorUsername) via Connector Builder",
    )
  }

  private fun generateContributionPullRequest(contributionCreate: ContributionCreate): ContributionCreateResult {
    val githubContributionService =
      GithubContributionService(contributionCreate.connectorImageName, contributionCreate.githubToken)
    val contributionInfo = getContributionInfo(contributionCreate, githubContributionService)

    // Create or get branch
    githubContributionService.prepareBranchForContribution()

    // Commit files to branch
    val fileGenerationMap = getFilesToCommitGenerationMap(contributionInfo, githubContributionService)
    val filesToCommit = fileGenerationMap.mapValues { it.value.invoke() }
    githubContributionService.commitFiles(filesToCommit)

    // Create / update pull request of branch
    val pullRequestDescription = contributionTemplates.renderContributionPullRequestDescription(contributionInfo)
    val pullRequest = githubContributionService.getOrCreatePullRequest(pullRequestDescription)

    return ContributionCreateResult(
      pullRequestUrl = pullRequest.htmlUrl.toString(),
      actorDefinitionId = UUID.fromString(contributionInfo.actorDefinitionId),
    )
  }

  private fun convertGithubExceptionToContributionException(e: HttpException): Exception =
    when (e.responseCode) {
      401 -> InvalidGithubTokenProblem()
      409 -> InsufficientGithubTokenPermissionsProblem(e)
      else -> GithubContributionFailedProblem(GithubContributionProblemData().status(e.responseCode).message(e.message))
    }

  private fun convertToContributionException(e: Exception): Exception {
    logger.error(e) { "Failed to generate contribution" }
    return when (e) {
      is HttpException -> convertGithubExceptionToContributionException(e)
      // GHFileNotFoundException is encountered when the GitHub token has insufficient permissions to fork the airbyte repo
      is GHFileNotFoundException -> InsufficientGithubTokenPermissionsProblem(e)
      else -> e
    }
  }

  fun generateContribution(contributionCreate: ContributionCreate): ContributionCreateResult {
    try {
      return generateContributionPullRequest(contributionCreate)
    } catch (e: Exception) {
      throw convertToContributionException(e)
    }
  }
}
