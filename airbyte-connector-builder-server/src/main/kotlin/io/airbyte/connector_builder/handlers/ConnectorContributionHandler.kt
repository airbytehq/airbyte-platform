@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.handlers

import io.airbyte.connector_builder.api.model.generated.ConnectorContributionRead
import io.airbyte.connector_builder.api.model.generated.ConnectorContributionReadRequestBody
import io.airbyte.connector_builder.api.model.generated.GenerateContributionRequestBody
import io.airbyte.connector_builder.api.model.generated.GenerateContributionResponse
import io.airbyte.connector_builder.exceptions.ContributionException
import io.airbyte.connector_builder.services.GithubContributionService
import io.airbyte.connector_builder.templates.ContributionTemplates
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.kohsuke.github.HttpException

@Singleton
class ConnectorContributionHandler(private val contributionTemplates: ContributionTemplates) {
  fun connectorContributionRead(request: ConnectorContributionReadRequestBody): ConnectorContributionRead {
    // Validate the request connector name
    checkConnectorImageNameIsValid(request.connectorImageName)

    // Instantiate the Connectors Contribution Service
    val githubContributionService = GithubContributionService(request.connectorImageName, null)

    // Check for existing connector
    val connectorExists = githubContributionService.checkConnectorExistsOnMain()
    val connectorName = if (connectorExists) githubContributionService.readConnectorMetadataName() else null
    val connectorPath = if (connectorExists) "airbytehq/airbyte/tree/master/airbyte-integrations/connectors/${request.connectorImageName}" else null

    return ConnectorContributionRead().apply {
      connectorImageName = connectorName
      githubUrl = connectorPath
      available = !connectorExists
    }
  }

  private fun checkConnectorImageNameIsValid(connectorImageName: String) {
    // Connector Image Names must begin with "source-" and can contain only lowercase letters, numbers and dashes
    val validPattern = Regex("^source-[a-z0-9-]+$")
    if (!connectorImageName.matches(validPattern)) {
      throw IllegalArgumentException("$connectorImageName is not a valid image name.")
    }
  }

  fun generateContributionPullRequest(generateContributionRequestBody: GenerateContributionRequestBody): GenerateContributionResponse {
    // TODO: TEST with an account outside of the airbyte org
    // TODO: generate metadata from manifest + name + description and latest compatible source-declarative-manifest version
    // TODO: generate acceptance-test-config (if it does not exist)
    // TODO: add placeholder icon SVG
    val githubToken = generateContributionRequestBody.githubToken
    val connectorImageName = generateContributionRequestBody.connectorImageName

    // 1. Create a branch
    val githubContributionService = GithubContributionService(connectorImageName, githubToken)
    githubContributionService.prepareBranchForContribution()

    // 2. Generate Files
    val readmeContent =
      contributionTemplates.renderContributionReadme(
        connectorImageName,
        generateContributionRequestBody.name,
        generateContributionRequestBody.description,
      )

    // 3. Commit Files
    // TODO: merge these into a single method
    val readmeFilePath = githubContributionService.constructConnectorFilePath("README.md")
    githubContributionService.commitFile(
      "Create README.md for connector $connectorImageName",
      readmeFilePath,
      readmeContent,
    )

    // TODO: Ensure manifest is correctly formatted
    val manifestContent = generateContributionRequestBody.manifestYaml
    val manifestFilePath = githubContributionService.constructConnectorFilePath("manifest.yaml")
    githubContributionService.commitFile(
      "Create manifest.yaml for connector $connectorImageName",
      manifestFilePath,
      manifestContent,
    )

    // 4. Create / update pull request
    val pullRequest = githubContributionService.getOrCreatePullRequest()

    return GenerateContributionResponse().pullRequestUrl(pullRequest.htmlUrl.toString())
  }

  fun convertGithubExceptionToContributionException(e: HttpException): Exception {
    return when (e.responseCode) {
      401 -> ContributionException("Invalid GitHub token provided.", HttpStatus.UNAUTHORIZED)
      409 ->
        ContributionException(
          "We could not create a fork of the Airbyte repository. Please provide an access token with repo:write permissions.",
          HttpStatus.PRECONDITION_FAILED,
        )
      else -> ContributionException("An unexpected error occurred when creating your github contribution.", HttpStatus.INTERNAL_SERVER_ERROR)
    }
  }

  fun convertToContributionException(e: Exception): Exception {
    return when (e) {
      is HttpException -> convertGithubExceptionToContributionException(e)
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
