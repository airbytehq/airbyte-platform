@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.handlers

import io.airbyte.connector_builder.api.model.generated.ConnectorContributionRead
import io.airbyte.connector_builder.api.model.generated.ConnectorContributionReadRequestBody
import io.airbyte.connector_builder.services.GithubContributionService
import jakarta.inject.Singleton

@Singleton
class ConnectorContributionHandler {
  fun connectorContributionRead(request: ConnectorContributionReadRequestBody): ConnectorContributionRead {
    // Validate the request connector ID
    checkConnectorIdIsValid(request.connectorId)

    // Instantiate the Connectors Contribution Service
    val githubContributionService = GithubContributionService(request.connectorId)

    // Check for existing connector
    val connectorExists = githubContributionService.checkConnectorExistsOnMain()
    val connectorName = if (connectorExists) githubContributionService.readConnectorMetadataName() else null
    val connectorPath = if (connectorExists) "airbytehq/airbyte/tree/master/airbyte-integrations/connectors/${request.connectorId}" else null

    return ConnectorContributionRead().apply {
      connectorImageName = connectorName
      githubUrl = connectorPath
      available = !connectorExists
    }
  }

  private fun checkConnectorIdIsValid(connectorId: String) {
    // Connector IDs must begin with "source-" and can contain only lowercase letters, numbers and dashes
    val validPattern = Regex("^source-[a-z0-9-]+$")
    if (!connectorId.matches(validPattern)) {
      throw IllegalArgumentException("$connectorId is not a valid connector ID.")
    }
  }
}
