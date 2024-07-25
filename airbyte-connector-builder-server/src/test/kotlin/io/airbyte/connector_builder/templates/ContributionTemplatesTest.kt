@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.templates

import org.junit.jupiter.api.Test

class ContributionTemplatesTest {
  @Test
  fun renderContributionReadme() {
    val contributionTemplates = ContributionTemplates()
    val connectorImageName = "test"
    val connectorName = "Test Connector"
    val description = "This is a test connector."
    val readme = contributionTemplates.renderContributionReadmeMd(connectorImageName, connectorName, description)

    // Assert that the rendered readme contains the connector name
    assert(readme.contains(connectorName))

    // Assert that the rendered readme contains the connector description
    assert(readme.contains(description))
  }
}
