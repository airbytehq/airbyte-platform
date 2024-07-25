@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.templates

import io.pebbletemplates.pebble.PebbleEngine
import io.pebbletemplates.pebble.template.PebbleTemplate
import jakarta.inject.Singleton
import java.io.StringWriter
import java.io.Writer

@Singleton
class ContributionTemplates {
  var templateEngine: PebbleEngine? = null

  init {
    templateEngine = PebbleEngine.Builder().build()
  }

  private fun renderTemplateString(
    templatePath: String,
    context: Map<String, Any>,
  ): String {
    val compiledTemplate: PebbleTemplate = templateEngine!!.getTemplate(templatePath)
    val writer: Writer = StringWriter()
    compiledTemplate.evaluate(writer, context)
    return writer.toString()
  }

  fun renderContributionReadmeMd(
    connectorImageName: String,
    connectorName: String,
    description: String,
  ): String {
    val context = mapOf("connectorImageName" to connectorImageName, "connectorName" to connectorName, "description" to description)
    return renderTemplateString("contribution_templates/readme.md.peb", context)
  }

  fun renderContributionMetadataYaml(
    connectorImageName: String,
    connectorName: String,
    actorDefinitionId: String,
    versionTag: String,
    baseImage: String,
    allowedHosts: List<String>,
    connectorDocsSlug: String,
    releaseDate: String,
  ): String {
    val context =
      mapOf(
        "allowedHosts" to allowedHosts,
        "connectorImageName" to connectorImageName,
        "baseImage" to baseImage,
        "actorDefinitionId" to actorDefinitionId,
        "versionTag" to versionTag,
        "connectorName" to connectorName,
        "releaseDate" to releaseDate,
        "connectorDocsSlug" to connectorDocsSlug,
      )
    return renderTemplateString("contribution_templates/metadata.yaml.peb", context)
  }

  fun renderIconSvg(): String {
    return renderTemplateString("contribution_templates/icon.svg", emptyMap())
  }

  fun renderAcceptanceTestConfigYaml(connectorImageName: String): String {
    val context = mapOf("connectorImageName" to connectorImageName)
    return renderTemplateString("contribution_templates/acceptance-test-config.yml.peb", context)
  }
}
