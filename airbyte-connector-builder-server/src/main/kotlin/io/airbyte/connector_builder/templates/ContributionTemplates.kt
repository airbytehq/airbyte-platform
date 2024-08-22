@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.templates

import io.airbyte.connector_builder.utils.ManifestParser
import io.pebbletemplates.pebble.PebbleEngine
import io.pebbletemplates.pebble.template.PebbleTemplate
import jakarta.inject.Singleton
import java.io.StringWriter
import java.io.Writer

data class TemplateStream(
  val name: String?,
  val primaryKey: String?,
  val paginationStrategy: String?,
  val incrementalSyncEnabled: Boolean,
)

data class TemplateSpecProperty(
  val name: String?,
  val type: String?,
  val description: String?,
  val default: Any?,
)

@Singleton
class ContributionTemplates {
  var templateEngine: PebbleEngine? = null

  init {
    templateEngine = PebbleEngine.Builder().build()
  }

  // HELPERS

  private fun renderTemplateString(
    templatePath: String,
    context: Map<String, Any?>,
  ): String {
    val compiledTemplate: PebbleTemplate = templateEngine!!.getTemplate(templatePath)
    val writer: Writer = StringWriter()
    compiledTemplate.evaluate(writer, context)
    return writer.toString()
  }

  fun getPaginatorTypeForStreamObject(streamObject: Map<String, Any>): String? {
    val paginator = streamObject["retriever"] as? Map<String, Any>
    val pagination = paginator?.get("paginator") as? Map<String, Any>
    return pagination?.get("type") as? String
  }

  fun toTemplateStreams(streams: List<Map<String, Any>>?): List<TemplateStream> {
    return streams?.map { stream ->
      TemplateStream(
        name = stream["name"] as? String,
        primaryKey = primaryKeyToString(stream["primary_key"]),
        paginationStrategy = getPaginatorTypeForStreamObject(stream) ?: "No pagination",
        incrementalSyncEnabled = stream["incremental_sync"] != null,
      )
    } ?: emptyList()
  }

  /**
   * Converts a primary key to a string representation.
   *
   * Examples:
   * - "id" -> "id"
   * - ["id"] -> "id"
   * - ["id", "name"] -> "id.name"
   * - ["id", ["name", "age"]] -> "id.name.age"
   * - [["id"], ["name", "age"]] -> "id.name.age"
   */
  fun primaryKeyToString(primaryKey: Any?): String {
    return when (primaryKey) {
      is String -> primaryKey
      is List<*> -> primaryKey.joinToString(".") { primaryKeyToString(it) }
      else -> ""
    }
  }

  fun toTemplateSpecProperties(spec: Map<String, Any>): List<TemplateSpecProperty> {
    val connectionSpec = spec["connection_specification"] as Map<String, Any>
    val properties = connectionSpec["properties"] as? Map<String, Map<String, Any>>
    return properties?.map { (key, property) ->
      TemplateSpecProperty(
        name = key,
        type = property["type"] as? String,
        description = toTemplatePropertyDescription(property),
        default = property["default"],
      )
    } ?: emptyList()
  }

  fun toTemplatePropertyDescription(property: Map<String, Any>): String {
    val title = if (property["title"] != null) "${property["title"]}. " else ""
    val description = if (property["description"] is String) (property["description"] as String).replace("\n", " ") else ""
    return "$title$description"
  }

  // TEMPLATES

  fun renderContributionReadmeMd(
    connectorImageName: String,
    connectorName: String,
    description: String,
  ): String {
    val context = mapOf("connectorImageName" to connectorImageName, "connectorName" to connectorName, "description" to description)
    return renderTemplateString("contribution_templates/readme.md.peb", context)
  }

  fun renderContributionDocsMd(
    connectorImageName: String,
    connectorVersionTag: String,
    connectorName: String,
    description: String,
    manifestParser: ManifestParser,
    releaseDate: String,
    authorUsername: String,
  ): String {
    val streams = toTemplateStreams(manifestParser.streams)
    val specProperties = toTemplateSpecProperties(manifestParser.spec)
    val context =
      mapOf(
        "connectorImageName" to connectorImageName,
        "connectorName" to connectorName,
        "versionTag" to connectorVersionTag,
        "description" to description,
        "specProperties" to specProperties,
        "streams" to streams,
        "releaseDate" to releaseDate,
        "changelogMessage" to "Initial release by $authorUsername via Connector Builder",
      )
    return renderTemplateString("contribution_templates/docs.md.peb", context)
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
