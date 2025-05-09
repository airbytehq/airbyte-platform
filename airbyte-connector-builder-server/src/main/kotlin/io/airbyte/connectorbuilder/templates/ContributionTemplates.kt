/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.templates

import io.airbyte.connectorbuilder.services.GithubContributionService
import io.airbyte.connectorbuilder.utils.BuilderContributionInfo
import io.airbyte.connectorbuilder.utils.ManifestParser
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

  @Suppress("UNCHECKED_CAST")
  fun getPaginatorTypeForStreamObject(streamObject: Map<String, Any>): String? {
    val paginator = streamObject["retriever"] as? Map<String, Any>
    val pagination = paginator?.get("paginator") as? Map<String, Any>
    return pagination?.get("type") as? String
  }

  fun toTemplateStreams(streams: List<Map<String, Any>>?): List<TemplateStream> =
    streams?.map { stream ->
      TemplateStream(
        name = stream["name"] as? String,
        primaryKey = primaryKeyToString(stream["primary_key"]),
        paginationStrategy = getPaginatorTypeForStreamObject(stream) ?: "No pagination",
        incrementalSyncEnabled = stream["incremental_sync"] != null,
      )
    } ?: emptyList()

  fun getAllowedHosts(streams: List<Map<String, Any>>): List<String> {
    val hostnameRegex = Regex("^(?:https?://)?(?:www\\.)?([^/{}]+)")

    val hosts =
      @Suppress("UNCHECKED_CAST")
      streams.mapNotNull { stream ->
        val retriever = stream["retriever"] as? Map<String, Any>
        val requester = retriever?.get("requester") as? Map<String, Any>
        val baseUrl = requester?.get("url_base") as? String

        baseUrl?.let { url ->
          hostnameRegex.find(url)?.groupValues?.getOrNull(1)
        }
      }

    // Since the requester is on every stream, we only need unique hostnames
    return hosts.distinct()
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
  fun primaryKeyToString(primaryKey: Any?): String =
    when (primaryKey) {
      is String -> primaryKey
      is List<*> -> primaryKey.joinToString(".") { primaryKeyToString(it) }
      else -> ""
    }

  @Suppress("UNCHECKED_CAST")
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

  fun renderContributionReadmeMd(contributionInfo: BuilderContributionInfo): String {
    val context =
      mapOf(
        "connectorImageName" to contributionInfo.connectorImageName,
        "connectorName" to contributionInfo.connectorName,
        "connectorDescription" to contributionInfo.connectorDescription,
      )
    return renderTemplateString("contribution_templates/readme.md.peb", context)
  }

  fun renderContributionDocsMd(contributionInfo: BuilderContributionInfo): String {
    val manifestParser = ManifestParser(contributionInfo.manifestYaml)
    val streams = toTemplateStreams(manifestParser.streams)
    val specProperties = toTemplateSpecProperties(manifestParser.spec)
    val context =
      mapOf(
        "connectorImageName" to contributionInfo.connectorImageName,
        "connectorName" to contributionInfo.connectorName,
        "versionTag" to contributionInfo.versionTag,
        "connectorDescription" to contributionInfo.connectorDescription,
        "specProperties" to specProperties,
        "streams" to streams,
        "releaseDate" to contributionInfo.updateDate,
        "changelogMessage" to contributionInfo.changelogMessage,
      )
    return renderTemplateString("contribution_templates/docs.md.peb", context)
  }

  fun renderContributionMetadataYaml(
    contributionInfo: BuilderContributionInfo,
    githubContributionService: GithubContributionService,
  ): String {
    val manifestParser = ManifestParser(contributionInfo.manifestYaml)
    val allowedHosts = getAllowedHosts(manifestParser.streams)

    // TODO: Ensure metadata is correctly formatted
    // TODO: Merge metadata with existing metadata if it exists
    val context =
      mapOf(
        // TODO: Parse Allowed Hosts from manifest
        "allowedHosts" to allowedHosts,
        "connectorImageName" to contributionInfo.connectorImageName,
        "baseImage" to contributionInfo.baseImage,
        "actorDefinitionId" to contributionInfo.actorDefinitionId,
        "versionTag" to contributionInfo.versionTag,
        "connectorName" to contributionInfo.connectorName,
        "releaseDate" to contributionInfo.updateDate,
        "connectorDocsSlug" to githubContributionService.connectorDocsSlug,
      )
    return renderTemplateString("contribution_templates/metadata.yaml.peb", context)
  }

  fun renderIconSvg(): String = renderTemplateString("contribution_templates/icon.svg", emptyMap())

  fun renderAcceptanceTestConfigYaml(contributionInfo: BuilderContributionInfo): String {
    val context = mapOf("connectorImageName" to contributionInfo.connectorImageName)
    return renderTemplateString("contribution_templates/acceptance-test-config.yml.peb", context)
  }

  fun renderContributionPullRequestDescription(contributionInfo: BuilderContributionInfo): String {
    val manifestParser = ManifestParser(contributionInfo.manifestYaml)
    val streams = toTemplateStreams(manifestParser.streams)
    val specProperties = toTemplateSpecProperties(manifestParser.spec)
    val context =
      mapOf(
        "connectorImageName" to contributionInfo.connectorImageName,
        "connectorName" to contributionInfo.connectorName,
        "connectorDescription" to contributionInfo.connectorDescription,
        "contributionDescription" to contributionInfo.contributionDescription,
        "specProperties" to specProperties,
        "streams" to streams,
      )
    val templatePath =
      if (contributionInfo.isEdit) {
        "contribution_templates/pull-request-edit.md.peb"
      } else {
        "contribution_templates/pull-request-new-connector.md.peb"
      }
    return renderTemplateString(templatePath, context)
  }
}
