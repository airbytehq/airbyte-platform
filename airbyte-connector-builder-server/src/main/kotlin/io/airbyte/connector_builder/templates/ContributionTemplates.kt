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

  fun renderContributionReadme(
    connectorImageName: String,
    connectorName: String,
    description: String,
  ): String {
    val context = mapOf("connectorImageName" to connectorImageName, "connectorName" to connectorName, "description" to description)
    return renderTemplateString("contribution_templates/readme.md", context)
  }
}
