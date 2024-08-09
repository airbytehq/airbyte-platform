@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.templates

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.connector_builder.utils.ManifestParser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

fun jacksonSerialize(input: String): String {
  val mapper = ObjectMapper()
  // Serialize the string to JSON, which will automatically escape quotes
  val jsonString = mapper.writeValueAsString(input)
  // Remove the enclosing double quotes that Jackson adds
  return jsonString
    .substring(1, jsonString.length - 1)
    .replace("\\\"", "\"")
}

class ContributionTemplatesTest {
  @Test
  fun `test readme template`() {
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

  @Test
  fun `test docs template`() {
    val contributionTemplates = ContributionTemplates()
    val connectorImageName = "test"
    val connectorName = "Test Connector"
    val description = "This is a test connector."
    val versionTag = "0.0.1"
    val releaseDate = "2021-01-01"
    val username = "testuser"

    val serialzedYamlContent = this::class.java.getResource("/valid_manifest.yaml")!!.readText()
    val jacksonYaml = jacksonSerialize(serialzedYamlContent)

    val manifestParser = ManifestParser(jacksonYaml)

    val docs =
      contributionTemplates.renderContributionDocsMd(
        connectorImageName = connectorImageName,
        connectorName = connectorName,
        connectorVersionTag = versionTag,
        description = description,
        releaseDate = releaseDate,
        manifestParser = manifestParser,
        authorUsername = username,
      )

    assert(docs.contains(connectorName))
    assert(docs.contains(description))
    assert(docs.contains(versionTag))
    assert(docs.contains(releaseDate))
    assert(docs.contains(username))

    for (stream in manifestParser.streams!!) {
      // Assert that the rendered docs contains the stream name
      assert(docs.contains("| ${stream["name"]} |"))
    }

    val connectionSpecification = manifestParser.spec?.get("connection_specification") as Map<String, Any>
    val properties = connectionSpecification["properties"] as Map<String, Any>

    for (prop in properties) {
      // Assert that the rendered docs contains the spec name
      assert(docs.contains("| `${prop.key}` |"))
    }
  }

  @Test
  fun `test privateKeyToString with Array`() {
    val contributionTemplates = ContributionTemplates()
    val expectedArrayPrivateKeyString = "id.name.age"

    val privateKey1 = listOf("id", listOf("name", "age"))
    val privateKey2 = listOf(listOf("id"), listOf("name", "age"))
    val privateKey3 = listOf("id", "name", "age")

    assertEquals(contributionTemplates.primaryKeyToString(privateKey1), expectedArrayPrivateKeyString)
    assertEquals(contributionTemplates.primaryKeyToString(privateKey2), expectedArrayPrivateKeyString)
    assertEquals(contributionTemplates.primaryKeyToString(privateKey3), expectedArrayPrivateKeyString)
  }

  @Test
  fun `test privateKeyToString with String`() {
    val contributionTemplates = ContributionTemplates()
    val expectedString = "id"

    val privateKey1 = "id"
    val privateKey2 = listOf("id")

    assertEquals(contributionTemplates.primaryKeyToString(privateKey1), expectedString)
    assertEquals(contributionTemplates.primaryKeyToString(privateKey2), expectedString)
  }

  @Test
  fun `test toTemplateSpecProperties`() {
    val contributionTemplates = ContributionTemplates()
    val spec =
      mapOf(
        "connection_specification" to
          mapOf(
            "properties" to
              mapOf(
                "property1" to
                  mapOf(
                    "type" to "string",
                    "title" to "property1 title",
                    "description" to "property1\ndescription",
                    "default" to "property1 default",
                  ),
                "property2" to
                  mapOf(
                    "type" to "integer",
                    "description" to "property2 description",
                    "default" to 0,
                  ),
              ),
          ),
      )

    val expectedSpecProperties =
      listOf(
        TemplateSpecProperty(
          name = "property1",
          type = "string",
          description = "property1 title. property1 description",
          default = "property1 default",
        ),
        TemplateSpecProperty(
          name = "property2",
          type = "integer",
          description = "property2 description",
          default = 0,
        ),
      )

    assertEquals(contributionTemplates.toTemplateSpecProperties(spec), expectedSpecProperties)
  }

  @Test
  fun `test toTemplateStreams`() {
    val contributionTemplates = ContributionTemplates()
    val streams =
      listOf(
        mapOf(
          "name" to "stream1",
          "primary_key" to listOf("id", "name"),
          "retriever" to
            mapOf(
              "paginator" to
                mapOf(
                  "type" to "PageIncrement",
                ),
            ),
          "incremental_sync" to
            mapOf(
              "type" to "DatetimeBasedCursor",
            ),
        ),
        mapOf(
          "name" to "stream2",
          "primary_key" to "id",
          "retriever" to
            mapOf(
              "paginator" to null,
            ),
          "incremental_sync" to null,
        ),
      ) as List<Map<String, Any>>

    val expectedTemplateStreams =
      listOf(
        TemplateStream(
          name = "stream1",
          primaryKey = "id.name",
          paginationStrategy = "PageIncrement",
          incrementalSyncEnabled = true,
        ),
        TemplateStream(
          name = "stream2",
          primaryKey = "id",
          paginationStrategy = "No pagination",
          incrementalSyncEnabled = false,
        ),
      )

    assertEquals(contributionTemplates.toTemplateStreams(streams), expectedTemplateStreams)
  }
}
