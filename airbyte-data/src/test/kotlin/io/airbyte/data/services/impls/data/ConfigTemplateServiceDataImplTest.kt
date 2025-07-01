/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.data.repositories.ConfigTemplateRepository
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

typealias EntityConfigTemplate = io.airbyte.data.repositories.entities.ConfigTemplate

class ConfigTemplateServiceDataImplTest {
  private lateinit var service: ConfigTemplateService
  private lateinit var configTemplateService: ConfigTemplateService
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var repository: ConfigTemplateRepository
  private lateinit var sourceService: SourceService
  private lateinit var oauthService: OAuthService
  private lateinit var validator: JsonSchemaValidator

  private val objectMapper: ObjectMapper = ObjectMapper()
  private val connectorSpecificationWithRequiredFields = createConnectorSpecification(listOf("a_config_field", "an_integer_field"))
  private val connectorSpecificationWithoutRequiredFields = createConnectorSpecification(emptyList())

  // Common test data
  private val actorDefinitionId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val configTemplateId = UUID.randomUUID()
  private val partialDefaultConfig: JsonNode = objectMapper.readTree("{}")
  private val userConfigSpecObject =
    objectMapper.valueToTree<JsonNode>(
      ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
    )
  private val sourceDefinition =
    StandardSourceDefinition().apply {
      name = "test-source"
      iconUrl = "test-icon"
      sourceDefinitionId = actorDefinitionId
    }

  private val actorDefinitionVersion = ActorDefinitionVersion()

  @BeforeEach
  fun setup() {
    configTemplateService = mockk()
    actorDefinitionService = mockk<ActorDefinitionService>()
    repository = mockk<ConfigTemplateRepository>()
    sourceService = mockk<SourceService>()
    oauthService = mockk<OAuthService>()
    validator = JsonSchemaValidator()

    service = ConfigTemplateServiceDataImpl(repository, actorDefinitionService, sourceService, oauthService, validator)

    every { sourceService.getStandardSourceDefinition(actorDefinitionId, false) } returns sourceDefinition
    every { sourceService.listStandardSourceDefinitions(false) } returns listOf(sourceDefinition)
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId) } returns
      Optional.of(
        actorDefinitionVersion,
      )

    actorDefinitionVersion.spec = connectorSpecificationWithoutRequiredFields
  }

  @Nested
  inner class CreateTemplateTest {
    @Test
    fun `test createConfigTemplate`() {
      val partialDefaultConfig = objectMapper.createObjectNode()
      val userConfigSpec = createPartialUserConfigSpecAsObject(mapOf("a_config_field" to "string", "an_integer_field" to "integer"))

      val id = UUID.randomUUID()

      val entity =
        EntityConfigTemplate(
          id = id,
          organizationId = organizationId,
          actorDefinitionId = actorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpec,
        )

      every {
        repository.save(
          any(),
        )
      } returns entity

      val configTemplate =
        service.createTemplate(
          OrganizationId(organizationId),
          ActorDefinitionId(actorDefinitionId),
          partialDefaultConfig,
          userConfigSpec,
        )

      assertEquals(configTemplate.configTemplate.organizationId, organizationId)
      assertEquals(configTemplate.configTemplate.actorDefinitionId, actorDefinitionId)
      assertEquals(configTemplate.configTemplate.partialDefaultConfig, partialDefaultConfig)
      assertEquals(userConfigSpec, objectMapper.valueToTree(configTemplate.configTemplate.userConfigSpec))
    }

    @Test
    fun `test createConfigTemplate without a user spec`() {
      actorDefinitionVersion.spec = connectorSpecificationWithRequiredFields

      val partialDefaultConfigJsonString =
        """
        {
          "an_integer_field": 42
        }
        """.trimIndent()
      val partialDefaultConfig = objectMapper.readTree(partialDefaultConfigJsonString)

      val expectedUserConfigSpec = createPartialUserConfigSpecAsObject(mapOf("a_config_field" to "string"))

      val id = UUID.randomUUID()

      val entity =
        EntityConfigTemplate(
          id = id,
          organizationId = organizationId,
          actorDefinitionId = actorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = expectedUserConfigSpec,
        )

      every {
        repository.save(
          any(),
        )
      } returns entity

      val configTemplate =
        service.createTemplate(
          OrganizationId(organizationId),
          ActorDefinitionId(actorDefinitionId),
          partialDefaultConfig,
        )

      assertEquals(configTemplate.configTemplate.organizationId, organizationId)
      assertEquals(configTemplate.configTemplate.actorDefinitionId, actorDefinitionId)
      assertEquals(configTemplate.configTemplate.partialDefaultConfig, partialDefaultConfig)
      assertEquals(expectedUserConfigSpec, objectMapper.valueToTree(configTemplate.configTemplate.userConfigSpec))
    }

    @Test
    fun `createDefaultValuesFromSchema constructs default values`() {
      val schemaDefaultValueService = SchemaDefaultValueHelper()
      val jsonNodeString =
        """
        {
          "type": "object",
          "properties": {
            "string": {
              "type": "string"
            },
            "integer": {
              "type": "integer"
            },
            "number": {
              "type": "number"
            },
            "boolean": {
              "type": "boolean"
            },
            "array_of_objects": {
              "type": "array",
              "items": {
                "type": "object",
                "properties": {
                  "string": {
                    "type": "string"
                  }
                },
                "required": ["string"]
              }
            },
            "array_of_numbers": {
              "type": "array",
              "items": {
                "type": "number"
              }
            },
            "array_no_items": {
              "type": "array"
            },
            "object": {
              "type": "object",
              "properties": {
                "string": {
                  "type": "string"
                },
                "integer": {
                  "type": "integer"
                }
              },
              "required": ["string", "integer"]
            },
            "object_deeply_nested": {
              "type": "object",
              "properties": {
                "nested": {
                  "type": "object",
                  "properties": {
                    "string": {
                      "type": "string"
                    },
                    "date": {
                      "type": "string",
                      "format": "date"
                    }
                  },
                  "required": ["string", "date"]
                },
                "integer": {
                  "type": "integer"
                }
              },
              "required": ["nested", "integer"]
            },
            "date": {
              "type": "string",
              "format": "date"
            },
            "date_time": {
              "type": "string",
              "format": "date-time"
            },
            "oneOf": {
              "type": "object",
              "oneOf": [
                {
                  "properties": {
                    "password": {
                      "type": "string"
                    }
                  },
                  "required": ["password"]
                },
                {
                  "properties": {
                    "key": {
                      "type": "string"
                    }
                  },
                  "required": ["key"]
                }
              ]
            }
          },
          "required": ["string", "integer", "number", "boolean", "array_of_objects", "array_of_numbers", "array_no_items", "object", "object_deeply_nested", "date", "date_time", "oneOf"]
        }
        """.trimIndent()
      val jsonNode = objectMapper.readTree(jsonNodeString)

      val expectedJsonString =
        """
        {
          "string": "mock_string",
          "integer": 42,
          "number": 3.14,
          "boolean": true,
          "array_of_objects": [{"string": "mock_string"}],
          "array_of_numbers": [3.14],
          "array_no_items": ["mock_string"],
          "object": {
            "string": "mock_string",
            "integer": 42
          },
          "object_deeply_nested": {
            "nested": {
              "string": "mock_string",
              "date": "1970-01-01"
            },
            "integer": 42
          },
          "date": "1970-01-01",
          "date_time": "1970-01-01T00:00:00Z",
          "oneOf": {
            "password": "mock_string"
          }
        }
        """.trimIndent()
      val expected = objectMapper.readTree(expectedJsonString)
      val actual = schemaDefaultValueService.createDefaultValuesFromSchema(jsonNode)

      assertEquals(expected, actual)
    }

    @Test
    fun `test createConfigTemplate where default params have complex values`() {
      // Set up the actor definition spec
      val actorDefinitionSpecJsonString =
        """
        {
          "type": "object",
          "properties": {
            "regex_pattern": {
              "type": "string",
              "pattern": "^[a-zA-Z0-9]+$",
              "pattern_descriptor": "aToZ123"
            },
            "nested": {
              "type": "object",
              "properties": {
                "regex_pattern": {
                  "type": "string",
                  "pattern": "^[a-zA-Z0-9]+$",
                  "pattern_descriptor": "aToZ123"
                },
                "date_time": {
                  "type": "string",
                  "format": "date-time"
                }
              },
              "required": ["regex_pattern", "date_time"]
            },
            "date": {
              "type": "string",
              "format": "date"
            },
            "integer_property": {
              "type": "integer"
            }
          },
          "required": ["regex_pattern", "nested", "date"]
        }
        """.trimIndent()
      val actorDefinitionVersion = ActorDefinitionVersion()
      every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId) } returns
        Optional.of(
          actorDefinitionVersion,
        )
      actorDefinitionVersion.spec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree(actorDefinitionSpecJsonString))

      val partialDefaultConfigJsonString =
        """
        {
          "integer_property": 42
        }
        """.trimIndent()
      val partialDefaultConfig = objectMapper.readTree(partialDefaultConfigJsonString)

      // The user config can be exactly the same as the actor config
      val userConfigSpecJsonString = actorDefinitionSpecJsonString
      val userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree(userConfigSpecJsonString))
      val userConfigSpecJsonObject = objectMapper.valueToTree<JsonNode>(userConfigSpec)

      val id = UUID.randomUUID()

      val entity =
        EntityConfigTemplate(
          id = id,
          organizationId = organizationId,
          actorDefinitionId = actorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpecJsonObject,
        )

      every {
        repository.save(
          any(),
        )
      } returns entity

      val configTemplate =
        service.createTemplate(
          OrganizationId(organizationId),
          ActorDefinitionId(actorDefinitionId),
          partialDefaultConfig,
          userConfigSpecJsonObject,
        )

      assertEquals(configTemplate.configTemplate.organizationId, organizationId)
      assertEquals(configTemplate.configTemplate.actorDefinitionId, actorDefinitionId)
      assertEquals(configTemplate.configTemplate.partialDefaultConfig, partialDefaultConfig)
      assertEquals(userConfigSpecJsonObject, objectMapper.valueToTree(configTemplate.configTemplate.userConfigSpec))
    }

    @Test
    fun `test create fails if the combination of the default params and user spec does not match the full config spec`() {
      val partialDefaultConfig = objectMapper.createObjectNode()
      val userConfigSpec = createPartialUserConfigSpecAsObject(mapOf())

      actorDefinitionVersion.actorDefinitionId = actorDefinitionId
      actorDefinitionVersion.spec = connectorSpecificationWithRequiredFields

      every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId) } returns
        Optional.of(
          actorDefinitionVersion,
        )

      val entity =
        EntityConfigTemplate(
          id = UUID.randomUUID(),
          organizationId = organizationId,
          actorDefinitionId = actorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpec,
        )

      every {
        repository.save(
          any(),
        )
      } returns entity

      org.junit.jupiter.api.assertThrows<JsonValidationException> {
        service.createTemplate(OrganizationId(organizationId), ActorDefinitionId(actorDefinitionId), partialDefaultConfig, userConfigSpec)
      }
    }

    @Test
    fun `test create succeeds if the default params respects the config spec`() {
      val partialDefaultConfig = objectMapper.createObjectNode()

      partialDefaultConfig.put("a_config_field", "value1")
      partialDefaultConfig.put("an_integer_field", 1)
      val userConfigSpec = createPartialUserConfigSpecAsObject(mapOf())

      actorDefinitionVersion.actorDefinitionId = actorDefinitionId
      actorDefinitionVersion.spec = connectorSpecificationWithRequiredFields

      every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId) } returns
        Optional.of(
          actorDefinitionVersion,
        )

      val entity =
        EntityConfigTemplate(
          id = UUID.randomUUID(),
          organizationId = organizationId,
          actorDefinitionId = actorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpec,
        )

      every {
        repository.save(
          any(),
        )
      } returns entity

      val configTemplate =
        service.createTemplate(
          OrganizationId(organizationId),
          ActorDefinitionId(actorDefinitionId),
          partialDefaultConfig,
          userConfigSpec,
        )

      assertEquals(partialDefaultConfig, configTemplate.configTemplate.partialDefaultConfig)
      assertEquals(userConfigSpec, objectMapper.valueToTree(configTemplate.configTemplate.userConfigSpec))
    }

    @Test
    fun `test create fails if the default params do not have the right type`() {
      val partialDefaultConfig = objectMapper.createObjectNode()
      partialDefaultConfig.put("a_config_field", "value1")
      partialDefaultConfig.put("an_integer_field", "not_an_integer")
      val userConfigSpec = createPartialUserConfigSpecAsObject(mapOf())

      actorDefinitionVersion.actorDefinitionId = actorDefinitionId
      actorDefinitionVersion.spec = connectorSpecificationWithRequiredFields

      every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId) } returns
        Optional.of(
          actorDefinitionVersion,
        )

      val entity =
        EntityConfigTemplate(
          id = UUID.randomUUID(),
          organizationId = organizationId,
          actorDefinitionId = actorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpec,
        )

      every {
        repository.save(
          any(),
        )
      } returns entity

      org.junit.jupiter.api.assertThrows<JsonValidationException> {
        service.createTemplate(
          OrganizationId(organizationId),
          ActorDefinitionId(actorDefinitionId),
          partialDefaultConfig,
          userConfigSpec,
        )
      }
    }

    @Test
    fun `test create succeeds if the user spec contains all required fields from the spec`() {
      val partialDefaultConfig = objectMapper.createObjectNode()
      val userConfigSpecObject = createPartialUserConfigSpecAsObject(mapOf("a_config_field" to "string", "an_integer_field" to "integer"))

      actorDefinitionVersion.actorDefinitionId = actorDefinitionId
      actorDefinitionVersion.spec = connectorSpecificationWithRequiredFields

      every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId) } returns
        Optional.of(
          actorDefinitionVersion,
        )

      val entity =
        EntityConfigTemplate(
          id = UUID.randomUUID(),
          organizationId = organizationId,
          actorDefinitionId = actorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpecObject,
        )

      every {
        repository.save(
          any(),
        )
      } returns entity

      val configTemplate =
        service.createTemplate(
          OrganizationId(organizationId),
          ActorDefinitionId(actorDefinitionId),
          partialDefaultConfig,
          userConfigSpecObject,
        )

      assertEquals(configTemplate.configTemplate.partialDefaultConfig, partialDefaultConfig)
      assertEquals(objectMapper.valueToTree(configTemplate.configTemplate.userConfigSpec), userConfigSpecObject)
    }

    @Test
    fun `test an error is raised when the actor definition does not exist`() {
      val partialDefaultConfig = objectMapper.createObjectNode()
      val userConfigSpec = createPartialUserConfigSpecAsObject(mapOf("a_config_field" to "string", "an_integer_field" to "integer"))

      actorDefinitionVersion.actorDefinitionId = actorDefinitionId
      actorDefinitionVersion.spec = connectorSpecificationWithRequiredFields

      every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId) } returns
        Optional.empty()

      val entity =
        EntityConfigTemplate(
          id = UUID.randomUUID(),
          organizationId = organizationId,
          actorDefinitionId = actorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpec,
        )

      every {
        repository.save(
          any(),
        )
      } returns entity

      org.junit.jupiter.api.assertThrows<RuntimeException> {
        service.createTemplate(
          OrganizationId(organizationId),
          ActorDefinitionId(actorDefinitionId),
          partialDefaultConfig,
          userConfigSpec,
        )
      }
    }

    @Test
    fun `test spec with a oneof`() {
      val connectorSpecNode =
        objectMapper.readTree(
          "{\n" +
            "    \"\$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
            "    \"title\": \"GitHub Source Spec\",\n" +
            "    \"type\": \"object\",\n" +
            "    \"required\": [\"credentials\", \"repositories\"],\n" +
            "    \"additionalProperties\": true,\n" +
            "    \"properties\": {\n" +
            "      \"credentials\": {\n" +
            "        \"title\": \"Authentication\",\n" +
            "        \"description\": \"Choose how to authenticate to GitHub\",\n" +
            "        \"type\": \"object\",\n" +
            "        \"order\": 0,\n" +
            "        \"group\": \"auth\",\n" +
            "        \"oneOf\": [\n" +
            "          {\n" +
            "            \"type\": \"object\",\n" +
            "            \"title\": \"OAuth\",\n" +
            "            \"required\": [\"access_token\"],\n" +
            "            \"properties\": {\n" +
            "              \"option_title\": {\n" +
            "                \"type\": \"string\",\n" +
            "                \"const\": \"OAuth Credentials\",\n" +
            "                \"order\": 0\n" +
            "              },\n" +
            "              \"access_token\": {\n" +
            "                \"type\": \"string\",\n" +
            "                \"title\": \"Access Token\",\n" +
            "                \"description\": \"OAuth access token\",\n" +
            "                \"airbyte_secret\": true\n" +
            "              },\n" +
            "              \"client_id\": {\n" +
            "                \"type\": \"string\",\n" +
            "                \"title\": \"Client Id\",\n" +
            "                \"description\": \"OAuth Client Id\",\n" +
            "                \"airbyte_secret\": true\n" +
            "              },\n" +
            "              \"client_secret\": {\n" +
            "                \"type\": \"string\",\n" +
            "                \"title\": \"Client secret\",\n" +
            "                \"description\": \"OAuth Client secret\",\n" +
            "                \"airbyte_secret\": true\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          {\n" +
            "            \"type\": \"object\",\n" +
            "            \"title\": \"Personal Access Token\",\n" +
            "            \"required\": [\"personal_access_token\"],\n" +
            "            \"properties\": {\n" +
            "              \"option_title\": {\n" +
            "                \"type\": \"string\",\n" +
            "                \"const\": \"PAT Credentials\",\n" +
            "                \"order\": 0\n" +
            "              },\n" +
            "              \"personal_access_token\": {\n" +
            "                \"type\": \"string\",\n" +
            "                \"title\": \"Personal Access Tokens\",\n" +
            "                \"description\": \"Log into GitHub\",\n" +
            "                \"airbyte_secret\": true\n" +
            "              }\n" +
            "            }\n" +
            "          }\n" +
            "        ]\n" +
            "      },\n" +
            "      \"repository\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"examples\": [\n" +
            "          \"airbytehq/airbyte airbytehq/another-repo\",\n" +
            "          \"airbytehq/*\",\n" +
            "          \"airbytehq/airbyte\"\n" +
            "        ],\n" +
            "        \"title\": \"GitHub Repositories\",\n" +
            "        \"description\": \"(DEPRCATED) Space-delimited list of GitHub organizations/repositories\",\n" +
            "        \"airbyte_hidden\": true,\n" +
            "        \"pattern\": \"\",\n" +
            "        \"pattern_descriptor\": \"org/repo org/another-repo org/*\"\n" +
            "      },\n" +
            "      \"repositories\": {\n" +
            "        \"type\": \"array\",\n" +
            "        \"items\": {\n" +
            "          \"type\": \"string\",\n" +
            "          \"pattern\": \"\" \n" +
            "        },\n" +
            "        \"minItems\": 1,\n" +
            "        \"examples\": [\n" +
            "          \"airbytehq/airbyte\",\n" +
            "          \"airbytehq/another-repo\",\n" +
            "          \"airbytehq/*\",\n" +
            "          \"airbytehq/a*\"\n" +
            "        ],\n" +
            "        \"title\": \"GitHub Repositories\",\n" +
            "        \"description\": \"List of GitHub organizations/repositories\"," +
            "        \"order\": 1,\n" +
            "        \"pattern_descriptor\": \"org/repo org/another-repo org/* org/a*\"\n" +
            "      },\n" +
            "      \"start_date\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"title\": \"Start date\",\n" +
            "        \"description\": \"The date from which you'd like to replicate data from GitHub in the format YYYY-MM-DDT00:00:00Z\"," +
            "        \"examples\": [\"2021-03-01T00:00:00Z\"]," +
            "        \"pattern\": \"\",\n" +
            "        \"pattern_descriptor\": \"YYYY-MM-DDTHH:mm:ssZ\",\n" +
            "        \"order\": 2,\n" +
            "        \"format\": \"date-time\"\n" +
            "      },\n" +
            "      \"api_url\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"examples\": [\"https://github.com\", \"https://github.company.org\"],\n" +
            "        \"title\": \"API URL\",\n" +
            "        \"default\": \"https://api.github.com/\",\n" +
            "        \"description\": \"Please enter your basic URL from self-hosted GitHub instance or leave it empty to use GitHub.\",\n" +
            "        \"order\": 3\n" +
            "      },\n" +
            "      \"branch\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"title\": \"Branch\",\n" +
            "        \"examples\": [\"airbytehq/airbyte/master airbytehq/airbyte/my-branch\"],\n" +
            "        \"description\": \"(DEPRCATED) Space-delimited list of GitHub repository branches to pull commits for, e.g.\",\n" +
            "        \"airbyte_hidden\": true,\n" +
            "        \"pattern_descriptor\": \"org/repo/branch1 org/repo/branch2\"\n" +
            "      },\n" +
            "      \"branches\": {\n" +
            "        \"type\": \"array\",\n" +
            "        \"items\": {\n" +
            "          \"type\": \"string\"\n" +
            "        },\n" +
            "        \"title\": \"Branches\",\n" +
            "        \"examples\": [\"airbytehq/airbyte/master\", \"airbytehq/airbyte/my-branch\"],\n" +
            "        \"description\": \"List of GitHub repository branches to pull commits for, e.g. `airbytehq/airbyte/master`.\",\n" +
            "        \"order\": 4,\n" +
            "        \"pattern_descriptor\": \"org/repo/branch1 org/repo/branch2\"\n" +
            "      },\n" +
            "      \"max_waiting_time\": {\n" +
            "        \"type\": \"integer\",\n" +
            "        \"title\": \"Max Waiting Time (in minutes)\",\n" +
            "        \"examples\": [10, 30, 60],\n" +
            "        \"default\": 10,\n" +
            "        \"minimum\": 1,\n" +
            "        \"maximum\": 60,\n" +
            "        \"description\": \"Max Waiting Time for rate limit.\",\n" +
            "        \"order\": 5\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"advanced_auth\": {\n" +
            "    \"auth_flow_type\": \"oauth2.0\",\n" +
            "    \"predicate_key\": [\"credentials\", \"option_title\"],\n" +
            "    \"predicate_value\": \"OAuth Credentials\",\n" +
            "    \"oauth_config_specification\": {\n" +
            "      \"complete_oauth_output_specification\": {\n" +
            "        \"type\": \"object\",\n" +
            "        \"additionalProperties\": false,\n" +
            "        \"properties\": {\n" +
            "          \"access_token\": {\n" +
            "            \"type\": \"string\",\n" +
            "            \"path_in_connector_config\": [\"credentials\", \"access_token\"]\n" +
            "          }\n" +
            "        }\n" +
            "      },\n" +
            "      \"complete_oauth_server_input_specification\": {\n" +
            "        \"type\": \"object\",\n" +
            "        \"additionalProperties\": false,\n" +
            "        \"properties\": {\n" +
            "          \"client_id\": {\n" +
            "            \"type\": \"string\"\n" +
            "          },\n" +
            "          \"client_secret\": {\n" +
            "            \"type\": \"string\"\n" +
            "          }\n" +
            "        }\n" +
            "      },\n" +
            "      \"complete_oauth_server_output_specification\": {\n" +
            "        \"type\": \"object\",\n" +
            "        \"additionalProperties\": false,\n" +
            "        \"properties\": {\n" +
            "          \"client_id\": {\n" +
            "            \"type\": \"string\",\n" +
            "            \"path_in_connector_config\": [\"credentials\", \"client_id\"]\n" +
            "          },\n" +
            "          \"client_secret\": {\n" +
            "            \"type\": \"string\",\n" +
            "            \"path_in_connector_config\": [\"credentials\", \"client_secret\"]\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "}",
        )
      val connectorSpecification = ConnectorSpecification().withConnectionSpecification(connectorSpecNode)
      actorDefinitionVersion.spec = connectorSpecification

      val partialDefaultConfig =
        objectMapper.readTree(
          "{\"repositories\": [\"airbytehq/*\"],\n" +
            "\"credentials\":{\"personal_access_token\": \"key\"}}",
        )
      val userConfigSpec = createPartialUserConfigSpecAsObject(mapOf())

      val entity =
        EntityConfigTemplate(
          id = UUID.randomUUID(),
          organizationId = organizationId,
          actorDefinitionId = actorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpec,
        )

      every {
        repository.save(
          any(),
        )
      } returns entity

      val configTemplate =
        service.createTemplate(
          OrganizationId(organizationId),
          ActorDefinitionId(actorDefinitionId),
          partialDefaultConfig,
          userConfigSpec,
        )

      assertEquals(partialDefaultConfig, configTemplate.configTemplate.partialDefaultConfig)
      assertEquals(userConfigSpec, objectMapper.valueToTree(configTemplate.configTemplate.userConfigSpec))
    }
  }

  @Nested
  inner class ListTemplatesTest {
    @Test
    fun testListConfigTemplatesOnlyForRelevantOrg() {
      val entityConfigTemplates =
        listOf(
          // instance default
          EntityConfigTemplate(
            id = UUID.randomUUID(),
            actorDefinitionId = UUID.randomUUID(),
            partialDefaultConfig = partialDefaultConfig,
            userConfigSpec = userConfigSpecObject,
          ),
          // relevant custom
          EntityConfigTemplate(
            id = UUID.randomUUID(),
            organizationId = organizationId,
            actorDefinitionId = actorDefinitionId,
            partialDefaultConfig = partialDefaultConfig,
            userConfigSpec = userConfigSpecObject,
          ),
          // irrelevant custom
          EntityConfigTemplate(
            id = UUID.randomUUID(),
            organizationId = UUID.randomUUID(),
            actorDefinitionId = actorDefinitionId,
            partialDefaultConfig = partialDefaultConfig,
            userConfigSpec = userConfigSpecObject,
          ),
        )

      // Use the same objectMapper instance for both cases
      val expectedConfigTemplates =
        listOf(
          ConfigTemplateWithActorDetails(
            configTemplate =
              ConfigTemplate(
                id = entityConfigTemplates[1].id!!,
                organizationId = entityConfigTemplates[1].organizationId,
                actorDefinitionId = entityConfigTemplates[1].actorDefinitionId,
                partialDefaultConfig = objectMapper.readTree("{}"),
                userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
              ),
            actorIcon = "icon2",
            actorName = "Source2",
          ),
          ConfigTemplateWithActorDetails(
            configTemplate =
              ConfigTemplate(
                id = entityConfigTemplates[0].id!!,
                actorDefinitionId = entityConfigTemplates[0].actorDefinitionId,
                partialDefaultConfig = objectMapper.readTree("{}"),
                userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
              ),
            actorIcon = "icon1",
            actorName = "Source1",
          ),
        )

      every {
        repository.findByOrganizationIdAndActorDefinitionIdInAndTombstoneFalse(
          any(),
          any(),
        )
      } returns listOf(entityConfigTemplates[1])

      every {
        repository.findByActorDefinitionIdInAndOrganizationIdIsNullAndTombstoneFalse(
          any(),
        )
      } returns listOf(entityConfigTemplates[0])

      every {
        sourceService.listStandardSourceDefinitions(false)
      } returns
        listOf(
          StandardSourceDefinition().apply {
            sourceDefinitionId = entityConfigTemplates[0].actorDefinitionId
            name = "Source1"
            iconUrl = "icon1"
          },
          StandardSourceDefinition().apply {
            sourceDefinitionId = entityConfigTemplates[1].actorDefinitionId
            name = "Source2"
            iconUrl = "icon2"
          },
        )

      val configTemplates = service.listConfigTemplatesForOrganization(OrganizationId(organizationId))
      assertEquals(2, configTemplates.size)

      val sortedExpected = expectedConfigTemplates.sortedBy { it.configTemplate.id.toString() }
      val sortedActual = configTemplates.sortedBy { it.configTemplate.id.toString() }
      assertEquals(sortedExpected, sortedActual)
    }

    @Test
    fun testListConfigTemplatesForOrganizationWithNoCustomTemplates() {
      val entityConfigTemplates =
        listOf(
          // default template
          EntityConfigTemplate(
            id = UUID.randomUUID(),
            actorDefinitionId = actorDefinitionId,
            partialDefaultConfig = partialDefaultConfig,
            userConfigSpec = userConfigSpecObject,
          ),
        )

      every { repository.findByOrganizationIdAndActorDefinitionIdInAndTombstoneFalse(organizationId, any()) } returns entityConfigTemplates

      every { repository.findByActorDefinitionIdInAndOrganizationIdIsNullAndTombstoneFalse(any()) } returns entityConfigTemplates

      val result = service.listConfigTemplatesForOrganization(OrganizationId(organizationId))

      assertEquals(1, result.size)
    }

    @Test
    fun testListConfigTemplatesForOrganizationWithNoInstanceDefaults() {
      val entityConfigTemplates =
        listOf(
          // custom template
          EntityConfigTemplate(
            id = UUID.randomUUID(),
            actorDefinitionId = actorDefinitionId,
            organizationId = organizationId,
            partialDefaultConfig = partialDefaultConfig,
            userConfigSpec = userConfigSpecObject,
          ),
        )

      every { repository.findByActorDefinitionIdInAndOrganizationIdIsNullAndTombstoneFalse(any()) } returns listOf()
      every { repository.findByOrganizationIdAndActorDefinitionIdInAndTombstoneFalse(any(), any()) } returns entityConfigTemplates

      val result = service.listConfigTemplatesForOrganization(OrganizationId(organizationId))

      assertEquals(1, result.size)
    }

    @Test
    fun testCombinesDefaultsAndCustomConfigTemplatesAcrossActors() {
      val secondActorDefinitionId = UUID.randomUUID()

      val sourceDefinitions =
        listOf(
          StandardSourceDefinition().apply {
            sourceDefinitionId = actorDefinitionId
            name = "test-source"
            iconUrl = "test-icon"
          },
          StandardSourceDefinition().apply {
            sourceDefinitionId = secondActorDefinitionId
            name = "second-source"
            iconUrl = "second-icon"
          },
        )

      val customTemplate =
        EntityConfigTemplate(
          id = UUID.randomUUID(),
          actorDefinitionId = actorDefinitionId,
          organizationId = organizationId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpecObject,
        )

      val defaultTemplate =
        EntityConfigTemplate(
          id = UUID.randomUUID(),
          actorDefinitionId = secondActorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpecObject,
        )

      every { sourceService.listStandardSourceDefinitions(false) } returns sourceDefinitions

      every {
        repository.findByOrganizationIdAndActorDefinitionIdInAndTombstoneFalse(organizationId, listOf(actorDefinitionId, secondActorDefinitionId))
      } returns listOf(customTemplate)

      every {
        repository.findByActorDefinitionIdInAndOrganizationIdIsNullAndTombstoneFalse(any())
      } returns listOf(defaultTemplate)

      val result = service.listConfigTemplatesForOrganization(OrganizationId(organizationId))
      // Verify we get both templates (one custom, one default)
      assertEquals(2, result.size)

      assertEquals(1, result.count { it.configTemplate.actorDefinitionId == actorDefinitionId })
      assertEquals(1, result.count { it.configTemplate.actorDefinitionId == secondActorDefinitionId })

      val actorNames = result.map { it.actorName }.toSet()
      assertEquals(setOf("test-source", "second-source"), actorNames)
    }

    @Test
    fun testListConfigTemplatesCustomOverridesDefault() {
      // Create two actor definitions
      val sourceDefinitions =
        listOf(
          StandardSourceDefinition().apply {
            sourceDefinitionId = actorDefinitionId
            name = "test-source"
            iconUrl = "test-icon"
          },
        )

      // Create a default template and a custom template for the same actor definition
      val entityConfigTemplates =
        listOf(
          // Default template for actorDefinition
          EntityConfigTemplate(
            id = UUID.randomUUID(),
            actorDefinitionId = actorDefinitionId,
            partialDefaultConfig = partialDefaultConfig,
            userConfigSpec = userConfigSpecObject,
          ),
          // Custom template for the same actorDefinition, using its versionId
          EntityConfigTemplate(
            id = UUID.randomUUID(),
            actorDefinitionId = actorDefinitionId,
            organizationId = organizationId,
            partialDefaultConfig = partialDefaultConfig,
            userConfigSpec = userConfigSpecObject,
          ),
        )

      // Mock source service to return our test source definitions
      every { sourceService.listStandardSourceDefinitions(false) } returns sourceDefinitions

      every {
        repository.findByActorDefinitionIdInAndOrganizationIdIsNullAndTombstoneFalse(listOf(actorDefinitionId))
      } returns listOf(entityConfigTemplates[1])

      // Mock custom templates - return the custom template for actorDefinitionId
      every {
        repository.findByOrganizationIdAndActorDefinitionIdInAndTombstoneFalse(organizationId, listOf(actorDefinitionId))
      } returns listOf(entityConfigTemplates[1])

      val result = service.listConfigTemplatesForOrganization(OrganizationId(organizationId))

      // Verify we only get one template (the custom one)
      assertEquals(1, result.size)

      // Verify the template is the custom one
      val returnedTemplate = result[0].configTemplate

      // Ensure actorDefinitionId is present (custom template) and not actorDefinitionVersionId (default template)
      assertNotNull(returnedTemplate.actorDefinitionId)
      assertEquals(actorDefinitionId, returnedTemplate.actorDefinitionId)
    }
  }

  @Nested
  inner class GetTemplateTests {
    @Test
    fun testGetConfigTemplate() {
      val configTemplate = createConfigTemplate()
      every { repository.findById(configTemplateId) } returns Optional.of(configTemplate)

      val result = service.getConfigTemplate(configTemplateId)

      assertNotNull(result)
      assertEquals(configTemplateId, result.configTemplate.id)
      assertEquals("test-source", result.actorName)
      assertEquals("test-icon", result.actorIcon)
      assertEquals(userConfigSpecObject, objectMapper.valueToTree(result.configTemplate.userConfigSpec))

      verify { repository.findById(configTemplateId) }
    }

    @Test
    fun `test getConfigTemplate`() {
      val configTemplateId = UUID.randomUUID()
      val entityConfigTemplate =
        EntityConfigTemplate(
          id = UUID.randomUUID(),
          organizationId = UUID.randomUUID(),
          actorDefinitionId = actorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpecObject,
        )

      val expectedConfigTemplate =
        ConfigTemplate(
          id = entityConfigTemplate.id!!,
          organizationId = entityConfigTemplate.organizationId,
          actorDefinitionId = entityConfigTemplate.actorDefinitionId,
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
        )

      every { repository.findById(configTemplateId) } returns Optional.of(entityConfigTemplate)

      val configTemplate = service.getConfigTemplate(configTemplateId)

      assertEquals(configTemplate.configTemplate, expectedConfigTemplate)
      assertEquals(configTemplate.actorIcon, sourceDefinition.iconUrl)
      assertEquals(configTemplate.actorName, sourceDefinition.name)
    }

    @Test
    fun `test getConfigTemplate raises an exception if no matching config template`() {
      val configTemplateId = UUID.randomUUID()

      every { repository.findById(configTemplateId) } returns Optional.empty()

      assertThrows(RuntimeException::class.java) {
        service.getConfigTemplate(configTemplateId)
      }
    }
  }

  @Nested
  inner class UpdateTemplateTests {
    private lateinit var configTemplateEntity: EntityConfigTemplate

    @BeforeEach
    fun setup() {
      val partialDefaultConfig = objectMapper.readTree("{}")
      val userConfigSpec = createPartialUserConfigSpecAsObject(mapOf("a_config_field" to "string", "an_integer_field" to "integer"))

      configTemplateEntity =
        EntityConfigTemplate(
          id = UUID.randomUUID(),
          organizationId,
          actorDefinitionId,
          partialDefaultConfig,
          userConfigSpec,
        )

      every { repository.findById(configTemplateEntity.id) } returns Optional.of(configTemplateEntity)
    }

    @Test
    fun `test update raises an exception if the config template does not exist`() {
      val id = UUID.randomUUID()
      every { repository.findById(id) } returns Optional.empty()
      org.junit.jupiter.api.assertThrows<NoSuchElementException> {
        service.updateTemplate(configTemplateId = id, organizationId = OrganizationId(organizationId))
      }
    }

    @Test
    fun `test update nothing should succeed`() {
      val response = service.updateTemplate(configTemplateId = configTemplateEntity.id!!, OrganizationId(organizationId))

      assertEquals(response.configTemplate.id, configTemplateEntity.id)
      assertEquals(response.configTemplate.organizationId, configTemplateEntity.organizationId)
      assertEquals(objectMapper.valueToTree(response.configTemplate.userConfigSpec), configTemplateEntity.userConfigSpec)
      assertEquals(response.configTemplate.actorDefinitionId, configTemplateEntity.actorDefinitionId)
      assertEquals(response.configTemplate.partialDefaultConfig, configTemplateEntity.partialDefaultConfig)
    }

    @Test
    fun `test update default values`() {
      val updatedDefaultValues = objectMapper.readTree("{}")

      every { repository.save(any()) } returns configTemplateEntity

      val updatedConfigTemplateEntity = configTemplateEntity.copy(partialDefaultConfig = updatedDefaultValues)

      every { repository.update(any()) } returns updatedConfigTemplateEntity

      val response =
        service.updateTemplate(
          configTemplateId = configTemplateEntity.id!!,
          partialDefaultConfig = updatedDefaultValues,
          organizationId = OrganizationId(organizationId),
        )

      assertEquals(response.configTemplate.id, configTemplateEntity.id)
      assertEquals(response.configTemplate.organizationId, configTemplateEntity.organizationId)
      assertEquals(objectMapper.valueToTree(response.configTemplate.userConfigSpec), configTemplateEntity.userConfigSpec)
      assertEquals(response.configTemplate.actorDefinitionId, configTemplateEntity.actorDefinitionId)
      assertEquals(updatedDefaultValues, response.configTemplate.partialDefaultConfig)
    }

    @Test
    fun `test update user spec`() {
      val updatedUserConfigSpec = createPartialUserConfigSpecAsObject(mapOf("a_config_field" to "string"))

      every { repository.save(any()) } returns configTemplateEntity

      val updatedConfigTemplateEntity = configTemplateEntity.copy(userConfigSpec = updatedUserConfigSpec)

      every { repository.update(any()) } returns updatedConfigTemplateEntity

      val response =
        service.updateTemplate(
          configTemplateId = configTemplateEntity.id!!,
          userConfigSpec = updatedUserConfigSpec,
          organizationId = OrganizationId(organizationId),
        )

      assertEquals(response.configTemplate.id, configTemplateEntity.id)
      assertEquals(response.configTemplate.organizationId, configTemplateEntity.organizationId)
      assertEquals(response.configTemplate.partialDefaultConfig, configTemplateEntity.partialDefaultConfig)
      assertEquals(response.configTemplate.actorDefinitionId, configTemplateEntity.actorDefinitionId)
      assertEquals(updatedUserConfigSpec, objectMapper.valueToTree(response.configTemplate.userConfigSpec))
    }

    @Test
    fun `test update fails if spec is not respected`() {
      actorDefinitionVersion.spec = connectorSpecificationWithRequiredFields
      val updatedUserConfigSpec = createPartialUserConfigSpecAsObject(mapOf("a_config_field" to "string"))

      every { repository.save(any()) } returns configTemplateEntity

      org.junit.jupiter.api.assertThrows<JsonValidationException> {
        service.updateTemplate(
          configTemplateId = configTemplateEntity.id!!,
          userConfigSpec = updatedUserConfigSpec,
          organizationId = OrganizationId(organizationId),
        )
      }
    }
  }

  /**
   * Helper function to create a ConfigTemplate for testing
   */
  private fun createConfigTemplate(
    id: UUID = configTemplateId,
    orgId: UUID = organizationId,
    actorDefId: UUID = actorDefinitionId,
    defaultConfig: JsonNode = partialDefaultConfig,
    configSpec: JsonNode = userConfigSpecObject,
  ): io.airbyte.data.repositories.entities.ConfigTemplate =
    io.airbyte.data.repositories.entities.ConfigTemplate(
      id = id,
      organizationId = orgId,
      actorDefinitionId = actorDefId,
      partialDefaultConfig = defaultConfig,
      userConfigSpec = configSpec,
    )

  private fun createPartialUserConfigSpecAsObject(fields: Map<String, String>): ObjectNode {
    val connectionSpecification = objectMapper.createObjectNode()
    val specRequired = objectMapper.createArrayNode()
    connectionSpecification.put("type", "object")
    connectionSpecification.put("required", specRequired)
    connectionSpecification.put("properties", objectMapper.createObjectNode())
    val specProperties = connectionSpecification["properties"] as ObjectNode
    for (f in fields) {
      specRequired.add(f.key)
      specProperties.set<ObjectNode>(f.key, objectMapper.createObjectNode().put("type", f.value))
    }

    val spec = ConnectorSpecification().withConnectionSpecification(connectionSpecification)

    return objectMapper.valueToTree(spec)
  }

  private fun createConnectorSpecification(requiredFields: List<String>): ConnectorSpecification {
    val connectionSpecification = objectMapper.createObjectNode().set<ObjectNode>("properties", objectMapper.createObjectNode())
    connectionSpecification.set<ArrayNode>("required", objectMapper.createArrayNode())
    val specProperties = connectionSpecification["properties"] as ObjectNode
    specProperties.set<ObjectNode>("a_config_field", objectMapper.createObjectNode().put("type", "string"))
    specProperties.set<ObjectNode>("an_integer_field", objectMapper.createObjectNode().put("type", "integer"))

    val requiredArray = connectionSpecification["required"] as ArrayNode
    for (field in requiredFields) {
      requiredArray.add(field)
    }

    return ConnectorSpecification().withConnectionSpecification(connectionSpecification)
  }
}
