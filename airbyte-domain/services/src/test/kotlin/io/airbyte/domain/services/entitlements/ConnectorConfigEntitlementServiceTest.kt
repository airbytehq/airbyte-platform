/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.entitlements

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.DestinationObjectStorageEntitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.services.storage.ConnectorObjectStorageService
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class ConnectorConfigEntitlementServiceTest {
  private val entitlementService = mockk<EntitlementService>()
  private val connectorObjectStorageService = mockk<ConnectorObjectStorageService>()
  private val objectMapper = ObjectMapper()
  private val objectStorageProperty = "object_storage_config"

  private lateinit var connectorConfigEntitlementService: ConnectorConfigEntitlementService

  @BeforeEach
  fun setUp() {
    connectorConfigEntitlementService =
      ConnectorConfigEntitlementService(
        entitlementService,
        connectorObjectStorageService,
      )
  }

  @Nested
  inner class GetEntitledSpec {
    private val organizationId = OrganizationId(UUID.randomUUID())
    private val actorDefinitionVersionId = UUID.randomUUID()

    @Test
    fun `returns original spec when no entitlement requirements`() {
      val spec = createBasicSpec()
      val actorDefinitionVersion =
        ActorDefinitionVersion()
          .withVersionId(actorDefinitionVersionId)
          .withSpec(spec)

      every { connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion) } returns null

      val result = connectorConfigEntitlementService.getEntitledConnectorSpec(organizationId, actorDefinitionVersion)

      assertEquals(spec, result.spec)
      assertTrue(result.missingEntitlements.isEmpty())
    }

    @Test
    fun `returns full spec when organization has entitlements`() {
      val spec = createSpecWithObjectStorage()
      val actorDefinitionVersion =
        ActorDefinitionVersion()
          .withVersionId(actorDefinitionVersionId)
          .withSpec(spec)

      every { connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion) } returns objectStorageProperty
      every { entitlementService.checkEntitlement(organizationId.value, DestinationObjectStorageEntitlement) } returns
        EntitlementResult(isEntitled = true, featureId = DestinationObjectStorageEntitlement.featureId)

      val result = connectorConfigEntitlementService.getEntitledConnectorSpec(organizationId, actorDefinitionVersion)

      assertTrue(
        result.spec.connectionSpecification
          .get("properties")
          .has(objectStorageProperty),
      )
      assertTrue(result.missingEntitlements.isEmpty())
    }

    @Test
    fun `marks fields with entitlements when organization lacks entitlements`() {
      val spec = createSpecWithObjectStorage()
      val actorDefinitionVersion =
        ActorDefinitionVersion()
          .withVersionId(actorDefinitionVersionId)
          .withSpec(spec)

      every { connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion) } returns objectStorageProperty
      every { entitlementService.checkEntitlement(organizationId.value, DestinationObjectStorageEntitlement) } returns
        EntitlementResult(isEntitled = false, featureId = DestinationObjectStorageEntitlement.featureId)

      val result = connectorConfigEntitlementService.getEntitledConnectorSpec(organizationId, actorDefinitionVersion)

      // Field should still exist but with airbyte_required_entitlement and airbyte_hidden properties
      assertTrue(
        result.spec.connectionSpecification
          .get("properties")
          .has(objectStorageProperty),
      )
      val objectStoragePropertyNode =
        result.spec.connectionSpecification
          .get("properties")
          .get(objectStorageProperty)
      assertEquals(DestinationObjectStorageEntitlement.featureId, objectStoragePropertyNode.get("airbyte_required_entitlement").asText())
      assertTrue(objectStoragePropertyNode.get("airbyte_hidden").asBoolean())

      assertTrue(
        result.spec.connectionSpecification
          .get("properties")
          .has("host"),
      )
      assertEquals(listOf(DestinationObjectStorageEntitlement.featureId), result.missingEntitlements)
    }

    @Test
    fun `only adds airbyte_required_entitlement when organization lacks entitlements`() {
      val spec = createSpecWithRequiredObjectStorage()
      val actorDefinitionVersion =
        ActorDefinitionVersion()
          .withVersionId(actorDefinitionVersionId)
          .withSpec(spec)

      every { connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion) } returns objectStorageProperty
      every { entitlementService.checkEntitlement(organizationId.value, DestinationObjectStorageEntitlement) } returns
        EntitlementResult(isEntitled = false, featureId = DestinationObjectStorageEntitlement.featureId)

      val result = connectorConfigEntitlementService.getEntitledConnectorSpec(organizationId, actorDefinitionVersion)

      // Required field should exist without airbyte_required_entitlement or airbyte_hidden properties
      assertTrue(
        result.spec.connectionSpecification
          .get("properties")
          .has(objectStorageProperty),
      )
      val objectStoragePropertyNode =
        result.spec.connectionSpecification
          .get("properties")
          .get(objectStorageProperty)
      assertEquals(DestinationObjectStorageEntitlement.featureId, objectStoragePropertyNode.get("airbyte_required_entitlement").asText())
      assertFalse(objectStoragePropertyNode.has("airbyte_hidden"))

      assertEquals(listOf(DestinationObjectStorageEntitlement.featureId), result.missingEntitlements)
    }
  }

  @Nested
  inner class EnsureEntitledConfig {
    private val organizationId = OrganizationId(UUID.randomUUID())
    private val actorDefinitionVersionId = UUID.randomUUID()

    @Test
    fun `passes validation when config has no entitled properties configured`() {
      val config =
        objectMapper.createObjectNode().apply {
          put("otherProperty", "value")
        }
      val actorDefinitionVersion = ActorDefinitionVersion().withVersionId(actorDefinitionVersionId)

      every { connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion) } returns objectStorageProperty

      connectorConfigEntitlementService.ensureEntitledConfig(organizationId, actorDefinitionVersion, config)
    }

    @Test
    fun `passes validation when entitled property is null`() {
      val config =
        objectMapper.createObjectNode().apply {
          putNull(objectStorageProperty)
        }
      val actorDefinitionVersion = ActorDefinitionVersion().withVersionId(actorDefinitionVersionId)

      every { connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion) } returns objectStorageProperty

      connectorConfigEntitlementService.ensureEntitledConfig(organizationId, actorDefinitionVersion, config)
    }

    @Test
    fun `passes validation when organization has entitlements`() {
      val config =
        objectMapper.createObjectNode().apply {
          put(objectStorageProperty, "storage-config")
        }
      val actorDefinitionVersion = ActorDefinitionVersion().withVersionId(actorDefinitionVersionId)

      every { connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion) } returns objectStorageProperty
      every { entitlementService.ensureEntitled(organizationId.value, DestinationObjectStorageEntitlement) } returns Unit

      connectorConfigEntitlementService.ensureEntitledConfig(organizationId, actorDefinitionVersion, config)
      verify { entitlementService.ensureEntitled(organizationId.value, DestinationObjectStorageEntitlement) }
    }

    @Test
    fun `throws exception when organization lacks entitlements`() {
      val config =
        objectMapper.createObjectNode().apply {
          put(objectStorageProperty, "storage-config")
        }
      val actorDefinitionVersion = ActorDefinitionVersion().withVersionId(actorDefinitionVersionId)
      val expectedException = RuntimeException("Not entitled")

      every { connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion) } returns objectStorageProperty
      every { entitlementService.ensureEntitled(organizationId.value, DestinationObjectStorageEntitlement) } throws expectedException

      assertThrows<RuntimeException> {
        connectorConfigEntitlementService.ensureEntitledConfig(organizationId, actorDefinitionVersion, config)
      }
    }

    @Test
    fun `passes validation when object storage has storage_type None`() {
      val config =
        Jsons.jsonNode(
          mapOf(
            objectStorageProperty to
              mapOf(
                "storage_type" to "None",
              ),
          ),
        )

      val actorDefinitionVersion = ActorDefinitionVersion().withVersionId(actorDefinitionVersionId)

      every { connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion) } returns objectStorageProperty

      // Should not call ensureEntitled because discriminator considers this as "not set"
      connectorConfigEntitlementService.ensureEntitledConfig(organizationId, actorDefinitionVersion, config)

      verify(exactly = 0) { entitlementService.ensureEntitled(any(), any()) }
    }

    @Test
    fun `requires entitlement when object storage has storage_type other than None`() {
      val config =
        Jsons.jsonNode(
          mapOf(
            objectStorageProperty to
              mapOf(
                "storage_type" to "S3",
                "bucket_name" to "my-bucket",
              ),
          ),
        )

      val actorDefinitionVersion = ActorDefinitionVersion().withVersionId(actorDefinitionVersionId)

      every { connectorObjectStorageService.getObjectStorageConfigProperty(actorDefinitionVersion) } returns objectStorageProperty
      every { entitlementService.ensureEntitled(organizationId.value, DestinationObjectStorageEntitlement) } returns Unit

      // Should call ensureEntitled because discriminator considers this as "set"
      connectorConfigEntitlementService.ensureEntitledConfig(organizationId, actorDefinitionVersion, config)

      verify(exactly = 1) { entitlementService.ensureEntitled(organizationId.value, DestinationObjectStorageEntitlement) }
    }
  }

  // Helper methods to create test data
  private fun createBasicSpec(): ConnectorSpecification {
    val jsonSchema =
      objectMapper.createObjectNode().apply {
        put("type", "object")
        put("additionalProperties", false)
        set<JsonNode>(
          "properties",
          objectMapper.createObjectNode().apply {
            put("host", "string")
            put("port", "integer")
          },
        )
      }

    return ConnectorSpecification()
      .withConnectionSpecification(jsonSchema)
  }

  private fun createSpecWithObjectStorage(): ConnectorSpecification {
    val jsonSchema =
      objectMapper.createObjectNode().apply {
        put("type", "object")
        put("additionalProperties", false)
        set<JsonNode>(
          "properties",
          objectMapper.createObjectNode().apply {
            put("host", "string")
            put("port", "integer")
            set<JsonNode>(
              objectStorageProperty,
              objectMapper.createObjectNode().apply {
                put("type", "object")
                put("title", "Object Storage Configuration")
              },
            )
          },
        )
      }

    return ConnectorSpecification()
      .withConnectionSpecification(jsonSchema)
  }

  private fun createSpecWithRequiredObjectStorage(): ConnectorSpecification {
    val jsonSchema =
      objectMapper.createObjectNode().apply {
        put("type", "object")
        put("additionalProperties", false)
        set<JsonNode>(
          "properties",
          objectMapper.createObjectNode().apply {
            put("host", "string")
            put("port", "integer")
            set<JsonNode>(
              objectStorageProperty,
              objectMapper.createObjectNode().apply {
                put("type", "object")
                put("title", "Object Storage Configuration")
              },
            )
          },
        )
        set<JsonNode>(
          "required",
          objectMapper.createArrayNode().apply {
            add("host")
            add(objectStorageProperty)
          },
        )
      }

    return ConnectorSpecification()
      .withConnectionSpecification(jsonSchema)
  }
}
